package main

import (
	"context"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/pion/rtp"
)

// pacedFrame is one logical emission: one or more RTP packets sharing the same
// schedule slot (e.g. one AAC AU or one HEVC AU split across RTP fragments).
type pacedFrame struct {
	pkts     []*rtp.Packet
	media    *description.Media
	duration time.Duration
}

// videoPaceState tracks the previous frame's continuous PTS (microseconds) for
// computing per-AU pacer durations (ΔPTS → wall spacing).
type videoPaceState struct {
	prevUS  uint64
	hasPrev bool
}

// durationForFrame returns the wall duration between this frame and the previous
// emitted video AU. First frame and anomalies (non-increasing or Δ ≥ 5s) yield 0.
func (s *videoPaceState) durationForFrame(continuousUS uint64) time.Duration {
	const maxDeltaUS = 5_000_000 // 5s of media time; larger deltas treated as discontinuities
	if !s.hasPrev {
		s.hasPrev = true
		s.prevUS = continuousUS
		return 0
	}
	if continuousUS <= s.prevUS {
		s.prevUS = continuousUS
		return 0
	}
	delta := continuousUS - s.prevUS
	s.prevUS = continuousUS
	if delta >= maxDeltaUS {
		return 0
	}
	return time.Duration(delta) * time.Microsecond
}

// mediaPacer smooths bursty upstream media onto the wire using an absolute
// next-emission cursor and wall-clock-relative scheduling.
type mediaPacer struct {
	ch             chan pacedFrame
	maxLead        time.Duration
	initialLatency time.Duration
	snapOnPast     bool
	handler        *rtspStreamHandler

	overflowMu      sync.Mutex
	lastOverflowLog time.Time
}

// enqueue sends a paced frame to the pacer goroutine. If the channel is full,
// the frame is dropped and overflow is logged at most once per minute.
func (p *mediaPacer) enqueue(item pacedFrame) {
	select {
	case p.ch <- item:
	default:
		p.warnOverflowOnce()
	}
}

// warnOverflowOnce logs a queue-overflow warning, rate-limited to once per
// minute to avoid log spam under sustained overload.
func (p *mediaPacer) warnOverflowOnce() {
	p.overflowMu.Lock()
	defer p.overflowMu.Unlock()
	now := time.Now()
	if now.Sub(p.lastOverflowLog) < 60*time.Second {
		return
	}
	p.lastOverflowLog = now
	log.Warnf("media pacer queue overflow (cap=%d); dropping frame", cap(p.ch))
}

// run drains pacedFrame values from the channel until ctx is cancelled or the
// channel closes. It waits on an absolute next-emission time, re-anchors the
// schedule per maxLead / snapOnPast / initialLatency, then writes each RTP
// packet to the handler and advances the cursor by item.duration.
func (p *mediaPacer) run(ctx context.Context) {
	var anchor time.Time
	var hasAnchor bool

	for {
		var item pacedFrame
		select {
		case <-ctx.Done():
			return
		case it, ok := <-p.ch:
			if !ok {
				return
			}
			item = it
		}

		now := time.Now()

		// naturalTarget is THIS item's own target: anchor (the previous item's target) plus
		// THIS item's own duration (the interval from the previous frame to this one, per
		// videoPaceState.durationForFrame). Using this item's own duration - not the
		// previous item's - matters: an earlier version advanced the schedule by the
		// PREVIOUS item's duration when computing THIS item's wait, silently applying each
		// frame's spacing to its successor instead of itself. That's invisible when frame
		// spacing is perfectly uniform, but this camera's Extern/Sub stream's real spacing
		// alternates (confirmed via live capture: ~40ms/~80ms), so the off-by-one produced
		// a persistent anti-phase jitter - every other frame emitted too early, the other
		// too late by the same amount.
		var naturalTarget time.Time
		if hasAnchor {
			naturalTarget = addDurationClampOverflow(anchor, item.duration)
		} else {
			naturalTarget = now.Add(p.initialLatency)
		}

		// Re-anchor. waitUntil controls when THIS (already-due) packet goes out; newAnchor
		// is the baseline the NEXT packet's naturalTarget advances from. These stay
		// independent: on a re-anchor we want to emit the current packet immediately
		// (waitUntil=now), not delay it by initialLatency too - that would turn "restore
		// some buffer margin" into "insert an initialLatency-long stall into live
		// playback", which is worse than the problem it's meant to fix.
		//
		// 1. Cursor too far in the future → we've built up excess lead (burst catch-up,
		//    or overshoot from case 2 below) - snap newAnchor back to bare now, NOT
		//    now+initialLatency. Re-inflating latency here created a self-sustaining loop:
		//    drift up to maxLead, snap to initialLatency, drift back up to maxLead, repeat
		//    - which pinned the queue at a permanent ~3s backlog that Protect's live-edge
		//    logic then had to fight by playing back faster than 1x.
		// 2. Cursor in the past → emit now only when snapOnPast (audio, and video for
		//    low-priority streams). Otherwise keep the past target so we burst-drain
		//    (video slope) - this is the one case where waitUntil intentionally lags now.
		//    This is the only branch that restores the initialLatency cushion, since it's
		//    the one actually recovering from a real stall and benefiting from headroom
		//    against the next one.
		var waitUntil, newAnchor time.Time
		switch {
		case hasAnchor && naturalTarget.After(now.Add(p.maxLead)):
			waitUntil = now
			newAnchor = now
		case hasAnchor && p.snapOnPast && naturalTarget.Before(now):
			waitUntil = now
			newAnchor = now.Add(p.initialLatency)
		default:
			waitUntil = naturalTarget
			newAnchor = naturalTarget
		}

		if waitUntil.After(now) {
			delay := time.Until(waitUntil)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}

		for _, pkt := range item.pkts {
			if p.handler != nil && item.media != nil && pkt != nil {
				p.handler.writePacket(item.media, pkt)
			}
		}

		// anchor becomes this item's own target (newAnchor already includes this item's
		// duration where relevant - see naturalTarget above), so the NEXT item's duration
		// gets added exactly once, not accumulated twice.
		anchor = newAnchor
		hasAnchor = true
	}
}

// addDurationClampOverflow returns t+d, or t if d is non-positive or adding d
// would overflow time.Time (in which case After would be false).
func addDurationClampOverflow(t time.Time, d time.Duration) time.Time {
	if d <= 0 {
		return t
	}
	out := t.Add(d)
	if !out.After(t) {
		return t
	}
	return out
}
