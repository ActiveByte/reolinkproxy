package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// hostOnly strips the port from an "ip:port" remote address (as found in http.Request.
// RemoteAddr), returning the raw string unchanged if it has no port - so callers can compare
// against a configured bare IP regardless of the client's ephemeral source port.
func hostOnly(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// onvifEventMsg is a single ONVIF notification message queued for a PullPoint subscriber.
// SourceItems and PropertyOperation are optional: when set (events forwarded verbatim from
// the camera's own ONVIF events service carry these), they're passed through as-is; when
// unset (our own synthesized motion/test events), the response falls back to a generic
// placeholder. Real notifications carry meaningful Source items (e.g. a "Rule" token for
// Motion, a per-channel "Source" id for smart-detection topics) that some ONVIF consumers
// require to accept/process an event at all - and use "Initialized" for an initial-state
// snapshot vs. "Changed" for an actual transition.
type onvifEventMsg struct {
	Topic             string
	ItemName          string
	ItemValue         string
	UTCTime           time.Time
	SourceItems       map[string]string
	PropertyOperation string
}

// pullPointSubscription is one ONVIF WS-BaseNotification PullPoint subscription. UniFi
// Protect (and most ONVIF NVRs) poll PullMessages on this instead of accepting a pushed
// HTTP callback, since that requires no inbound connectivity to the NVR.
type pullPointSubscription struct {
	id string
	ch chan onvifEventMsg

	// remoteAddr is the client IP that created this subscription (port stripped), so the web
	// UI can show which NVR/IP is actually pulling events for a camera.
	remoteAddr string
	createdAt  time.Time

	mu         sync.Mutex
	lastAccess time.Time
}

func (s *pullPointSubscription) touch() {
	s.mu.Lock()
	s.lastAccess = time.Now()
	s.mu.Unlock()
}

func (s *pullPointSubscription) idleSince(cutoff time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAccess.Before(cutoff)
}

// loggedEvent is a recent event kept around for the web UI's live event log, so AI/smart
// events (and test events) can be visually verified without needing a real NVR.
type loggedEvent struct {
	Source    string    `json:"source"` // "motion", "camera-onvif", or "test"
	Topic     string    `json:"topic"`
	ItemName  string    `json:"item_name"`
	ItemValue string    `json:"item_value"`
	Time      time.Time `json:"time"`
}

const maxRecentEvents = 50

// eventsBroker fans out one camera's events to any number of ONVIF PullPoint subscribers.
// Events come from two sources: (1) events forwarded verbatim from the camera's own ONVIF
// events service via Forward (motion, plus smart/AI topics on camera models that expose
// them - the camera's own Motion event already covers what UniFi Protect needs, so no
// separate Baichuan-derived motion signal is broadcast here), and (2) synthetic events
// injected via InjectTest for verifying the whole pipeline without waiting on real camera
// behavior.
type eventsBroker struct {
	cameraName string

	mu        sync.Mutex
	subs      map[string]*pullPointSubscription
	lastKnown map[string]onvifEventMsg // keyed by Topic+"|"+ItemName

	recentMu sync.Mutex
	recent   []loggedEvent
}

func newEventsBroker(cameraName string) *eventsBroker {
	b := &eventsBroker{
		cameraName: cameraName,
		subs:       make(map[string]*pullPointSubscription),
		lastKnown:  make(map[string]onvifEventMsg),
	}
	go b.reapLoop()
	return b
}

const (
	pullPointSubscriptionTTL = 5 * time.Minute
	pullPointChannelBuffer   = 64
)

// Forward publishes an event received from the camera's own ONVIF events service verbatim
// (topic and all) to every current PullPoint subscriber.
func (b *eventsBroker) Forward(ev onvifEventMsg) {
	b.broadcast("camera-onvif", ev)
}

// InjectTest publishes a synthetic event, letting the web UI verify the ONVIF Events
// pipeline end to end (through to Protect) without depending on real camera behavior.
func (b *eventsBroker) InjectTest(topic, itemName, itemValue string) {
	b.broadcast("test", onvifEventMsg{Topic: topic, ItemName: itemName, ItemValue: itemValue, UTCTime: time.Now().UTC()})
}

func (b *eventsBroker) broadcast(source string, ev onvifEventMsg) {
	b.record(source, ev)

	b.mu.Lock()
	b.lastKnown[lastKnownKey(ev.Topic, ev.ItemName)] = ev
	subs := make([]*pullPointSubscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()

	for _, s := range subs {
		enqueue(s.ch, ev)
	}
}

func lastKnownKey(topic, itemName string) string {
	return topic + "|" + itemName
}

func (b *eventsBroker) record(source string, ev onvifEventMsg) {
	b.recentMu.Lock()
	defer b.recentMu.Unlock()
	b.recent = append(b.recent, loggedEvent{Source: source, Topic: ev.Topic, ItemName: ev.ItemName, ItemValue: ev.ItemValue, Time: ev.UTCTime})
	if len(b.recent) > maxRecentEvents {
		b.recent = b.recent[len(b.recent)-maxRecentEvents:]
	}
}

// RecentEvents returns the most recent events (oldest first) for the web UI's live log.
func (b *eventsBroker) RecentEvents() []loggedEvent {
	b.recentMu.Lock()
	defer b.recentMu.Unlock()
	return append([]loggedEvent(nil), b.recent...)
}

// SubscriberCount reports how many ONVIF PullPoint subscriptions are currently live for
// this camera. Broadcasts (including test events) only reach subscriptions that already
// exist at broadcast time - if this is 0, nothing is actually listening right now,
// regardless of whether the broadcast itself succeeded.
func (b *eventsBroker) SubscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

// subscriberInfo is a snapshot of one live PullPoint subscription for the web UI.
type subscriberInfo struct {
	RemoteAddr string
	CreatedAt  time.Time
	LastSeen   time.Time
}

// Subscribers returns identifying info for every currently-live PullPoint subscription, so
// the web UI can show which IPs (e.g. UniFi Protect) are actually pulling events.
func (b *eventsBroker) Subscribers() []subscriberInfo {
	b.mu.Lock()
	subs := make([]*pullPointSubscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()

	out := make([]subscriberInfo, 0, len(subs))
	for _, s := range subs {
		s.mu.Lock()
		out = append(out, subscriberInfo{RemoteAddr: s.remoteAddr, CreatedAt: s.createdAt, LastSeen: s.lastAccess})
		s.mu.Unlock()
	}
	return out
}

// createPullPoint registers a new subscription and immediately seeds it with the current
// known state of every event topic/item this broker has ever broadcast, mirroring what
// real ONVIF cameras do (confirmed by comparison against a real camera's own subscription
// behavior: it delivers a full current-state snapshot immediately on subscribe, not just
// future transitions). Without this, a freshly subscribed NVR sees total silence until the
// next actual state change, which some NVR-side ONVIF clients read as a sign of a
// misbehaving/rate-limited server rather than "nothing has happened yet".
func (b *eventsBroker) createPullPoint(remoteAddr string) *pullPointSubscription {
	now := time.Now()
	sub := &pullPointSubscription{
		id:         uuid.New().String(),
		ch:         make(chan onvifEventMsg, pullPointChannelBuffer),
		remoteAddr: hostOnly(remoteAddr),
		createdAt:  now,
		lastAccess: now,
	}

	b.mu.Lock()
	b.subs[sub.id] = sub
	initial := make([]onvifEventMsg, 0, len(b.lastKnown))
	for _, ev := range b.lastKnown {
		initial = append(initial, ev)
	}
	b.mu.Unlock()

	for _, ev := range initial {
		enqueue(sub.ch, ev)
	}
	log.Printf("onvif pullpoint: camera=%s sub=%s created replay_count=%d", b.cameraName, sub.id, len(initial))
	return sub
}

// enqueue pushes ev, dropping the oldest queued event first if the channel is full so
// subscribers that briefly fall behind still see the latest state rather than stalling.
func enqueue(ch chan onvifEventMsg, ev onvifEventMsg) {
	select {
	case ch <- ev:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- ev:
	default:
	}
}

func (b *eventsBroker) get(id string) *pullPointSubscription {
	b.mu.Lock()
	sub := b.subs[id]
	b.mu.Unlock()
	if sub != nil {
		sub.touch()
	}
	return sub
}

func (b *eventsBroker) unsubscribe(id string) {
	b.mu.Lock()
	delete(b.subs, id)
	b.mu.Unlock()
}

func (b *eventsBroker) reapLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-pullPointSubscriptionTTL)

		b.mu.Lock()
		var expired []string
		for id, sub := range b.subs {
			if sub.idleSince(cutoff) {
				expired = append(expired, id)
			}
		}
		b.mu.Unlock()

		for _, id := range expired {
			log.Printf("onvif events: reaping idle pullpoint subscription camera=%s id=%s", b.cameraName, id)
			b.unsubscribe(id)
		}
	}
}

const eventsServiceCapabilitiesXML = `<tev:GetServiceCapabilitiesResponse><tev:Capabilities WSSubscriptionPolicySupport="false" WSPullPointSupport="true" WSPausableSubscriptionManagerInterfaceSupport="false" MaxNotificationProducers="1" MaxPullPoints="8" PersistentNotificationStorage="false"/></tev:GetServiceCapabilitiesResponse>`

const eventsPropertiesXML = `<tev:GetEventPropertiesResponse>` +
	`<tev:TopicNamespaceLocation>http://www.onvif.org/onvif/ver10/topics/topicns.xml</tev:TopicNamespaceLocation>` +
	`<wsnt:FixedTopicSet>true</wsnt:FixedTopicSet>` +
	`<wsnt:TopicSet>` +
	`<tns1:RuleEngine><tns1:CellMotionDetector><tns1:Motion wstop:topic="true">` +
	`<tt:MessageDescription IsProperty="true">` +
	`<tt:Source><tt:SimpleItemDescription Name="Source" Type="tt:ReferenceToken"/></tt:Source>` +
	`<tt:Data><tt:SimpleItemDescription Name="IsMotion" Type="xs:boolean"/></tt:Data>` +
	`</tt:MessageDescription>` +
	`</tns1:Motion></tns1:CellMotionDetector></tns1:RuleEngine>` +
	`<tns1:VideoSource><tns1:MotionAlarm wstop:topic="true">` +
	`<tt:MessageDescription IsProperty="true">` +
	`<tt:Source><tt:SimpleItemDescription Name="Source" Type="tt:ReferenceToken"/></tt:Source>` +
	`<tt:Data><tt:SimpleItemDescription Name="State" Type="xs:boolean"/></tt:Data>` +
	`</tt:MessageDescription>` +
	`</tns1:MotionAlarm></tns1:VideoSource>` +
	`</wsnt:TopicSet>` +
	`<tev:TopicExpressionDialect>http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet</tev:TopicExpressionDialect>` +
	`</tev:GetEventPropertiesResponse>`

const unsubscribeResponseXML = `<wsnt:UnsubscribeResponse></wsnt:UnsubscribeResponse>`

func (s *onvifServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		writeSOAPFault(w, http.StatusBadRequest, "ter:InvalidArgVal", "failed to read request body")
		return
	}

	if !s.authenticate(string(body)) {
		writeSOAPFault(w, http.StatusUnauthorized, "ter:NotAuthorized", "The action requires authorization")
		return
	}

	action := soapAction(r, string(body), []string{
		"GetServiceCapabilities",
		"GetEventProperties",
		"CreatePullPointSubscription",
	})

	switch action {
	case "GetServiceCapabilities":
		writeSOAPResponse(w, eventsServiceCapabilitiesXML)
	case "GetEventProperties":
		writeSOAPResponse(w, eventsPropertiesXML)
	case "CreatePullPointSubscription":
		if s.events == nil {
			writeSOAPFault(w, http.StatusInternalServerError, "ter:Action", "events unavailable")
			return
		}
		sub := s.events.createPullPoint(r.RemoteAddr)
		writeSOAPResponse(w, s.createPullPointSubscriptionResponse(r, sub))
	default:
		log.Printf("onvif events: unsupported action %q (body: %s)", action, body)
		writeSOAPFault(w, http.StatusBadRequest, "ter:ActionNotSupported", "events action not supported")
	}
}

func (s *onvifServer) handlePullPoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/onvif/pullpoint/"), "/")

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		writeSOAPFault(w, http.StatusBadRequest, "ter:InvalidArgVal", "failed to read request body")
		return
	}

	if !s.authenticate(string(body)) {
		writeSOAPFault(w, http.StatusUnauthorized, "ter:NotAuthorized", "The action requires authorization")
		return
	}

	if s.events == nil {
		writeSOAPFault(w, http.StatusNotFound, "ter:InvalidArgVal", "unknown subscription")
		return
	}

	sub := s.events.get(id)
	if sub == nil {
		writeSOAPFault(w, http.StatusNotFound, "ter:InvalidArgVal", "unknown or expired subscription")
		return
	}

	switch soapAction(r, string(body), []string{"PullMessages", "Renew", "Unsubscribe"}) {
	case "PullMessages":
		s.handlePullMessages(w, r, sub, string(body))
	case "Renew":
		writeSOAPResponse(w, renewResponseXML(pullPointSubscriptionTTL))
	case "Unsubscribe":
		s.events.unsubscribe(id)
		writeSOAPResponse(w, unsubscribeResponseXML)
	default:
		writeSOAPFault(w, http.StatusBadRequest, "ter:ActionNotSupported", "pullpoint action not supported")
	}
}

var isoDurationRE = regexp.MustCompile(`^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$`)

// parseISODurationClamped parses a minimal xs:duration like "PT30S"/"PT2M", clamping the
// result to [min, max] and falling back to max if parsing fails. NVRs use this to tell the
// PullPoint how long it's willing to long-poll for.
func parseISODurationClamped(raw string, min, max time.Duration) time.Duration {
	m := isoDurationRE.FindStringSubmatch(strings.TrimSpace(raw))
	if m == nil {
		return max
	}
	var total time.Duration
	if m[1] != "" {
		h, _ := strconv.Atoi(m[1])
		total += time.Duration(h) * time.Hour
	}
	if m[2] != "" {
		mi, _ := strconv.Atoi(m[2])
		total += time.Duration(mi) * time.Minute
	}
	if m[3] != "" {
		s, _ := strconv.ParseFloat(m[3], 64)
		total += time.Duration(s * float64(time.Second))
	}
	if total <= 0 {
		return max
	}
	if total < min {
		return min
	}
	if total > max {
		return max
	}
	return total
}

func (s *onvifServer) handlePullMessages(w http.ResponseWriter, r *http.Request, sub *pullPointSubscription, body string) {
	type pullMessagesEnvelope struct {
		Body struct {
			PullMessages struct {
				Timeout      string `xml:"Timeout"`
				MessageLimit int    `xml:"MessageLimit"`
			} `xml:"PullMessages"`
		} `xml:"Body"`
	}

	var env pullMessagesEnvelope
	_ = xml.Unmarshal([]byte(body), &env)

	limit := env.Body.PullMessages.MessageLimit
	if limit <= 0 {
		limit = 10
	}
	timeout := parseISODurationClamped(env.Body.PullMessages.Timeout, 2*time.Second, 30*time.Second)

	log.Debugf("onvif pullpoint: camera=%s sub=%s remote=%s parsed_timeout_raw=%q parsed_limit=%d pending_in_channel=%d",
		s.cfg.DeviceName, sub.id, r.RemoteAddr, env.Body.PullMessages.Timeout, env.Body.PullMessages.MessageLimit, len(sub.ch))

	var msgs []onvifEventMsg
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	start := time.Now()
	waitResult := "event"
	select {
	case ev, ok := <-sub.ch:
		if ok {
			msgs = append(msgs, ev)
		} else {
			waitResult = "channel-closed"
		}
	case <-timer.C:
		waitResult = "timeout"
	case <-r.Context().Done():
		waitResult = "context-done: " + fmt.Sprint(r.Context().Err())
	}
	elapsed := time.Since(start)
	if elapsed < timeout/2 && waitResult != "event" {
		log.Warnf("onvif pullpoint: camera=%s sub=%s returned early result=%s requested_timeout=%s elapsed=%s remote=%s",
			s.cfg.DeviceName, sub.id, waitResult, timeout, elapsed, r.RemoteAddr)
	}

drain:
	for len(msgs) < limit {
		select {
		case ev, ok := <-sub.ch:
			if !ok {
				break drain
			}
			msgs = append(msgs, ev)
		default:
			break drain
		}
	}

	responseXML := s.pullMessagesResponseXML(msgs, timeout)
	log.Debugf("onvif pullpoint: camera=%s sub=%s requested_timeout=%s wait=%s messages=%d",
		s.cfg.DeviceName, sub.id, timeout, waitResult, len(msgs))

	writeSOAPResponse(w, responseXML)
}

func (s *onvifServer) pullMessagesResponseXML(msgs []onvifEventMsg, timeout time.Duration) string {
	now := time.Now().UTC()
	term := now.Add(timeout)

	var b strings.Builder
	fmt.Fprintf(&b, `<tev:PullMessagesResponse><tev:CurrentTime>%s</tev:CurrentTime><tev:TerminationTime>%s</tev:TerminationTime>`,
		now.Format(time.RFC3339), term.Format(time.RFC3339))

	for _, m := range msgs {
		op := m.PropertyOperation
		if op == "" {
			op = "Changed"
		}

		var sourceXML strings.Builder
		if len(m.SourceItems) > 0 {
			for name, value := range m.SourceItems {
				fmt.Fprintf(&sourceXML, `<tt:SimpleItem Name="%s" Value="%s"/>`, xmlEscape(name), xmlEscape(value))
			}
		} else {
			fmt.Fprintf(&sourceXML, `<tt:SimpleItem Name="Source" Value="%s"/>`, xmlEscape(s.cfg.DeviceName))
		}

		fmt.Fprintf(&b,
			`<wsnt:NotificationMessage><wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">%s</wsnt:Topic>`+
				`<wsnt:Message><tt:Message UtcTime="%s" PropertyOperation="%s">`+
				`<tt:Source>%s</tt:Source>`+
				`<tt:Data><tt:SimpleItem Name="%s" Value="%s"/></tt:Data>`+
				`</tt:Message></wsnt:Message></wsnt:NotificationMessage>`,
			xmlEscape(m.Topic), m.UTCTime.Format(time.RFC3339), xmlEscape(op), sourceXML.String(), xmlEscape(m.ItemName), xmlEscape(m.ItemValue))
	}

	b.WriteString(`</tev:PullMessagesResponse>`)
	return b.String()
}

func (s *onvifServer) createPullPointSubscriptionResponse(r *http.Request, sub *pullPointSubscription) string {
	now := time.Now().UTC()
	term := now.Add(pullPointSubscriptionTTL)
	addr := buildURL("http", s.authorityForRequest(r, s.cfg.Address), "/onvif/pullpoint/"+sub.id)

	return fmt.Sprintf(
		`<tev:CreatePullPointSubscriptionResponse><tev:SubscriptionReference><wsa:Address>%s</wsa:Address></tev:SubscriptionReference>`+
			`<wsnt:CurrentTime>%s</wsnt:CurrentTime><wsnt:TerminationTime>%s</wsnt:TerminationTime></tev:CreatePullPointSubscriptionResponse>`,
		xmlEscape(addr), now.Format(time.RFC3339), term.Format(time.RFC3339),
	)
}

func renewResponseXML(timeout time.Duration) string {
	now := time.Now().UTC()
	term := now.Add(timeout)
	return fmt.Sprintf(`<wsnt:RenewResponse><wsnt:CurrentTime>%s</wsnt:CurrentTime><wsnt:TerminationTime>%s</wsnt:TerminationTime></wsnt:RenewResponse>`,
		now.Format(time.RFC3339), term.Format(time.RFC3339))
}
