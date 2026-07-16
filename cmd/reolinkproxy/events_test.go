package main

import (
	"strings"
	"testing"
	"time"
)

func waitForEvent(t *testing.T, ch chan onvifEventMsg, topic string) onvifEventMsg {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Topic == topic {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event on topic %q", topic)
		}
	}
}

func TestEventsBrokerGetTouchesLastAccessAndUnsubscribeStopsFeed(t *testing.T) {
	t.Parallel()

	broker := newEventsBroker("front")
	sub := broker.createPullPoint("127.0.0.1:12345")

	if broker.get(sub.id) == nil {
		t.Fatal("expected to find subscription by id")
	}

	broker.unsubscribe(sub.id)

	if broker.get(sub.id) != nil {
		t.Fatal("expected subscription to be gone after unsubscribe")
	}

	// Broadcasts read the current subscriber set fresh each time, so a removed
	// subscription must not receive further events.
	broker.Forward(onvifEventMsg{Topic: "tns1:RuleEngine/CellMotionDetector/Motion", ItemName: "IsMotion", ItemValue: "true", UTCTime: time.Now()})
	select {
	case ev, ok := <-sub.ch:
		if ok {
			t.Fatalf("expected no further events after unsubscribe, got %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func TestEventsBrokerReplaysLastKnownStateToNewSubscribers(t *testing.T) {
	t.Parallel()

	broker := newEventsBroker("front")

	// Simulate state that arrived before anyone had subscribed yet - e.g. the camera's
	// own forwarded smart-event snapshot pulled shortly after startup.
	broker.Forward(onvifEventMsg{Topic: "tns1:RuleEngine/MyRuleDetector/PeopleDetect", ItemName: "State", ItemValue: "false", UTCTime: time.Now()})
	broker.Forward(onvifEventMsg{Topic: "tns1:RuleEngine/MyRuleDetector/VehicleDetect", ItemName: "State", ItemValue: "false", UTCTime: time.Now()})

	sub := broker.createPullPoint("127.0.0.1:12345")

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-sub.ch:
			seen[ev.Topic] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replayed event %d", i)
		}
	}
	if !seen["tns1:RuleEngine/MyRuleDetector/PeopleDetect"] || !seen["tns1:RuleEngine/MyRuleDetector/VehicleDetect"] {
		t.Fatalf("expected both last-known topics replayed to the new subscription, got %+v", seen)
	}
}

func TestEventsBrokerForwardDeliversToSubscribersAndRecentLog(t *testing.T) {
	t.Parallel()

	broker := newEventsBroker("front")
	sub := broker.createPullPoint("127.0.0.1:12345")

	broker.Forward(onvifEventMsg{Topic: "tns1:RuleEngine/PeopleDetector/People", ItemName: "IsPeople", ItemValue: "true", UTCTime: time.Now()})

	ev := waitForEvent(t, sub.ch, "tns1:RuleEngine/PeopleDetector/People")
	if ev.ItemName != "IsPeople" || ev.ItemValue != "true" {
		t.Fatalf("unexpected forwarded event: %+v", ev)
	}

	recent := broker.RecentEvents()
	if len(recent) != 1 || recent[0].Source != "camera-onvif" {
		t.Fatalf("expected 1 recorded camera-onvif event, got %+v", recent)
	}
}

func TestEventsBrokerInjectTestDeliversAndTagsSourceAsTest(t *testing.T) {
	t.Parallel()

	broker := newEventsBroker("front")
	sub := broker.createPullPoint("127.0.0.1:12345")

	broker.InjectTest("tns1:VideoSource/MotionAlarm", "State", "true")

	ev := waitForEvent(t, sub.ch, "tns1:VideoSource/MotionAlarm")
	if ev.ItemValue != "true" {
		t.Fatalf("unexpected test event: %+v", ev)
	}

	recent := broker.RecentEvents()
	if len(recent) != 1 || recent[0].Source != "test" {
		t.Fatalf("expected 1 recorded test event, got %+v", recent)
	}
}

func TestEventsBrokerSubscriberCount(t *testing.T) {
	t.Parallel()

	broker := newEventsBroker("front")
	if broker.SubscriberCount() != 0 {
		t.Fatalf("expected 0 subscribers initially, got %d", broker.SubscriberCount())
	}

	sub1 := broker.createPullPoint("127.0.0.1:12345")
	if broker.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", broker.SubscriberCount())
	}

	broker.createPullPoint("127.0.0.1:12345")
	if broker.SubscriberCount() != 2 {
		t.Fatalf("expected 2 subscribers, got %d", broker.SubscriberCount())
	}

	broker.unsubscribe(sub1.id)
	if broker.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber after unsubscribe, got %d", broker.SubscriberCount())
	}
}

func TestEventsBrokerRecentEventsCapsAtMax(t *testing.T) {
	t.Parallel()

	broker := newEventsBroker("front")
	for i := 0; i < maxRecentEvents+10; i++ {
		broker.InjectTest("t", "n", "v")
	}

	recent := broker.RecentEvents()
	if len(recent) != maxRecentEvents {
		t.Fatalf("expected recent events capped at %d, got %d", maxRecentEvents, len(recent))
	}
}

func TestPullMessagesResponseXMLUsesWSNTNamespaceForNotificationMessage(t *testing.T) {
	t.Parallel()

	s := &onvifServer{cfg: onvifConfig{DeviceName: "front"}}
	xmlOut := s.pullMessagesResponseXML([]onvifEventMsg{
		{Topic: "tns1:RuleEngine/MyRuleDetector/PeopleDetect", ItemName: "State", ItemValue: "true", UTCTime: time.Now()},
	}, 5*time.Second)

	// NotificationMessage is defined by WS-BaseNotification in the wsnt namespace, not
	// ONVIF's own tev (events WSDL) namespace. A strict/namespace-aware ONVIF client binds
	// on the correct namespace and will silently find zero notifications - not an error,
	// just an empty-looking response - if this regresses to <tev:NotificationMessage>.
	// Confirmed against a real camera's own raw PullMessages response, which uses
	// <wsnt:NotificationMessage>.
	if !strings.Contains(xmlOut, "<wsnt:NotificationMessage>") {
		t.Fatalf("expected NotificationMessage in the wsnt namespace, got: %s", xmlOut)
	}
	if strings.Contains(xmlOut, "<tev:NotificationMessage>") {
		t.Fatalf("NotificationMessage must not be in the tev namespace, got: %s", xmlOut)
	}
	if !strings.Contains(xmlOut, "</wsnt:NotificationMessage>") {
		t.Fatalf("expected matching wsnt:NotificationMessage closing tag, got: %s", xmlOut)
	}
}

func TestPullMessagesResponseXMLPreservesRealSourceItemsAndOperation(t *testing.T) {
	t.Parallel()

	s := &onvifServer{cfg: onvifConfig{DeviceName: "front"}}

	xmlOut := s.pullMessagesResponseXML([]onvifEventMsg{
		{
			Topic:             "tns1:RuleEngine/MyRuleDetector/PeopleDetect",
			ItemName:          "State",
			ItemValue:         "true",
			UTCTime:           time.Now(),
			SourceItems:       map[string]string{"Source": "000"},
			PropertyOperation: "Changed",
		},
	}, 5*time.Second)

	if !strings.Contains(xmlOut, `PropertyOperation="Changed"`) {
		t.Fatalf("expected the real PropertyOperation to be preserved, got: %s", xmlOut)
	}
	if !strings.Contains(xmlOut, `<tt:SimpleItem Name="Source" Value="000"/>`) {
		t.Fatalf("expected the real Source item to be preserved, got: %s", xmlOut)
	}
	if strings.Contains(xmlOut, `Value="front"`) {
		t.Fatalf("did not expect the synthetic device-name fallback when real Source items are present, got: %s", xmlOut)
	}
}

func TestPullMessagesResponseXMLFallsBackWhenNoSourceItemsOrOperation(t *testing.T) {
	t.Parallel()

	s := &onvifServer{cfg: onvifConfig{DeviceName: "front"}}

	xmlOut := s.pullMessagesResponseXML([]onvifEventMsg{
		{Topic: "tns1:RuleEngine/CellMotionDetector/Motion", ItemName: "IsMotion", ItemValue: "true", UTCTime: time.Now()},
	}, 5*time.Second)

	if !strings.Contains(xmlOut, `PropertyOperation="Changed"`) {
		t.Fatalf("expected default PropertyOperation of Changed, got: %s", xmlOut)
	}
	if !strings.Contains(xmlOut, `<tt:SimpleItem Name="Source" Value="front"/>`) {
		t.Fatalf("expected the synthetic device-name fallback Source, got: %s", xmlOut)
	}
}

func TestParseISODurationClamped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw      string
		min, max time.Duration
		want     time.Duration
	}{
		{"PT30S", time.Second, time.Minute, 30 * time.Second},
		{"PT2M", time.Second, 5 * time.Minute, 2 * time.Minute},
		{"", time.Second, 30 * time.Second, 30 * time.Second},
		{"garbage", time.Second, 30 * time.Second, 30 * time.Second},
		{"PT1S", 5 * time.Second, 30 * time.Second, 5 * time.Second},
		{"PT5M", time.Second, 30 * time.Second, 30 * time.Second},
	}

	for _, c := range cases {
		got := parseISODurationClamped(c.raw, c.min, c.max)
		if got != c.want {
			t.Errorf("parseISODurationClamped(%q, %v, %v) = %v, want %v", c.raw, c.min, c.max, got, c.want)
		}
	}
}
