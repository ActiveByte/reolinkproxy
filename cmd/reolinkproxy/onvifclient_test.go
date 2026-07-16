package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// realCameraCapabilitiesXML is a trimmed capture from a real Reolink camera's
// GetCapabilities response - notably its events service is mounted at
// /onvif/event_service (singular), not /onvif/events_service like this proxy's own
// server, which is exactly why discovery (rather than a hardcoded path) matters here.
const realCameraCapabilitiesXML = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:tt="http://www.onvif.org/ver10/schema" xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
<SOAP-ENV:Body><tds:GetCapabilitiesResponse><tds:Capabilities>
<tt:Device><tt:XAddr>http://10.0.30.4:8000/onvif/device_service</tt:XAddr></tt:Device>
<tt:Events><tt:XAddr>http://10.0.30.4:8000/onvif/event_service</tt:XAddr><tt:WSPullPointSupport>true</tt:WSPullPointSupport></tt:Events>
<tt:Media><tt:XAddr>http://10.0.30.4:8000/onvif/media_service</tt:XAddr></tt:Media>
</tds:Capabilities></tds:GetCapabilitiesResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

const realCameraCreateSubscriptionXML = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
<SOAP-ENV:Body><tev:CreatePullPointSubscriptionResponse>
<tev:SubscriptionReference><wsa:Address>http://10.0.30.4:8000/onvif/event_service?Idx=1</wsa:Address></tev:SubscriptionReference>
</tev:CreatePullPointSubscriptionResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

// realCameraMotionPullXML is captured (structurally) from a real Reolink camera's actual
// PullMessages response. Notably, real notifications carry meaningful <tt:Source> items
// (a "Rule" token for Motion, distinct from the generic single "Source" item used for
// RuleEngine/MyRuleDetector/* smart-detection topics) and PropertyOperation="Initialized"
// for the initial-state snapshot - both of which this client used to silently discard
// before forwarding, which turned out to be why a downstream ONVIF consumer accepted
// forwarded motion events but not smart-detection ones.
const realCameraMotionPullXML = `<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2" xmlns:tt="http://www.onvif.org/ver10/schema" xmlns:tev="http://www.onvif.org/ver10/events/wsdl" xmlns:tns1="http://www.onvif.org/ver10/topics">
<SOAP-ENV:Body><tev:PullMessagesResponse>
<wsnt:NotificationMessage>
<wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">tns1:RuleEngine/CellMotionDetector/Motion</wsnt:Topic>
<wsnt:Message><tt:Message UtcTime="2026-07-15T14:55:32Z" PropertyOperation="Initialized"><tt:Source><tt:SimpleItem Name="VideoSourceConfigurationToken" Value="000"/><tt:SimpleItem Name="VideoAnalyticsConfigurationToken" Value="000"/><tt:SimpleItem Name="Rule" Value="000"/></tt:Source><tt:Data><tt:SimpleItem Name="IsMotion" Value="true"/></tt:Data></tt:Message></wsnt:Message>
</wsnt:NotificationMessage>
<wsnt:NotificationMessage>
<wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">tns1:RuleEngine/MyRuleDetector/PeopleDetect</wsnt:Topic>
<wsnt:Message><tt:Message UtcTime="2026-07-15T14:55:32Z" PropertyOperation="Initialized"><tt:Source><tt:SimpleItem Name="Source" Value="000"/></tt:Source><tt:Data><tt:SimpleItem Name="State" Value="false"/></tt:Data></tt:Message></wsnt:Message>
</wsnt:NotificationMessage>
<wsnt:NotificationMessage>
<wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">tns1:Media/ConfigurationChanged</wsnt:Topic>
<wsnt:Message><tt:Message UtcTime="2026-07-15T14:55:33Z"><tt:Source><tt:SimpleItem Name="Token" Value="x"/></tt:Source></tt:Message></wsnt:Message>
</wsnt:NotificationMessage>
</tev:PullMessagesResponse></SOAP-ENV:Body></SOAP-ENV:Envelope>`

func TestCameraONVIFClientGetCapabilitiesParsesRealCameraResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, realCameraCapabilitiesXML)
	}))
	defer srv.Close()

	c := &cameraONVIFClient{baseURL: srv.URL, http: srv.Client()}
	caps, err := c.getCapabilities(context.Background())
	if err != nil {
		t.Fatalf("getCapabilities: %v", err)
	}
	if caps.EventsXAddr != "http://10.0.30.4:8000/onvif/event_service" {
		t.Fatalf("unexpected events XAddr: %q", caps.EventsXAddr)
	}
	if caps.MediaXAddr != "http://10.0.30.4:8000/onvif/media_service" {
		t.Fatalf("unexpected media XAddr: %q", caps.MediaXAddr)
	}
}

func TestCameraONVIFClientCreatePullPointSubscription(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, realCameraCreateSubscriptionXML)
	}))
	defer srv.Close()

	c := &cameraONVIFClient{baseURL: srv.URL, username: "admin", password: "secret", http: srv.Client()}
	addr, err := c.createPullPointSubscription(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("createPullPointSubscription: %v", err)
	}
	if addr != "http://10.0.30.4:8000/onvif/event_service?Idx=1" {
		t.Fatalf("unexpected subscription address: %q", addr)
	}
}

func TestCameraONVIFClientPullMessagesParsesMotionAndSkipsEmptyData(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, realCameraMotionPullXML)
	}))
	defer srv.Close()

	c := &cameraONVIFClient{baseURL: srv.URL, username: "admin", password: "secret", http: srv.Client()}
	events, err := c.pullMessages(context.Background(), srv.URL, 5*time.Second, 10)
	if err != nil {
		t.Fatalf("pullMessages: %v", err)
	}

	// The ConfigurationChanged notification only has Source items, no Data/SimpleItem, and
	// must be skipped rather than forwarded as an empty event.
	if len(events) != 2 {
		t.Fatalf("expected 2 events with data, got %d: %+v", len(events), events)
	}

	motion := events[0]
	if motion.Topic != "tns1:RuleEngine/CellMotionDetector/Motion" {
		t.Fatalf("unexpected topic: %q", motion.Topic)
	}
	if motion.Items["IsMotion"] != "true" {
		t.Fatalf("expected IsMotion=true, got %+v", motion.Items)
	}
	if motion.UTCTime.IsZero() {
		t.Fatal("expected a parsed UtcTime")
	}
	if motion.PropertyOperation != "Initialized" {
		t.Fatalf("expected PropertyOperation to be preserved, got %q", motion.PropertyOperation)
	}
	if motion.SourceItems["Rule"] != "000" || motion.SourceItems["VideoSourceConfigurationToken"] != "000" {
		t.Fatalf("expected real Source items to be preserved, got %+v", motion.SourceItems)
	}

	people := events[1]
	if people.Topic != "tns1:RuleEngine/MyRuleDetector/PeopleDetect" {
		t.Fatalf("unexpected topic: %q", people.Topic)
	}
	if people.SourceItems["Source"] != "000" {
		t.Fatalf("expected smart-detection Source item to be preserved, got %+v", people.SourceItems)
	}
}

func TestCameraONVIFClientSnapshotUsesReolinkQueryParamAuth(t *testing.T) {
	t.Parallel()

	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte{0xFF, 0xD8, 0xFF, 0xDB})
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	c := &cameraONVIFClient{host: host, username: "admin", password: "p@ss word", http: srv.Client()}

	data, contentType, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if contentType != "image/jpeg" {
		t.Fatalf("unexpected content type: %q", contentType)
	}
	if len(data) != 4 || data[0] != 0xFF || data[1] != 0xD8 {
		t.Fatalf("unexpected snapshot bytes: %v", data)
	}
	if !strings.Contains(gotQuery, "cmd=Snap") || !strings.Contains(gotQuery, "user=admin") {
		t.Fatalf("unexpected query: %q", gotQuery)
	}
	if !strings.Contains(gotQuery, "password=p%40ss+word") && !strings.Contains(gotQuery, "password=p%40ss%20word") {
		t.Fatalf("expected password to be query-escaped, got: %q", gotQuery)
	}
}

func TestCameraONVIFClientSnapshotSurfacesNon200Status(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	c := &cameraONVIFClient{host: host, username: "admin", password: "secret", http: srv.Client()}

	if _, _, err := c.Snapshot(context.Background()); err == nil {
		t.Fatal("expected an error for a non-200 snapshot response")
	}
}

func TestWSSecurityHeaderIsValidXMLAndIncludesUsername(t *testing.T) {
	t.Parallel()

	header := wsSecurityHeader("admin", "secret")
	if !strings.Contains(header, "<Username>admin</Username>") {
		t.Fatalf("expected header to include username, got: %s", header)
	}
	if !strings.Contains(header, "PasswordDigest") {
		t.Fatalf("expected header to use digest auth, got: %s", header)
	}
}
