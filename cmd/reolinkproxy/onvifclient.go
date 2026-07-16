package main

import (
	"context"
	"crypto/rand"
	"crypto/sha1" //#nosec G505
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// cameraONVIFClient talks to a camera's own built-in ONVIF service - a different thing
// from the virtual per-camera ONVIF service this proxy exposes to NVRs. Used to forward the
// camera's own events (which may include smart/AI topics on models that expose them via
// ONVIF) and to fetch real snapshots for thumbnails.
type cameraONVIFClient struct {
	host     string
	baseURL  string
	username string
	password string
	http     *http.Client
}

func newCameraONVIFClient(host string, port int, username, password string) *cameraONVIFClient {
	if port == 0 {
		port = 8000
	}
	return &cameraONVIFClient{
		host:     host,
		baseURL:  fmt.Sprintf("http://%s:%d", host, port),
		username: username,
		password: password,
		// Must comfortably exceed the longest PullMessages long-poll timeout requested
		// (runCameraEventForwarder asks for PT20S) - a shorter client timeout aborts a
		// legitimately-slow-but-successful long-poll mid-flight with "context deadline
		// exceeded", which was causing constant resubscription. Each resubscribe made the
		// camera resend its full "Initialized" event burst, which then got forwarded again
		// - so this one bug was also the source of repeated/duplicate event spam.
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

type cameraCapabilities struct {
	EventsXAddr string
	MediaXAddr  string
}

// wsSecurityHeader builds an outgoing WS-Security UsernameToken digest header, the
// authentication scheme ONVIF cameras (including Reolink's) expect.
func wsSecurityHeader(username, password string) string {
	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce)
	nonceB64 := base64.StdEncoding.EncodeToString(nonce)
	created := time.Now().UTC().Format(time.RFC3339)

	h := sha1.New() //#nosec G401
	h.Write(nonce)
	h.Write([]byte(created))
	h.Write([]byte(password))
	digest := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return fmt.Sprintf(
		`<Security xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">`+
			`<UsernameToken><Username>%s</Username>`+
			`<Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">%s</Password>`+
			`<Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">%s</Nonce>`+
			`<Created xmlns="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">%s</Created>`+
			`</UsernameToken></Security>`,
		xmlEscape(username), digest, nonceB64, created,
	)
}

func (c *cameraONVIFClient) call(ctx context.Context, url, body string, auth bool) (string, error) {
	return c.callWithAction(ctx, url, "", body, auth)
}

// callWithAction is like call but also adds WS-Addressing To/Action headers when action is
// non-empty. Some cameras' PullPoint subscription-manager endpoints (as opposed to their
// main device/events service) strictly require these even though the main service doesn't.
func (c *cameraONVIFClient) callWithAction(ctx context.Context, url, action, body string, auth bool) (string, error) {
	header := ""
	if auth {
		header = wsSecurityHeader(c.username, c.password)
	}
	if action != "" {
		header = fmt.Sprintf(
			`<wsa:To xmlns:wsa="http://www.w3.org/2005/08/addressing">%s</wsa:To><wsa:Action xmlns:wsa="http://www.w3.org/2005/08/addressing">%s</wsa:Action>%s`,
			xmlEscape(url), xmlEscape(action), header,
		)
	}
	envelope := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?><soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">`+
			`<soap:Header>%s</soap:Header><soap:Body>%s</soap:Body></soap:Envelope>`,
		header, body,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(envelope))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/soap+xml; charset=utf-8")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		// These cameras' SOAP responses carry a huge, fixed xmlns preamble before the
		// actual body content (fault or otherwise), so keep the tail, not the head.
		snippet := string(data)
		if len(snippet) > 1000 {
			snippet = snippet[len(snippet)-1000:]
		}
		return "", fmt.Errorf("onvif call to %s failed: status %d: %s", url, resp.StatusCode, snippet)
	}
	return string(data), nil
}

// getCapabilities discovers the camera's own events/media service addresses. Different
// camera firmwares mount these at different paths (this Reolink camera's events service is
// at /onvif/event_service, singular - not /onvif/events_service like our own), so this must
// always be discovered rather than assumed.
func (c *cameraONVIFClient) getCapabilities(ctx context.Context) (cameraCapabilities, error) {
	body := `<GetCapabilities xmlns="http://www.onvif.org/ver10/device/wsdl"><Category>All</Category></GetCapabilities>`
	resp, err := c.call(ctx, c.baseURL+"/onvif/device_service", body, false)
	if err != nil {
		return cameraCapabilities{}, err
	}

	type capsEnvelope struct {
		Body struct {
			GetCapabilitiesResponse struct {
				Capabilities struct {
					Events struct {
						XAddr string `xml:"XAddr"`
					} `xml:"Events"`
					Media struct {
						XAddr string `xml:"XAddr"`
					} `xml:"Media"`
				} `xml:"Capabilities"`
			} `xml:"GetCapabilitiesResponse"`
		} `xml:"Body"`
	}

	var env capsEnvelope
	if err := xml.Unmarshal([]byte(resp), &env); err != nil {
		return cameraCapabilities{}, fmt.Errorf("parse GetCapabilities response: %w", err)
	}

	caps := env.Body.GetCapabilitiesResponse.Capabilities
	if caps.Events.XAddr == "" {
		return cameraCapabilities{}, fmt.Errorf("camera did not advertise an events service")
	}
	return cameraCapabilities{EventsXAddr: caps.Events.XAddr, MediaXAddr: caps.Media.XAddr}, nil
}

// cameraDeviceInfo is a camera's own reported ONVIF device identity.
type cameraDeviceInfo struct {
	Manufacturer    string
	Model           string
	FirmwareVersion string
	SerialNumber    string
	HardwareID      string
}

// getDeviceInformation fetches the camera's own real ONVIF device identity, so our virtual
// ONVIF device for it can report the same Manufacturer/Model/HardwareId rather than a generic
// placeholder - useful since some NVRs may behave differently for recognized hardware.
func (c *cameraONVIFClient) getDeviceInformation(ctx context.Context) (cameraDeviceInfo, error) {
	body := `<GetDeviceInformation xmlns="http://www.onvif.org/ver10/device/wsdl"/>`
	resp, err := c.call(ctx, c.baseURL+"/onvif/device_service", body, true)
	if err != nil {
		return cameraDeviceInfo{}, err
	}

	type infoEnvelope struct {
		Body struct {
			GetDeviceInformationResponse struct {
				Manufacturer    string `xml:"Manufacturer"`
				Model           string `xml:"Model"`
				FirmwareVersion string `xml:"FirmwareVersion"`
				SerialNumber    string `xml:"SerialNumber"`
				HardwareID      string `xml:"HardwareId"`
			} `xml:"GetDeviceInformationResponse"`
		} `xml:"Body"`
	}

	var env infoEnvelope
	if err := xml.Unmarshal([]byte(resp), &env); err != nil {
		return cameraDeviceInfo{}, fmt.Errorf("parse GetDeviceInformation response: %w", err)
	}

	info := env.Body.GetDeviceInformationResponse
	if info.Manufacturer == "" && info.Model == "" {
		return cameraDeviceInfo{}, fmt.Errorf("camera did not return device information")
	}
	return cameraDeviceInfo{
		Manufacturer:    info.Manufacturer,
		Model:           info.Model,
		FirmwareVersion: info.FirmwareVersion,
		SerialNumber:    info.SerialNumber,
		HardwareID:      info.HardwareID,
	}, nil
}

func (c *cameraONVIFClient) createPullPointSubscription(ctx context.Context, eventsXAddr string) (string, error) {
	body := `<CreatePullPointSubscription xmlns="http://www.onvif.org/ver10/events/wsdl"/>`
	resp, err := c.call(ctx, eventsXAddr, body, true)
	if err != nil {
		return "", err
	}

	type subEnvelope struct {
		Body struct {
			CreatePullPointSubscriptionResponse struct {
				SubscriptionReference struct {
					Address string `xml:"Address"`
				} `xml:"SubscriptionReference"`
			} `xml:"CreatePullPointSubscriptionResponse"`
		} `xml:"Body"`
	}

	var env subEnvelope
	if err := xml.Unmarshal([]byte(resp), &env); err != nil {
		return "", fmt.Errorf("parse CreatePullPointSubscription response: %w", err)
	}

	addr := env.Body.CreatePullPointSubscriptionResponse.SubscriptionReference.Address
	if addr == "" {
		return "", fmt.Errorf("camera did not return a subscription address")
	}
	return addr, nil
}

// forwardedEvent is one notification pulled from the camera's own ONVIF events service,
// kept generic (arbitrary Name/Value items) rather than assuming a fixed schema, since
// smart-event topics vary by camera model and firmware. SourceItems and Operation are
// preserved verbatim from the camera - real notifications carry meaningful Source items
// (e.g. a "Rule" token for Motion, a "Source" channel id for RuleEngine/MyRuleDetector/*
// topics) and a PropertyOperation ("Initialized"/"Changed"/"Deleted") that a downstream
// ONVIF consumer may require to accept/process the event at all.
type forwardedEvent struct {
	Topic             string
	Items             map[string]string
	SourceItems       map[string]string
	PropertyOperation string
	UTCTime           time.Time
}

func (c *cameraONVIFClient) pullMessages(ctx context.Context, subscriptionAddr string, timeout time.Duration, limit int) ([]forwardedEvent, error) {
	body := fmt.Sprintf(
		`<PullMessages xmlns="http://www.onvif.org/ver10/events/wsdl"><Timeout>PT%dS</Timeout><MessageLimit>%d</MessageLimit></PullMessages>`,
		int(timeout.Seconds()), limit,
	)
	const pullAction = "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription/PullMessagesRequest"
	resp, err := c.callWithAction(ctx, subscriptionAddr, pullAction, body, true)
	if err != nil {
		return nil, err
	}
	type simpleItem struct {
		Name  string `xml:"Name,attr"`
		Value string `xml:"Value,attr"`
	}
	type message struct {
		UtcTime           string `xml:"UtcTime,attr"`
		PropertyOperation string `xml:"PropertyOperation,attr"`
		Source            struct {
			SimpleItem []simpleItem `xml:"SimpleItem"`
		} `xml:"Source"`
		Data struct {
			SimpleItem []simpleItem `xml:"SimpleItem"`
		} `xml:"Data"`
	}
	type notification struct {
		Topic   string `xml:"Topic"`
		Message struct {
			Message message `xml:"Message"`
		} `xml:"Message"`
	}
	type pullEnvelope struct {
		Body struct {
			PullMessagesResponse struct {
				NotificationMessage []notification `xml:"NotificationMessage"`
			} `xml:"PullMessagesResponse"`
		} `xml:"Body"`
	}

	var env pullEnvelope
	if err := xml.Unmarshal([]byte(resp), &env); err != nil {
		return nil, fmt.Errorf("parse PullMessages response: %w", err)
	}

	events := make([]forwardedEvent, 0, len(env.Body.PullMessagesResponse.NotificationMessage))
	for _, n := range env.Body.PullMessagesResponse.NotificationMessage {
		ev := forwardedEvent{
			Topic:             strings.TrimSpace(n.Topic),
			Items:             make(map[string]string),
			PropertyOperation: n.Message.Message.PropertyOperation,
		}
		if t, err := time.Parse(time.RFC3339, n.Message.Message.UtcTime); err == nil {
			ev.UTCTime = t
		} else {
			ev.UTCTime = time.Now().UTC()
		}
		for _, item := range n.Message.Message.Data.SimpleItem {
			ev.Items[item.Name] = item.Value
		}
		if len(n.Message.Message.Source.SimpleItem) > 0 {
			ev.SourceItems = make(map[string]string, len(n.Message.Message.Source.SimpleItem))
			for _, item := range n.Message.Message.Source.SimpleItem {
				ev.SourceItems[item.Name] = item.Value
			}
		}
		if len(ev.Items) == 0 {
			continue
		}
		events = append(events, ev)
	}
	return events, nil
}

// getSnapshotURI fetches a snapshot URL from the camera's own media service for the first
// Snapshot fetches a fresh JPEG using Reolink's own HTTP snapshot API
// (cgi-bin/api.cgi?cmd=Snap), which takes credentials directly as query parameters. Much
// simpler than the generic ONVIF path (GetCapabilities -> GetProfiles -> GetSnapshotUri,
// then an HTTP Digest challenge/response against the resulting URL) and works the same way
// across Reolink models/firmwares, so there's no need to carry both.
func (c *cameraONVIFClient) Snapshot(ctx context.Context) ([]byte, string, error) {
	uri := fmt.Sprintf(
		"http://%s/cgi-bin/api.cgi?cmd=Snap&channel=0&user=%s&password=%s",
		c.host, url.QueryEscape(c.username), url.QueryEscape(c.password),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("fetch snapshot from camera: status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, "", err
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "image/jpeg"
	}
	return data, contentType, nil
}

// runCameraEventForwarder connects to the camera's own ONVIF events service and forwards
// every notification it receives into broker verbatim, so smart/AI topics the camera itself
// exposes (on models/firmwares that support it) reach NVRs through our own ONVIF Events
// service too, not just the basic Baichuan-derived motion signal.
func runCameraEventForwarder(ctx context.Context, cameraName string, client *cameraONVIFClient, broker *eventsBroker) {
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			caps, err := client.getCapabilities(ctx)
			if err != nil {
				broker.SetForwarderSubscribed(false)
				log.Warnf("camera onvif forwarder %s: get capabilities failed: %v, retrying in 30s", cameraName, err)
				if !sleepOrDone(ctx, 30*time.Second) {
					return
				}
				continue
			}

			subAddr, err := client.createPullPointSubscription(ctx, caps.EventsXAddr)
			if err != nil {
				broker.SetForwarderSubscribed(false)
				log.Warnf("camera onvif forwarder %s: create subscription failed: %v, retrying in 30s", cameraName, err)
				if !sleepOrDone(ctx, 30*time.Second) {
					return
				}
				continue
			}

			broker.SetForwarderSubscribed(true)
			log.Printf("camera onvif forwarder %s: subscribed to camera's own onvif events at %s", cameraName, caps.EventsXAddr)

			for ctx.Err() == nil {
				// NOTE: tried reducing this to 5s on the theory that the camera's own
				// subscription TTL (observed ~10s once) was expiring mid-long-poll. That
				// made the intermittent empty-reason 400 faults MORE frequent (as often as
				// every ~16s instead of every ~2.5-3min), not less - polling more often
				// looks more likely to be tripping some request-rate guard on the camera's
				// embedded ONVIF stack than fixing a TTL race. Reverted to 20s.
				events, err := client.pullMessages(ctx, subAddr, 20*time.Second, 20)
				if err != nil {
					// The subscription itself is usually still valid after one of these
					// transient empty-reason faults - retry once on the SAME subscription
					// before paying for a full resubscribe, which can make the camera
					// resend its whole "Initialized" event burst (see the http.Client
					// comment above).
					log.Warnf("camera onvif forwarder %s: pull failed: %v, retrying same subscription in 2s", cameraName, err)
					if !sleepOrDone(ctx, 2*time.Second) {
						return
					}
					events, err = client.pullMessages(ctx, subAddr, 20*time.Second, 20)
				}
				if err != nil {
					broker.SetForwarderSubscribed(false)
					log.Warnf("camera onvif forwarder %s: pull failed again: %v, resubscribing in 10s", cameraName, err)
					if !sleepOrDone(ctx, 10*time.Second) {
						return
					}
					break
				}

				for _, ev := range events {
					for name, value := range ev.Items {
						broker.Forward(onvifEventMsg{
							Topic:             ev.Topic,
							ItemName:          name,
							ItemValue:         value,
							UTCTime:           ev.UTCTime,
							SourceItems:       ev.SourceItems,
							PropertyOperation: ev.PropertyOperation,
						})
					}
				}
			}
		}
	}()
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
