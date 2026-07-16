package main

import (
	"bytes"
	"context"
	"crypto/sha1" //#nosec G505
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// soapBodyRE extracts the raw content of a SOAP envelope's <Body> element, namespace prefix
// agnostic (matches <Body>, <s:Body>, <SOAP-ENV:Body>, etc.).
var soapBodyRE = regexp.MustCompile(`(?s)<(?:[\w-]+:)?Body[^>]*>(.*)</(?:[\w-]+:)?Body>`)

// profileTokenRE extracts a <ProfileToken> element's value, namespace prefix agnostic.
var profileTokenRE = regexp.MustCompile(`<(?:[\w-]+:)?ProfileToken>([^<]+)<`)

// profileTokenNameRE pairs a Media2 profile's token attribute with its Name element, e.g.
// <ns1:Profiles token="000" fixed="true"><ns1:Name>Profile000_MainStream</ns1:Name>.
var profileTokenNameRE = regexp.MustCompile(`(?s)<(?:[\w-]+:)?Profiles\s+token="([^"]+)"[^>]*>\s*<(?:[\w-]+:)?Name>([^<]*)</(?:[\w-]+:)?Name>`)

// rtspURIExtractRE matches a whole rtsp:// URI (host[:port] plus path), used to locate and
// replace the stream URI in a proxied GetStreamUriResponse.
var rtspURIExtractRE = regexp.MustCompile(`rtsp://[^/\s"<]+(?:/[^\s"<]*)?`)

type onvifConfig struct {
	Address         string
	DevicePath      string
	MediaPath       string
	Media2Path      string
	EventsPath      string
	AdvertiseHost   string
	RTSPAddress     string
	RTSPPath        string
	DeviceName      string
	Manufacturer    string
	Model           string
	FirmwareVersion string
	SerialNumber    string
	HardwareID      string
	ProfileToken    string
	Username        string
	Password        string
	// HwAddress is a fake, stable per-camera identifier reported in
	// GetNetworkInterfaces/discovery scopes. Not a real network-layer MAC.
	HwAddress string
	// DeviceUUID is a stable per-camera identifier used for the ONVIF endpoint reference
	// and WS-Discovery ProbeMatch address. Must stay constant across restarts or NVRs such
	// as UniFi Protect will treat the camera as new on every change.
	DeviceUUID string
}

type onvifServer struct {
	cfg      onvifConfig
	metas    []*streamMetadata
	mux      *http.ServeMux
	events   *eventsBroker
	snapshot *cameraONVIFClient

	// tokenName caches the real camera's own Media2 ProfileToken -> local stream name
	// ("main"/"sub") mapping, learned from proxied GetProfiles responses. See
	// learnMediaTokenNames/resolveMetaForToken.
	tokenNameMu sync.Mutex
	tokenName   map[string]string
}

func newONVIFHandler(cfg onvifConfig, metas []*streamMetadata, events *eventsBroker, snapshot *cameraONVIFClient) http.Handler {
	mux := http.NewServeMux()
	server := &onvifServer{cfg: cfg, metas: metas, mux: mux, events: events, snapshot: snapshot, tokenName: make(map[string]string)}
	mux.HandleFunc(cfg.DevicePath, server.handleDevice)
	mux.HandleFunc(cfg.MediaPath, server.handleMedia)
	if cfg.Media2Path != "" {
		mux.HandleFunc(cfg.Media2Path, server.handleMedia2)
	}
	if cfg.EventsPath != "" {
		mux.HandleFunc(cfg.EventsPath, server.handleEvents)
		// Real Reolink cameras mount their events service at /onvif/event_service
		// (singular). Some ONVIF clients built against that convention hit it directly
		// instead of following the XAddr this server actually advertises via
		// GetCapabilities/GetServices, so alias it here too rather than relying on every
		// client to discover the path dynamically.
		if cfg.EventsPath != "/onvif/event_service" {
			mux.HandleFunc("/onvif/event_service", server.handleEvents)
		}
		mux.HandleFunc("/onvif/pullpoint/", server.handlePullPoint)
	}
	mux.HandleFunc("/api/snapshot/", server.handleSnapshot)
	return mux
}

func (s *onvifServer) authenticate(body string) bool {
	if s.cfg.Username == "" && s.cfg.Password == "" {
		return true
	}

	type Security struct {
		Username string `xml:"UsernameToken>Username"`
		Password string `xml:"UsernameToken>Password"`
		Nonce    string `xml:"UsernameToken>Nonce"`
		Created  string `xml:"UsernameToken>Created"`
	}
	type Envelope struct {
		Security Security `xml:"Header>Security"`
	}

	var env Envelope
	if err := xml.Unmarshal([]byte(body), &env); err != nil {
		log.Printf("onvif auth: xml unmarshal error: %v", err)
		return false
	}

	if env.Security.Username != s.cfg.Username {
		log.Printf("onvif auth: expected username %q, got %q", s.cfg.Username, env.Security.Username)
		return false
	}

	nonce, err := base64.StdEncoding.DecodeString(env.Security.Nonce)
	if err != nil {
		log.Printf("onvif auth: failed to decode nonce: %v", err)
		return false
	}

	h := sha1.New() //#nosec G401
	h.Write(nonce)
	h.Write([]byte(env.Security.Created))
	h.Write([]byte(s.cfg.Password))
	expected := base64.StdEncoding.EncodeToString(h.Sum(nil))

	if expected != env.Security.Password && env.Security.Password != s.cfg.Password {
		// Log failures carefully to avoid logging valid passwords if a user typoed or we misparsed it.
		log.Printf("onvif auth: digest mismatch. Expected: %s, Got: <redacted> (nonce base64: %s, username: %s)", expected, env.Security.Nonce, env.Security.Username)
		return false
	}

	return true
}

func (s *onvifServer) handleDevice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024)) // 1MB max payload
	if err != nil {
		writeSOAPFault(w, http.StatusBadRequest, "ter:InvalidArgVal", "failed to read request body")
		return
	}

	action := soapAction(r, string(body), []string{
		"GetCapabilities",
		"GetDeviceInformation",
		"GetScopes",
		"GetServices",
		"GetSystemDateAndTime",
		"GetNetworkInterfaces",
		"GetEndpointReference",
	})

	if action != "GetSystemDateAndTime" && !s.authenticate(string(body)) {
		writeSOAPFault(w, http.StatusUnauthorized, "ter:NotAuthorized", "The action requires authorization")
		return
	}

	switch action {
	case "GetCapabilities":
		writeSOAPResponse(w, s.deviceCapabilitiesResponse(r))
	case "GetDeviceInformation":
		writeSOAPResponse(w, s.deviceInformationResponse())
	case "GetScopes":
		writeSOAPResponse(w, s.deviceScopesResponse())
	case "GetServices":
		writeSOAPResponse(w, s.deviceServicesResponse(r))
	case "GetSystemDateAndTime":
		writeSOAPResponse(w, s.deviceSystemDateAndTimeResponse())
	case "GetNetworkInterfaces":
		writeSOAPResponse(w, s.deviceNetworkInterfacesResponse())
	case "GetEndpointReference":
		writeSOAPResponse(w, s.deviceEndpointReferenceResponse())
	default:
		log.Printf("onvif device: unsupported action %q (body: %s)", action, body)
		writeSOAPFault(w, http.StatusBadRequest, "ter:ActionNotSupported", "device action not supported")
	}
}

func (s *onvifServer) handleMedia(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024)) // 1MB max payload
	if err != nil {
		writeSOAPFault(w, http.StatusBadRequest, "ter:InvalidArgVal", "failed to read request body")
		return
	}

	if !s.authenticate(string(body)) {
		writeSOAPFault(w, http.StatusUnauthorized, "ter:NotAuthorized", "The action requires authorization")
		return
	}

	switch action := soapAction(r, string(body), []string{
		"GetAudioEncoderConfigurations",
		"GetAudioDecoderConfigurations",
		"GetAudioSources",
		"GetAudioOutputs",
		"GetAudioOutputConfigurations",
		"AddAudioOutputConfiguration",
		"AddAudioDecoderConfiguration",
		"SetSynchronizationPoint",
		"GetProfile",
		"GetProfiles",
		"GetServiceCapabilities",
		"GetStreamUri",
		"GetSnapshotUri",
		"GetVideoEncoderConfigurations",
		"GetVideoSources",
	}); action {
	case "GetProfiles":
		writeSOAPResponse(w, s.mediaProfilesResponse())
	case "GetProfile":
		writeSOAPResponse(w, s.mediaProfileResponse(string(body)))
	case "GetStreamUri":
		writeSOAPResponse(w, s.mediaStreamURIResponse(r, string(body)))
	case "GetSnapshotUri":
		writeSOAPResponse(w, s.mediaSnapshotURIResponse(r, string(body)))
	case "GetServiceCapabilities":
		writeSOAPResponse(w, `<trt:GetServiceCapabilitiesResponse><trt:Capabilities SnapshotUri="false" Rotation="false" VideoSourceMode="false" OSD="false" TemporaryOSDText="false" EXICompression="false"/></trt:GetServiceCapabilitiesResponse>`)
	case "GetVideoSources":
		writeSOAPResponse(w, s.mediaVideoSourcesResponse(string(body)))
	case "GetVideoEncoderConfigurations":
		writeSOAPResponse(w, s.mediaVideoEncoderConfigurationsResponse(string(body)))
	case "GetAudioSources":
		writeSOAPResponse(w, s.mediaAudioSourcesResponse(string(body)))
	case "GetAudioOutputs":
		writeSOAPResponse(w, s.mediaAudioOutputsResponse(string(body)))
	case "GetAudioOutputConfigurations":
		writeSOAPResponse(w, s.mediaAudioOutputConfigurationsResponse(string(body)))
	case "GetAudioEncoderConfigurations":
		writeSOAPResponse(w, s.mediaAudioEncoderConfigurationsResponse(string(body)))
	case "GetAudioDecoderConfigurations":
		writeSOAPResponse(w, s.mediaAudioDecoderConfigurationsResponse(string(body)))
	case "AddAudioOutputConfiguration", "AddAudioDecoderConfiguration", "SetSynchronizationPoint":
		// These configuration actions are effectively no-ops since the profiles are statically generated
		// and already include the audio output/decoder tokens. SetSynchronizationPoint (I-Frame request)
		// is ignored since the camera dictates keyframes or we'd need to send a Baichuan command.
		writeSOAPResponse(w, fmt.Sprintf(`<trt:%sResponse></trt:%sResponse>`, action, action))
	default:
		log.Printf("onvif media: unsupported action %q (body: %s)", action, body)
		writeSOAPFault(w, http.StatusBadRequest, "ter:ActionNotSupported", "media action not supported")
	}
}

// handleMedia2 transparently reverse-proxies every Media2 action to the camera's own real
// ONVIF service instead of hand-rolling response XML: the profiles/capabilities/encoder
// configs we synthesized never quite matched the real camera closely enough for UniFi
// Protect to offer a Low-quality stream, whereas relaying the real camera's own byte-for-byte
// responses (confirmed via a transparent MITM capture) worked correctly. GetSnapshotUri is
// the one exception - it must keep pointing at our own /api/snapshot/ proxy rather than
// leaking the real camera's direct URL/credentials to the NVR. Every response that contains
// an rtsp:// stream URI gets that URI's host/path swapped for our own RTSP server's, so media
// keeps flowing over this proxy's stable Baichuan-fed pipeline rather than the camera's own
// (less reliable) native RTSP.
func (s *onvifServer) handleMedia2(w http.ResponseWriter, r *http.Request) {
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

	if soapAction(r, string(body), []string{"GetSnapshotUri"}) == "GetSnapshotUri" {
		writeSOAPResponse(w, s.media2SnapshotURIResponse(r, string(body)))
		return
	}

	if s.snapshot == nil {
		writeSOAPFault(w, http.StatusInternalServerError, "ter:Action", "no upstream camera configured for this device")
		return
	}

	inner, ok := extractSOAPBody(string(body))
	if !ok {
		writeSOAPFault(w, http.StatusBadRequest, "ter:InvalidArgVal", "missing SOAP body")
		return
	}

	resp, err := s.snapshot.call(r.Context(), s.snapshot.baseURL+"/onvif/Media2", inner, true)
	if err != nil {
		log.Printf("onvif media2 proxy: camera=%s upstream call failed: %v", s.cfg.DeviceName, err)
		writeSOAPFault(w, http.StatusBadGateway, "ter:Action", "upstream camera request failed")
		return
	}

	s.learnMediaTokenNames(resp)

	if strings.Contains(resp, "rtsp://") {
		if token := extractProfileToken(inner); token != "" {
			if m := s.resolveMetaForToken(r.Context(), token); m != nil {
				newURI := buildURL("rtsp", s.authorityForRequest(r, s.cfg.RTSPAddress), m.path)
				if loc := rtspURIExtractRE.FindStringIndex(resp); loc != nil {
					resp = resp[:loc[0]] + newURI + resp[loc[1]:]
				}
			}
		}
	}

	writeRawSOAPResponse(w, resp)
}

// extractSOAPBody returns the raw content of a SOAP envelope's <Body> element (namespace
// prefix agnostic), used to forward just the method call - not the caller's own Security
// header - to the real camera, which gets re-signed with its own credentials separately.
func extractSOAPBody(raw string) (string, bool) {
	m := soapBodyRE.FindStringSubmatch(raw)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// extractProfileToken pulls the <ProfileToken> value out of a Media2 request body (namespace
// prefix agnostic).
func extractProfileToken(body string) string {
	m := profileTokenRE.FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m[1])
}

// learnMediaTokenNames watches every proxied GetProfilesResponse and remembers which of the
// real camera's own profile tokens is "main" vs "sub" (by the Main/Sub substring the real
// camera's own Profile Name always carries, e.g. "Profile000_MainStream"), so a later
// GetStreamUri response for that token can be resolved back to our own local stream/path
// without needing to guess or hardcode the real camera's token scheme.
func (s *onvifServer) learnMediaTokenNames(resp string) {
	matches := profileTokenNameRE.FindAllStringSubmatch(resp, -1)
	if len(matches) == 0 {
		return
	}

	s.tokenNameMu.Lock()
	defer s.tokenNameMu.Unlock()
	for _, m := range matches {
		token, name := m[1], strings.ToLower(m[2])
		switch {
		case strings.Contains(name, "main"):
			s.tokenName[token] = "main"
		case strings.Contains(name, "sub"):
			s.tokenName[token] = "sub"
		}
	}
}

// resolveMetaForToken maps a real camera ProfileToken (as seen in a GetStreamUri request) to
// our own local streamMetadata, using the cache learnMediaTokenNames built. On a cold cache
// (e.g. right after startup, before we've relayed a GetProfiles call) it fetches the real
// camera's profiles itself once to populate it.
func (s *onvifServer) resolveMetaForToken(ctx context.Context, token string) *streamMetadata {
	s.tokenNameMu.Lock()
	name, ok := s.tokenName[token]
	s.tokenNameMu.Unlock()

	if !ok {
		resp, err := s.snapshot.call(ctx, s.snapshot.baseURL+"/onvif/Media2", `<GetProfiles xmlns="http://www.onvif.org/ver20/media/wsdl"/>`, true)
		if err != nil {
			log.Printf("onvif media2 proxy: camera=%s failed to resolve profile token %q: %v", s.cfg.DeviceName, token, err)
			return nil
		}
		s.learnMediaTokenNames(resp)

		s.tokenNameMu.Lock()
		name, ok = s.tokenName[token]
		s.tokenNameMu.Unlock()
	}

	if !ok {
		return nil
	}
	if m := s.getMeta(name); m != nil && m.name == name {
		return m
	}
	return nil
}

func writeRawSOAPResponse(w http.ResponseWriter, raw string) {
	w.Header().Set("Content-Type", "application/soap+xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, raw)
}

// handleSnapshot proxies a real thumbnail from the camera's own ONVIF snapshot URI when
// available. Returning 404 (rather than a fabricated placeholder image) tells the NVR to
// fall back to grabbing a frame from the RTSP stream itself.
func (s *onvifServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.snapshot == nil {
		http.Error(w, "no snapshot source configured for this camera", http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	data, contentType, err := s.snapshot.Snapshot(ctx)
	if err != nil {
		log.Warnf("onvif snapshot: fetch failed for camera=%s: %v", s.cfg.DeviceName, err)
		http.Error(w, "failed to fetch snapshot from camera", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *onvifServer) deviceInformationResponse() string {
	return fmt.Sprintf(
		`<tds:GetDeviceInformationResponse><tds:Manufacturer>%s</tds:Manufacturer><tds:Model>%s</tds:Model><tds:FirmwareVersion>%s</tds:FirmwareVersion><tds:SerialNumber>%s</tds:SerialNumber><tds:HardwareId>%s</tds:HardwareId></tds:GetDeviceInformationResponse>`,
		xmlEscape(s.cfg.Manufacturer),
		xmlEscape(s.cfg.Model),
		xmlEscape(s.cfg.FirmwareVersion),
		xmlEscape(s.cfg.SerialNumber),
		xmlEscape(s.cfg.HardwareID),
	)
}

func (s *onvifServer) deviceServicesResponse(r *http.Request) string {
	deviceXAddr := xmlEscape(s.deviceServiceURL(r))
	mediaXAddr := xmlEscape(s.mediaServiceURL(r))
	media2XAddr := xmlEscape(s.media2ServiceURL(r))
	eventsXAddr := xmlEscape(s.eventsServiceURL(r))

	services := `<tds:GetServicesResponse>` +
		fmt.Sprintf(`<tds:Service><tds:Namespace>http://www.onvif.org/ver10/device/wsdl</tds:Namespace><tds:XAddr>%s</tds:XAddr><tds:Version><tt:Major>1</tt:Major><tt:Minor>0</tt:Minor></tds:Version></tds:Service>`, deviceXAddr) +
		fmt.Sprintf(`<tds:Service><tds:Namespace>http://www.onvif.org/ver10/media/wsdl</tds:Namespace><tds:XAddr>%s</tds:XAddr><tds:Version><tt:Major>1</tt:Major><tt:Minor>0</tt:Minor></tds:Version></tds:Service>`, mediaXAddr) +
		fmt.Sprintf(`<tds:Service><tds:Namespace>http://www.onvif.org/ver20/media/wsdl</tds:Namespace><tds:XAddr>%s</tds:XAddr><tds:Version><tt:Major>2</tt:Major><tt:Minor>0</tt:Minor></tds:Version></tds:Service>`, media2XAddr)
	if s.cfg.EventsPath != "" {
		services += fmt.Sprintf(`<tds:Service><tds:Namespace>http://www.onvif.org/ver10/events/wsdl</tds:Namespace><tds:XAddr>%s</tds:XAddr><tds:Version><tt:Major>1</tt:Major><tt:Minor>0</tt:Minor></tds:Version></tds:Service>`, eventsXAddr)
	}
	services += `</tds:GetServicesResponse>`
	return services
}

func (s *onvifServer) deviceCapabilitiesResponse(r *http.Request) string {
	deviceXAddr := xmlEscape(s.deviceServiceURL(r))
	mediaXAddr := xmlEscape(s.mediaServiceURL(r))

	eventsBlock := ""
	if s.cfg.EventsPath != "" {
		eventsBlock = fmt.Sprintf(
			`<tt:Events><tt:XAddr>%s</tt:XAddr><tt:WSPullPointSupport>true</tt:WSPullPointSupport><tt:WSSubscriptionPolicySupport>false</tt:WSSubscriptionPolicySupport></tt:Events>`,
			xmlEscape(s.eventsServiceURL(r)),
		)
	}

	return fmt.Sprintf(
		`<tds:GetCapabilitiesResponse><tds:Capabilities>`+
			`<tt:Device>`+
			`<tt:XAddr>%s</tt:XAddr>`+
			`<tt:Network><tt:IPFilter>false</tt:IPFilter><tt:ZeroConfiguration>false</tt:ZeroConfiguration><tt:IPVersion6>false</tt:IPVersion6><tt:DynDNS>false</tt:DynDNS></tt:Network>`+
			`<tt:System><tt:DiscoveryResolve>false</tt:DiscoveryResolve><tt:DiscoveryBye>false</tt:DiscoveryBye><tt:RemoteDiscovery>false</tt:RemoteDiscovery><tt:SystemBackup>false</tt:SystemBackup><tt:SystemLogging>false</tt:SystemLogging><tt:FirmwareUpgrade>false</tt:FirmwareUpgrade></tt:System>`+
			`<tt:IO><tt:InputConnectors>0</tt:InputConnectors><tt:RelayOutputs>0</tt:RelayOutputs></tt:IO>`+
			`<tt:Security><tt:TLS1.1>false</tt:TLS1.1><tt:TLS1.2>false</tt:TLS1.2><tt:OnboardKeyGeneration>false</tt:OnboardKeyGeneration><tt:AccessPolicyConfig>false</tt:AccessPolicyConfig><tt:X.509Token>false</tt:X.509Token><tt:SAMLToken>false</tt:SAMLToken><tt:KerberosToken>false</tt:KerberosToken><tt:RELToken>false</tt:RELToken></tt:Security>`+
			`</tt:Device>`+
			`<tt:Media>`+
			`<tt:XAddr>%s</tt:XAddr>`+
			`<tt:StreamingCapabilities><tt:RTPMulticast>false</tt:RTPMulticast><tt:RTP_TCP>true</tt:RTP_TCP><tt:RTP_RTSP_TCP>true</tt:RTP_RTSP_TCP></tt:StreamingCapabilities>`+
			`<tt:ProfileCapabilities><tt:MaximumNumberOfProfiles>%d</tt:MaximumNumberOfProfiles></tt:ProfileCapabilities>`+
			`</tt:Media>`+
			`%s`+
			`</tds:Capabilities></tds:GetCapabilitiesResponse>`,
		deviceXAddr,
		mediaXAddr,
		len(s.metas),
		eventsBlock,
	)
}

func (s *onvifServer) deviceScopesResponse() string {
	model := strings.ReplaceAll(strings.TrimSpace(s.cfg.Model), " ", "_")
	name := strings.ReplaceAll(strings.TrimSpace(s.cfg.DeviceName), " ", "_")

	return fmt.Sprintf(
		`<tds:GetScopesResponse>`+
			`<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>onvif://www.onvif.org/Profile/Streaming</tt:ScopeItem></tds:Scopes>`+
			`<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>onvif://www.onvif.org/Profile/S</tt:ScopeItem></tds:Scopes>`+
			`<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>onvif://www.onvif.org/Profile/T</tt:ScopeItem></tds:Scopes>`+
			`<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>onvif://www.onvif.org/type/video_encoder</tt:ScopeItem></tds:Scopes>`+
			`<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>onvif://www.onvif.org/hardware/%s</tt:ScopeItem></tds:Scopes>`+
			`<tds:Scopes><tt:ScopeDef>Fixed</tt:ScopeDef><tt:ScopeItem>onvif://www.onvif.org/name/%s</tt:ScopeItem></tds:Scopes>`+
			`</tds:GetScopesResponse>`,
		xmlEscape(model),
		xmlEscape(name),
	)
}

func (s *onvifServer) deviceSystemDateAndTimeResponse() string {
	now := time.Now().UTC()
	return fmt.Sprintf(
		`<tds:GetSystemDateAndTimeResponse><tds:SystemDateAndTime><tt:DateTimeType>NTP</tt:DateTimeType><tt:DaylightSavings>false</tt:DaylightSavings><tt:TimeZone><tt:TZ>UTC</tt:TZ></tt:TimeZone><tt:UTCDateTime><tt:Time><tt:Hour>%d</tt:Hour><tt:Minute>%d</tt:Minute><tt:Second>%d</tt:Second></tt:Time><tt:Date><tt:Year>%d</tt:Year><tt:Month>%d</tt:Month><tt:Day>%d</tt:Day></tt:Date></tt:UTCDateTime><tt:LocalDateTime><tt:Time><tt:Hour>%d</tt:Hour><tt:Minute>%d</tt:Minute><tt:Second>%d</tt:Second></tt:Time><tt:Date><tt:Year>%d</tt:Year><tt:Month>%d</tt:Month><tt:Day>%d</tt:Day></tt:Date></tt:LocalDateTime></tds:SystemDateAndTime></tds:GetSystemDateAndTimeResponse>`,
		now.Hour(), now.Minute(), now.Second(),
		now.Year(), int(now.Month()), now.Day(),
		now.Hour(), now.Minute(), now.Second(),
		now.Year(), int(now.Month()), now.Day(),
	)
}

func (s *onvifServer) deviceNetworkInterfacesResponse() string {
	host := "127.0.0.1"
	if s.cfg.AdvertiseHost != "" && s.cfg.AdvertiseHost != "0.0.0.0" && s.cfg.AdvertiseHost != "::" {
		host = s.cfg.AdvertiseHost
	} else if outbound := getOutboundIP(); outbound != "" {
		host = outbound
	} else if s.cfg.Address != "" {
		if parsedHost, _, err := net.SplitHostPort(s.cfg.Address); err == nil && parsedHost != "" && parsedHost != "0.0.0.0" && parsedHost != "::" {
			host = parsedHost
		}
	}

	hwAddress := s.cfg.HwAddress
	if hwAddress == "" {
		hwAddress = "00:00:00:00:00:00"
	}

	return fmt.Sprintf(`<tds:GetNetworkInterfacesResponse><tds:NetworkInterfaces token="eth0"><tt:Enabled>true</tt:Enabled><tt:Info><tt:Name>eth0</tt:Name><tt:HwAddress>%s</tt:HwAddress><tt:MTU>1500</tt:MTU></tt:Info><tt:IPv4><tt:Enabled>true</tt:Enabled><tt:Config><tt:Manual><tt:Address>%s</tt:Address><tt:PrefixLength>24</tt:PrefixLength></tt:Manual><tt:DHCP>false</tt:DHCP></tt:Config></tt:IPv4></tds:NetworkInterfaces></tds:GetNetworkInterfacesResponse>`, xmlEscape(hwAddress), xmlEscape(host))
}

func (s *onvifServer) deviceEndpointReferenceResponse() string {
	guid := s.cfg.DeviceUUID
	if guid == "" {
		guid = "00000000-0000-0000-0000-000000000000"
	}
	return fmt.Sprintf(
		`<tds:GetEndpointReferenceResponse><tds:GUID>urn:uuid:%s</tds:GUID></tds:GetEndpointReferenceResponse>`,
		xmlEscape(guid),
	)
}

func (s *onvifServer) getMeta(token string) *streamMetadata {
	for _, m := range s.metas {
		if m.token == token || m.name == token {
			return m
		}
	}
	if len(s.metas) > 0 {
		return s.metas[0]
	}
	return nil
}

func (s *onvifServer) mediaSnapshotURIResponse(r *http.Request, body string) string {
	token := s.extractToken(body)
	m := s.getMeta(token)

	// If we have metadata, we use the actual RTSP path since that's where the stream is mounted
	path := "camera/main"
	if m != nil && m.path != "" {
		path = m.path
	}

	return fmt.Sprintf(
		`<trt:GetSnapshotUriResponse><trt:MediaUri><tt:Uri>%s</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect><tt:InvalidAfterReboot>false</tt:InvalidAfterReboot><tt:Timeout>PT0S</tt:Timeout></trt:MediaUri></trt:GetSnapshotUriResponse>`,
		xmlEscape(buildURL("http", s.authorityForRequest(r, s.cfg.Address), fmt.Sprintf("/api/snapshot/%s", path))),
	)
}

func (s *onvifServer) media2SnapshotURIResponse(r *http.Request, body string) string {
	token := s.extractToken(body)
	m := s.getMeta(token)

	path := "camera/main"
	if m != nil && m.path != "" {
		path = m.path
	}

	return fmt.Sprintf(
		`<tr2:GetSnapshotUriResponse><tr2:Uri>%s</tr2:Uri></tr2:GetSnapshotUriResponse>`,
		xmlEscape(buildURL("http", s.authorityForRequest(r, s.cfg.Address), fmt.Sprintf("/api/snapshot/%s", path))),
	)
}

func (s *onvifServer) mediaProfilesResponse() string {
	var b strings.Builder
	b.WriteString(`<trt:GetProfilesResponse>`)
	for _, m := range s.metas {
		b.WriteString(s.profileXML("trt:Profiles", m.token, m))
	}
	b.WriteString(`</trt:GetProfilesResponse>`)
	return b.String()
}

func (s *onvifServer) mediaProfileResponse(body string) string {
	token := s.extractToken(body)
	m := s.getMeta(token)
	return `<trt:GetProfileResponse>` + s.profileXML("trt:Profile", token, m) + `</trt:GetProfileResponse>`
}

// extractOptionalElement does a namespace-agnostic search for <element>...</element> (or
// <prefix:element>...</prefix:element>) and returns its value and whether it was found at
// all - with no fallback, unlike extractToken below.
func extractOptionalElement(body, element string) (string, bool) {
	idx := strings.Index(body, ":"+element+">")
	if idx == -1 {
		idx = strings.Index(body, "<"+element+">")
	} else {
		// adjust idx to point exactly before the element name for parity
		idx++
	}

	if idx == -1 {
		return "", false
	}

	closeBracketIdx := idx + len(element)
	if closeBracketIdx >= len(body) || body[closeBracketIdx] != '>' {
		return "", false
	}
	valStart := closeBracketIdx + 1
	valEnd := strings.Index(body[valStart:], "<")
	if valEnd == -1 {
		return "", false
	}
	return body[valStart : valStart+valEnd], true
}

// extractToken is for REQUIRED token fields (e.g. GetStreamUri's ProfileToken) - callers
// need some token to act on even if parsing fails, so this falls back to the first
// configured stream rather than an empty string.
func (s *onvifServer) extractToken(body string) string {
	if value, ok := extractOptionalElement(body, "ProfileToken"); ok {
		return value
	}

	// default fallback
	if len(s.metas) > 0 {
		if s.metas[0].token != "" {
			return s.metas[0].token
		}
		return s.metas[0].name
	}
	return "main"
}

func (s *onvifServer) mediaStreamURIResponse(r *http.Request, body string) string {
	token := s.extractToken(body)
	m := s.getMeta(token)
	path := s.cfg.RTSPPath
	if m != nil && m.path != "" {
		path = m.path
	}

	return fmt.Sprintf(
		`<trt:GetStreamUriResponse><trt:MediaUri><tt:Uri>%s</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect><tt:InvalidAfterReboot>false</tt:InvalidAfterReboot><tt:Timeout>PT0S</tt:Timeout></trt:MediaUri></trt:GetStreamUriResponse>`,
		xmlEscape(buildURL("rtsp", s.authorityForRequest(r, s.cfg.RTSPAddress), path)),
	)
}

func (s *onvifServer) mediaVideoSourcesResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetVideoSourcesResponse>`)

	// Map to track cameras we've already added a VideoSource for
	added := make(map[string]bool)

	for _, m := range s.metas {
		if added[m.cameraName] {
			continue
		}
		added[m.cameraName] = true

		snap := m.snapshot().normalized()
		fmt.Fprintf(
			&b,
			`<trt:VideoSources token="VideoSource_%s"><tt:Framerate>%d</tt:Framerate><tt:Resolution><tt:Width>%d</tt:Width><tt:Height>%d</tt:Height></tt:Resolution></trt:VideoSources>`,
			xmlEscape(m.cameraName),
			snap.FPS,
			snap.Width,
			snap.Height,
		)
	}

	if len(s.metas) == 0 {
		b.WriteString(`<trt:VideoSources token="VideoSource_0"><tt:Framerate>15</tt:Framerate><tt:Resolution><tt:Width>3840</tt:Width><tt:Height>2160</tt:Height></tt:Resolution></trt:VideoSources>`)
	}

	b.WriteString(`</trt:GetVideoSourcesResponse>`)
	return b.String()
}

func (s *onvifServer) mediaVideoEncoderConfigurationsResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetVideoEncoderConfigurationsResponse>`)
	for _, m := range s.metas {
		token := m.token
		if token == "" {
			token = m.name
		}
		b.WriteString(s.videoEncoderConfigXML("trt:Configurations", token, m.cameraName, m.snapshot().normalized()))
	}
	b.WriteString(`</trt:GetVideoEncoderConfigurationsResponse>`)
	return b.String()
}

func (s *onvifServer) mediaAudioSourcesResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetAudioSourcesResponse>`)

	added := make(map[string]bool)
	hasAudio := false

	for _, m := range s.metas {
		if added[m.cameraName] {
			continue
		}
		added[m.cameraName] = true

		snap := m.snapshot().normalized()
		if snap.AudioCodec != "" {
			hasAudio = true
			fmt.Fprintf(
				&b,
				`<trt:AudioSources token="AudioSource_%s"><tt:Channels>%d</tt:Channels></trt:AudioSources>`,
				xmlEscape(m.cameraName),
				snap.AudioChannels,
			)
		}
	}

	if !hasAudio && len(s.metas) == 0 {
		return `<trt:GetAudioSourcesResponse/>`
	}

	b.WriteString(`</trt:GetAudioSourcesResponse>`)
	return b.String()
}

func (s *onvifServer) mediaAudioOutputsResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetAudioOutputsResponse>`)

	added := make(map[string]bool)

	for _, m := range s.metas {
		if added[m.cameraName] {
			continue
		}
		added[m.cameraName] = true

		// If a stream exists, assume the camera has a speaker for 2-way talk
		fmt.Fprintf(
			&b,
			`<trt:AudioOutputs token="AudioOutput_%s"></trt:AudioOutputs>`,
			xmlEscape(m.cameraName),
		)
	}

	b.WriteString(`</trt:GetAudioOutputsResponse>`)
	return b.String()
}

func (s *onvifServer) mediaAudioOutputConfigurationsResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetAudioOutputConfigurationsResponse>`)

	added := make(map[string]bool)

	for _, m := range s.metas {
		if added[m.cameraName] {
			continue
		}
		added[m.cameraName] = true

		token := "AudioOutputConfig_" + xmlEscape(m.cameraName)
		fmt.Fprintf(
			&b,
			`<trt:Configurations token="%s"><tt:Name>%s</tt:Name><tt:UseCount>1</tt:UseCount><tt:OutputToken>AudioOutput_%s</tt:OutputToken></trt:Configurations>`,
			token, token, xmlEscape(m.cameraName),
		)
	}

	b.WriteString(`</trt:GetAudioOutputConfigurationsResponse>`)
	return b.String()
}

func (s *onvifServer) mediaAudioDecoderConfigurationsResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetAudioDecoderConfigurationsResponse>`)

	added := make(map[string]bool)

	for _, m := range s.metas {
		if added[m.cameraName] {
			continue
		}
		added[m.cameraName] = true

		b.WriteString(s.audioDecoderConfigXML("trt:Configurations", m.cameraName, m.snapshot().normalized()))
	}

	b.WriteString(`</trt:GetAudioDecoderConfigurationsResponse>`)
	return b.String()
}

// mediaAudioEncoderConfigurationsResponse returns one entry per camera (not per profile),
// matching profileXML's shared per-camera AudioEncoderConfiguration token/content - real
// cameras likewise expose a single shared audio encoder config referenced by every profile.
func (s *onvifServer) mediaAudioEncoderConfigurationsResponse(_ string) string {
	var b strings.Builder
	b.WriteString(`<trt:GetAudioEncoderConfigurationsResponse>`)
	added := make(map[string]bool)
	for _, m := range s.metas {
		if added[m.cameraName] {
			continue
		}
		snap := m.snapshot().normalized()
		if snap.AudioCodec == "" {
			continue
		}
		added[m.cameraName] = true
		b.WriteString(s.audioEncoderConfigXML("trt:Configurations", m.cameraName, snap))
	}
	b.WriteString(`</trt:GetAudioEncoderConfigurationsResponse>`)
	return b.String()
}

func (s *onvifServer) audioDecoderConfigXML(tag string, token string, _ streamMetadataSnapshot) string {
	// Expose PCMU (G.711) as a supported decoder so the client knows how to send audio.
	return fmt.Sprintf(
		`<%s token="AudioDecoder_%s">`+
			`<tt:Name>AudioDecoder_%s</tt:Name>`+
			`<tt:UseCount>1</tt:UseCount>`+
			`</%s>`,
		tag,
		xmlEscape(token),
		xmlEscape(token),
		tag,
	)
}

func (s *onvifServer) profileXML(tag string, token string, m *streamMetadata) string {
	var snap streamMetadataSnapshot
	var cameraName string
	if m != nil {
		snap = m.snapshot().normalized()
		cameraName = m.cameraName
	} else {
		snap = streamMetadataSnapshot{}.normalized()
		cameraName = "0"
	}

	videoSourceToken := xmlEscape("VideoSource_" + cameraName)
	audioSourceToken := xmlEscape("AudioSource_" + cameraName)
	profileToken := xmlEscape(token)
	// Name follows the real camera's own convention (e.g. "Profile000_MainStream") rather
	// than DeviceName+"_"+token, which duplicated the camera name (token already contains
	// it - see onvifProfileToken) and didn't spell out which tier a profile actually was.
	name := xmlEscape(s.cameraProfileDisplayName(cameraName, snap))

	// VideoSourceConfiguration/AudioSourceConfiguration/AudioEncoderConfiguration describe
	// the camera's shared video/audio source and encoder, not this profile's own encoding -
	// they use a stable per-camera token (not per-profile) so every profile referencing the
	// same source reports it identically, matching real Reolink cameras (whose Main and Sub
	// profiles reference the exact same VideoSourceConfiguration/AudioEncoderConfiguration
	// token and content).
	videoSourceConfigToken := xmlEscape("VideoSourceConfig_" + cameraName)
	audioSourceConfigToken := xmlEscape("AudioSourceConfig_" + cameraName)
	nativeWidth, nativeHeight := s.cameraNativeResolution(cameraName)
	if nativeWidth == 0 {
		nativeWidth, nativeHeight = snap.Width, snap.Height
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<%s token="%s" fixed="true">`, tag, profileToken)
	fmt.Fprintf(&b, `<tt:Name>%s</tt:Name>`, name)

	// VideoSource
	fmt.Fprintf(&b, `<tt:VideoSourceConfiguration token="%s"><tt:Name>%s</tt:Name><tt:UseCount>1</tt:UseCount><tt:SourceToken>%s</tt:SourceToken><tt:Bounds x="0" y="0" width="%d" height="%d"/></tt:VideoSourceConfiguration>`, videoSourceConfigToken, videoSourceConfigToken, videoSourceToken, nativeWidth, nativeHeight)

	// AudioSource
	if snap.AudioCodec != "" {
		fmt.Fprintf(&b, `<tt:AudioSourceConfiguration token="%s"><tt:Name>%s</tt:Name><tt:UseCount>1</tt:UseCount><tt:SourceToken>%s</tt:SourceToken></tt:AudioSourceConfiguration>`, audioSourceConfigToken, audioSourceConfigToken, audioSourceToken)
	}

	// VideoEncoder - per profile, with Quality/BitrateLimit differentiated by tier.
	b.WriteString(s.videoEncoderConfigXML("tt:VideoEncoderConfiguration", token, cameraName, snap))

	// AudioEncoder - shared per camera, matching real cameras' single audio encoder config
	// referenced identically from every video profile.
	if snap.AudioCodec != "" {
		b.WriteString(s.audioEncoderConfigXML("tt:AudioEncoderConfiguration", cameraName, snap))
	}

	// VideoAnalytics + Metadata - present on every profile on real Reolink cameras (purely
	// informational; we don't run real analytics against them), so a client comparing our
	// profile shape against a real camera's sees the same set of configuration blocks.
	// AudioOutput/AudioDecoder (used for 2-way talk) are deliberately NOT included here: the
	// real camera's own profiles don't carry them either, and its 2-way talk still works - so
	// they're negotiated some other way (RTSP backchannel), not by being embedded in profiles.
	b.WriteString(videoAnalyticsConfigXML(cameraName))
	b.WriteString(metadataConfigXML(cameraName))

	fmt.Fprintf(&b, `</%s>`, tag)
	return b.String()
}

// cameraNativeResolution returns a camera's full sensor resolution (its highest-resolution
// configured stream). ONVIF VideoSourceConfiguration.Bounds describes the video source's
// frame geometry, not a profile's encoded output size, so every profile for a camera must
// report the same Bounds regardless of that profile's own encoder resolution - real Reolink
// cameras do exactly this (Sub profile still reports the Main profile's 3840x2160 Bounds).
func (s *onvifServer) cameraNativeResolution(cameraName string) (uint32, uint32) {
	var w, h uint32
	for _, meta := range s.metas {
		if meta.cameraName != cameraName {
			continue
		}
		snap := meta.snapshot().normalized()
		if snap.Width > w {
			w, h = snap.Width, snap.Height
		}
	}
	return w, h
}

// cameraStreamQuality returns a relative ONVIF Quality value for a stream, ranked against
// the other streams configured for the same camera. Confirmed against a real Reolink
// camera's own Media2 response (captured via a transparent relay): its Quality values are
// the OPPOSITE of what the ONVIF spec text ("normalized to start at 1, the lowest quality")
// would suggest - Main (the best/highest-res stream) reports 0, Sub (the worst) reports 2.
// Ranked ascending here to match: the highest-resolution stream gets 0, each lower tier gets
// a higher number.
func (s *onvifServer) cameraStreamQuality(cameraName string, width uint32) int {
	sorted := s.cameraStreamWidths(cameraName)
	if len(sorted) <= 1 {
		return 0
	}

	for rank, w := range sorted {
		if w != width {
			continue
		}
		return rank * 2
	}
	return 0
}

const (
	nativeBitrateLimitKbps = 8192
	minBitrateLimitKbps    = 128
)

// cameraStreamBitrateLimit scales a profile's advertised BitrateLimit down from the camera's
// native-resolution bitrate in proportion to its pixel count relative to the native
// resolution, roughly matching how real cameras report a much lower BitrateLimit for their
// Sub stream than their Main stream (rather than the same fixed value for every profile).
func (s *onvifServer) cameraStreamBitrateLimit(cameraName string, width, height uint32) uint32 {
	nativeWidth, nativeHeight := s.cameraNativeResolution(cameraName)
	nativeArea := float64(nativeWidth) * float64(nativeHeight)
	thisArea := float64(width) * float64(height)
	if nativeArea == 0 || thisArea == 0 {
		return nativeBitrateLimitKbps
	}

	bitrate := uint32(nativeBitrateLimitKbps * thisArea / nativeArea)
	if bitrate < minBitrateLimitKbps {
		bitrate = minBitrateLimitKbps
	}
	return bitrate
}

// cameraStreamWidths returns the distinct configured stream widths for a camera, sorted
// descending (highest/native resolution first).
func (s *onvifServer) cameraStreamWidths(cameraName string) []uint32 {
	widths := make(map[uint32]bool)
	for _, meta := range s.metas {
		if meta.cameraName != cameraName {
			continue
		}
		widths[meta.snapshot().normalized().Width] = true
	}

	sorted := make([]uint32, 0, len(widths))
	for w := range widths {
		sorted = append(sorted, w)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	return sorted
}

// cameraProfileDisplayName builds a profile Name in the real camera's own style, e.g.
// "Profile1_MainStream" / "Profile2_SubStream" - numbered by resolution rank (1 = highest/
// Main) with the stream tier spelled out, rather than a bare internal token, so it's obvious
// in an NVR's UI which profile is which.
func (s *onvifServer) cameraProfileDisplayName(cameraName string, snap streamMetadataSnapshot) string {
	rank := 0
	for i, w := range s.cameraStreamWidths(cameraName) {
		if w == snap.Width {
			rank = i
			break
		}
	}

	label := strings.ToLower(strings.TrimSpace(snap.Name))
	switch label {
	case "":
		label = "Stream"
	default:
		label = strings.ToUpper(label[:1]) + label[1:]
	}

	return fmt.Sprintf("Profile%d_%sStream", rank+1, label)
}

// videoAnalyticsConfigXML mirrors the VideoAnalyticsConfiguration block real Reolink cameras
// include on every profile (a CellMotionEngine module and CellMotionDetector rule) - purely
// informational (we don't run real analytics against it), matching how the real device's
// profiles are shaped rather than omitting the block entirely.
func videoAnalyticsConfigXML(token string) string {
	return fmt.Sprintf(
		`<tt:VideoAnalyticsConfiguration token="VideoAnalytics_%s"><tt:Name>VideoA_%s</tt:Name><tt:UseCount>1</tt:UseCount>`+
			`<tt:AnalyticsEngineConfiguration><tt:AnalyticsModule Type="CellMotionEngine" Name="MyCellMotionModule">`+
			`<tt:Parameters><tt:SimpleItem Name="Sensitivity" Value="60"/>`+
			`<tt:ElementItem Name="Layout"><tt:CellLayout Columns="22" Rows="18">`+
			`<tt:Transformation><tt:Translate x="-1" y="-1"/><tt:Scale x="0.00625" y="0.00834"/></tt:Transformation>`+
			`</tt:CellLayout></tt:ElementItem></tt:Parameters></tt:AnalyticsModule></tt:AnalyticsEngineConfiguration>`+
			`<tt:RuleEngineConfiguration><tt:Rule Type="CellMotionDetector" Name="MyMotionDetectorRule">`+
			`<tt:Parameters><tt:SimpleItem Name="MinCount" Value="20"/><tt:SimpleItem Name="AlarmOnDelay" Value="1000"/>`+
			`<tt:SimpleItem Name="AlarmOffDelay" Value="1000"/></tt:Parameters></tt:Rule></tt:RuleEngineConfiguration>`+
			`</tt:VideoAnalyticsConfiguration>`,
		xmlEscape(token), xmlEscape(token),
	)
}

// metadataConfigXML mirrors the MetadataConfiguration block real Reolink cameras include on
// every profile.
func metadataConfigXML(token string) string {
	return fmt.Sprintf(
		`<tt:MetadataConfiguration token="Metadata_%s"><tt:Name>Metadata_%s</tt:Name><tt:UseCount>1</tt:UseCount>`+
			`<tt:PTZStatus><tt:Status>false</tt:Status><tt:Position>false</tt:Position></tt:PTZStatus><tt:Analytics>false</tt:Analytics>`+
			`<tt:Multicast><tt:Address><tt:Type>IPv4</tt:Type><tt:IPv4Address>224.2.0.0</tt:IPv4Address></tt:Address><tt:Port>40020</tt:Port><tt:TTL>64</tt:TTL><tt:AutoStart>true</tt:AutoStart></tt:Multicast>`+
			`<tt:SessionTimeout>PT60S</tt:SessionTimeout></tt:MetadataConfiguration>`,
		xmlEscape(token), xmlEscape(token),
	)
}

func onvifProfileToken(cameraName string, streamName string) string {
	replacer := strings.NewReplacer(" ", "_", "/", "_", "\\", "_")
	cameraName = replacer.Replace(strings.TrimSpace(cameraName))
	streamName = replacer.Replace(strings.TrimSpace(streamName))

	if cameraName == "" {
		cameraName = "camera"
	}
	if streamName == "" {
		streamName = "main"
	}

	return cameraName + "_" + streamName
}

func (s *onvifServer) videoEncoderConfigXML(tag string, token string, cameraName string, snap streamMetadataSnapshot) string {
	encoding := snap.VideoCodec
	if encoding == "" {
		encoding = "H265"
	}
	quality := s.cameraStreamQuality(cameraName, snap.Width)
	bitrate := s.cameraStreamBitrateLimit(cameraName, snap.Width, snap.Height)
	govLength := uint32(snap.FPS)
	if govLength == 0 {
		govLength = 25
	}

	return fmt.Sprintf(
		`<%s token="VideoEncoder_%s"><tt:Name>VideoE_%s</tt:Name><tt:UseCount>1</tt:UseCount><tt:Encoding>%s</tt:Encoding><tt:Resolution><tt:Width>%d</tt:Width><tt:Height>%d</tt:Height></tt:Resolution><tt:Quality>%d</tt:Quality><tt:RateControl><tt:FrameRateLimit>%d</tt:FrameRateLimit><tt:EncodingInterval>1</tt:EncodingInterval><tt:BitrateLimit>%d</tt:BitrateLimit></tt:RateControl><tt:%s><tt:GovLength>%d</tt:GovLength><tt:%sProfile>Main</tt:%sProfile></tt:%s><tt:Multicast><tt:Address><tt:Type>IPv4</tt:Type><tt:IPv4Address>239.0.1.0</tt:IPv4Address></tt:Address><tt:Port>4000</tt:Port><tt:TTL>64</tt:TTL><tt:AutoStart>false</tt:AutoStart></tt:Multicast><tt:SessionTimeout>PT10S</tt:SessionTimeout></%s>`,
		tag,
		xmlEscape(token),
		xmlEscape(token),
		encoding,
		snap.Width,
		snap.Height,
		quality,
		snap.FPS,
		bitrate,
		encoding,
		govLength,
		encoding,
		encoding,
		encoding,
		tag,
	)
}

func (s *onvifServer) audioEncoderConfigXML(tag string, token string, snap streamMetadataSnapshot) string {
	if snap.AudioSampleRate == 0 {
		snap.AudioSampleRate = 16000
	}
	if snap.AudioChannels == 0 {
		snap.AudioChannels = 1
	}

	encoding := snap.AudioCodec
	if encoding == "" {
		encoding = "AAC"
	}
	// G711 must be G711 according to ONVIF
	if encoding == "PCMA" || encoding == "PCMU" {
		encoding = "G711"
	}

	// ONVIF's AudioEncoderConfiguration.SampleRate is defined in kHz, not Hz - real cameras
	// report e.g. 16 for a 16000Hz stream.
	sampleRateKHz := snap.AudioSampleRate / 1000
	if sampleRateKHz == 0 {
		sampleRateKHz = 16
	}

	return fmt.Sprintf(
		`<%s token="AudioEncoder_%s"><tt:Name>AudioE_%s</tt:Name><tt:UseCount>1</tt:UseCount><tt:Encoding>%s</tt:Encoding><tt:Bitrate>64</tt:Bitrate><tt:SampleRate>%d</tt:SampleRate><tt:Multicast><tt:Address><tt:Type>IPv4</tt:Type><tt:IPv4Address>238.255.255.255</tt:IPv4Address></tt:Address><tt:Port>25320</tt:Port><tt:TTL>60</tt:TTL><tt:AutoStart>false</tt:AutoStart></tt:Multicast><tt:SessionTimeout>PT0S</tt:SessionTimeout></%s>`,
		tag,
		xmlEscape(token),
		xmlEscape(token),
		encoding,
		sampleRateKHz,
		tag,
	)
}

func (s *onvifServer) deviceServiceURL(r *http.Request) string {
	return buildURL("http", s.authorityForRequest(r, s.cfg.Address), s.cfg.DevicePath)
}

func (s *onvifServer) mediaServiceURL(r *http.Request) string {
	return buildURL("http", s.authorityForRequest(r, s.cfg.Address), s.cfg.MediaPath)
}

func (s *onvifServer) media2ServiceURL(r *http.Request) string {
	path := s.cfg.Media2Path
	if path == "" {
		path = "/onvif/media2_service"
	}
	return buildURL("http", s.authorityForRequest(r, s.cfg.Address), path)
}

func (s *onvifServer) eventsServiceURL(r *http.Request) string {
	path := s.cfg.EventsPath
	if path == "" {
		path = "/onvif/events_service"
	}
	return buildURL("http", s.authorityForRequest(r, s.cfg.Address), path)
}

func (s *onvifServer) authorityForRequest(r *http.Request, listenAddr string) string {
	if s.cfg.AdvertiseHost != "" {
		return advertisedAuthority(listenAddr, s.cfg.AdvertiseHost)
	}

	if r != nil && r.Host != "" {
		host := r.Host
		if parsedHost, _, err := net.SplitHostPort(r.Host); err == nil {
			host = parsedHost
		}
		return advertisedAuthority(listenAddr, host)
	}

	return advertisedAuthority(listenAddr, "")
}

func soapAction(r *http.Request, body string, known []string) string {
	if raw := strings.Trim(strings.TrimSpace(r.Header.Get("SOAPAction")), `"`); raw != "" {
		if idx := strings.LastIndexAny(raw, "/#"); idx >= 0 && idx < len(raw)-1 {
			// Some clients send a quoted SOAPAction like "http://www.onvif.org/ver10/media/wsdl/GetStreamUri"
			// Extracting the final part of the URL path as the action name
			action := raw[idx+1:]

			// Some NVRs prefix it with trt: or tr2: like "trt:GetStreamUri" in the header!
			if colonIdx := strings.IndexByte(action, ':'); colonIdx >= 0 {
				action = action[colonIdx+1:]
			}
			// Some (gSOAP-generated) clients use the WSDL input-message name in
			// SOAPAction, which by convention is the operation name plus "Request"
			// (e.g. "CreatePullPointSubscriptionRequest"), not the bare operation name.
			// None of our known action names end in "Request" themselves, so trimming
			// it unconditionally is safe.
			action = strings.TrimSuffix(action, "Request")
			return action
		}

		// Un-namespaced raw action
		if colonIdx := strings.IndexByte(raw, ':'); colonIdx >= 0 {
			raw = raw[colonIdx+1:]
		}
		return raw
	}

	// Sort known actions by length descending so longer matching strings win
	// e.g. "GetProfiles" matched before "GetProfile"
	for i := 0; i < len(known); i++ {
		for j := i + 1; j < len(known); j++ {
			if len(known[i]) < len(known[j]) {
				known[i], known[j] = known[j], known[i]
			}
		}
	}

	for _, action := range known {
		if hasSOAPActionBody(body, action) {
			return action
		}
	}
	return ""
}

func hasSOAPActionBody(body string, action string) bool {
	patterns := []string{
		":" + action + ">",
		":" + action + " ",
		":" + action + "/",
		"<" + action + ">",
		"<" + action + " ",
		"<" + action + "/",
		action, // fallback just in case namespace is completely omitted or weird
	}

	for _, pattern := range patterns {
		if strings.Contains(body, pattern) {
			return true
		}
	}

	return false
}

func writeSOAPResponse(w http.ResponseWriter, inner string) {
	w.Header().Set("Content-Type", `application/soap+xml; charset=utf-8`)
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, soapEnvelope(inner))
}

func writeSOAPFault(w http.ResponseWriter, statusCode int, subcode string, reason string) {
	w.Header().Set("Content-Type", `application/soap+xml; charset=utf-8`)
	w.WriteHeader(statusCode)
	_, _ = io.WriteString(w, soapEnvelope(
		fmt.Sprintf(
			`<soap:Fault><soap:Code><soap:Value>soap:Sender</soap:Value><soap:Subcode><soap:Value>%s</soap:Value></soap:Subcode></soap:Code><soap:Reason><soap:Text xml:lang="en">%s</soap:Text></soap:Reason></soap:Fault>`,
			xmlEscape(subcode),
			xmlEscape(reason),
		),
	))
}

func soapEnvelope(inner string) string {
	// wsa here MUST be the WS-Addressing 1.0 namespace (2005/08), not the older 2004/08
	// one - ONVIF's Events/WS-Notification WSDL specifies 2005/08, and the real camera's
	// own CreatePullPointSubscriptionResponse uses it too (as wsa5:Address). UniFi
	// Protect's own ONVIF client tolerates either since it matches by local name, but a
	// strict/gSOAP-generated client will fail to recognize wsa:Address at all if the
	// namespace doesn't match, since XML-namespace-aware parsers bind by URI, not just tag
	// name - this caused exactly that kind of client to fail to parse subscription
	// responses from this server before the fix.
	return `<?xml version="1.0" encoding="UTF-8"?>` +
		`<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:tds="http://www.onvif.org/ver10/device/wsdl" xmlns:trt="http://www.onvif.org/ver10/media/wsdl" xmlns:tr2="http://www.onvif.org/ver20/media/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema" xmlns:ter="http://www.onvif.org/ver10/error" xmlns:tev="http://www.onvif.org/ver10/events/wsdl" xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:tns1="http://www.onvif.org/ver10/topics" xmlns:wstop="http://docs.oasis-open.org/wsn/t-1">` +
		`<soap:Body>` + inner + `</soap:Body></soap:Envelope>`
}

func xmlEscape(v string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(v))
	return buf.String()
}
