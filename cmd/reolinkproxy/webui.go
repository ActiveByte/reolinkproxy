package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// processStartTime is used to report server uptime in the status API. Set at package init,
// which is close enough to real process start for a diagnostics figure.
var processStartTime = time.Now()

// cameraStatus is the live, in-memory runtime state for one camera, registered once at
// startup by runApp. The web UI reads it read-only; it never mutates camera config.
type cameraStatus struct {
	Name        string
	Host        string
	ONVIFPort   int
	ONVIFMAC    string
	ONVIFSerial string
	// ONVIFAuthority is the host:port an ONVIF client (e.g. UniFi Protect) should actually
	// connect to - the proxy's own advertised address, not the camera's LAN IP.
	ONVIFAuthority string

	device *CameraDevice
	metas  []*streamMetadata
	motion *cameraMotionState
	events *eventsBroker
}

type statusRegistry struct {
	mu         sync.RWMutex
	byName     map[string]*cameraStatus
	rtspServer *rtspServerHandler
}

func newStatusRegistry(rtspServer *rtspServerHandler) *statusRegistry {
	return &statusRegistry{byName: make(map[string]*cameraStatus), rtspServer: rtspServer}
}

func (r *statusRegistry) register(cs *cameraStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byName[cs.Name] = cs
}

type streamStatusView struct {
	Path   string `json:"path"`
	Width  uint32 `json:"width"`
	Height uint32 `json:"height"`
	FPS    uint8  `json:"fps"`
	Codec  string `json:"codec"`
}

type motionStatusView struct {
	Known       bool `json:"known"`
	Active      bool `json:"active"`
	Unsupported bool `json:"unsupported"`
}

// clientView identifies one RTSP client currently pulling a stream. Path identifies which
// stream tier (e.g. testcam/stream_main vs testcam/stream_sub) it's connected to, so it's
// possible to confirm an NVR is actually pulling the tier it claims to have selected.
type clientView struct {
	IP          string    `json:"ip"`
	Path        string    `json:"path"`
	ConnectedAt time.Time `json:"connected_at"`
}

// subscriberView identifies one live ONVIF PullPoint subscriber.
type subscriberView struct {
	IP        string    `json:"ip"`
	CreatedAt time.Time `json:"created_at"`
	LastSeen  time.Time `json:"last_seen"`
}

type cameraStatusView struct {
	Name                string           `json:"name"`
	Host                string           `json:"host"`
	Connected           bool             `json:"connected"`
	ONVIFPort           int              `json:"onvif_port"`
	ONVIFMAC            string           `json:"onvif_mac"`
	ONVIFSerial         string           `json:"onvif_serial"`
	ONVIFAuthority      string           `json:"onvif_authority"`
	EventSubscribers    int              `json:"event_subscribers"`
	EventSubscriberList []subscriberView `json:"event_subscriber_list"`
	RTSPClients         []clientView     `json:"rtsp_clients"`
	// ProtectConnected is true when protectIP (the configured NVR IP passed to snapshot) has
	// an active RTSP client or ONVIF event subscription against this camera right now.
	ProtectConnected bool               `json:"protect_connected"`
	Streams          []streamStatusView `json:"streams"`
	Motion           motionStatusView   `json:"motion"`
}

// snapshot builds the current status view for every registered camera. protectIP, if set, is
// compared (host-only, ignoring source port) against every RTSP client and ONVIF event
// subscriber IP to populate ProtectConnected - letting the UI show whether the configured NVR
// is actually talking to a given camera right now, not just whether the camera itself is
// reachable.
func (r *statusRegistry) snapshot(protectIP string) []cameraStatusView {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	sort.Strings(names)

	views := make([]cameraStatusView, 0, len(names))
	for _, name := range names {
		cs := r.byName[name]
		view := cameraStatusView{
			Name:           cs.Name,
			Host:           cs.Host,
			ONVIFPort:      cs.ONVIFPort,
			ONVIFMAC:       cs.ONVIFMAC,
			ONVIFSerial:    cs.ONVIFSerial,
			ONVIFAuthority: cs.ONVIFAuthority,
		}
		if cs.device != nil {
			view.Connected = cs.device.Connected()
		}
		if cs.events != nil {
			view.EventSubscribers = cs.events.SubscriberCount()
			for _, sub := range cs.events.Subscribers() {
				view.EventSubscriberList = append(view.EventSubscriberList, subscriberView{
					IP:        hostOnly(sub.RemoteAddr),
					CreatedAt: sub.CreatedAt,
					LastSeen:  sub.LastSeen,
				})
			}
		}
		seenClient := make(map[string]bool)
		for _, m := range cs.metas {
			snap := m.snapshot()
			view.Streams = append(view.Streams, streamStatusView{
				Path:   snap.Path,
				Width:  snap.Width,
				Height: snap.Height,
				FPS:    snap.FPS,
				Codec:  snap.VideoCodec,
			})
			if r.rtspServer == nil {
				continue
			}
			stream := r.rtspServer.getStream(snap.Path)
			if stream == nil {
				continue
			}
			for _, c := range stream.Clients() {
				ip := hostOnly(c.RemoteAddr)
				key := ip + "@" + snap.Path
				if seenClient[key] {
					continue
				}
				seenClient[key] = true
				view.RTSPClients = append(view.RTSPClients, clientView{IP: ip, Path: snap.Path, ConnectedAt: c.ConnectedAt})
			}
		}
		if cs.motion != nil {
			snap := cs.motion.snapshotCopy()
			view.Motion = motionStatusView{Known: snap.Known, Active: snap.Active, Unsupported: snap.Unsupported}
		}
		if protectIP != "" {
			for _, c := range view.RTSPClients {
				if c.IP == protectIP {
					view.ProtectConnected = true
				}
			}
			for _, s := range view.EventSubscriberList {
				if s.IP == protectIP {
					view.ProtectConnected = true
				}
			}
		}
		views = append(views, view)
	}
	return views
}

func (r *statusRegistry) getEvents(name string) *eventsBroker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cs := r.byName[name]
	if cs == nil {
		return nil
	}
	return cs.events
}

// webUIServer serves the status/config dashboard and its JSON API. Camera mutations are
// persisted to the config file immediately but only take effect for the running Baichuan/
// RTSP/ONVIF sessions after a process restart - tearing those down and rebuilding them live
// is real future work, not something worth faking here. requestRestart triggers that
// restart (graceful shutdown + self-respawn) from the UI instead of requiring a human at a
// terminal.
type webUIServer struct {
	store          *ConfigStore
	status         *statusRegistry
	onvifBasePort  int
	requestRestart func()
}

func newWebUIHandler(store *ConfigStore, status *statusRegistry, onvifBasePort int, requestRestart func()) http.Handler {
	h := &webUIServer{store: store, status: status, onvifBasePort: onvifBasePort, requestRestart: requestRestart}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", h.handleIndex)
	mux.HandleFunc("GET /api/status", h.handleStatus)
	mux.HandleFunc("GET /api/server-stats", h.handleServerStats)
	mux.HandleFunc("GET /api/settings", h.handleGetSettings)
	mux.HandleFunc("PUT /api/settings", h.handleUpdateSettings)
	mux.HandleFunc("GET /api/cameras", h.handleListCameras)
	mux.HandleFunc("POST /api/cameras", h.handleCreateCamera)
	mux.HandleFunc("PUT /api/cameras/{name}", h.handleUpdateCamera)
	mux.HandleFunc("DELETE /api/cameras/{name}", h.handleDeleteCamera)
	mux.HandleFunc("GET /api/cameras/{name}/events", h.handleCameraEvents)
	mux.HandleFunc("POST /api/cameras/{name}/test-event", h.handleTestEvent)
	mux.HandleFunc("POST /api/restart", h.handleRestart)
	return mux
}

// handleCameraEvents returns the camera's recent event log (motion, forwarded from the
// camera's own ONVIF events service, and synthetic test events) so AI/smart events can be
// visually verified from the browser without a real NVR.
func (h *webUIServer) handleCameraEvents(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	broker := h.status.getEvents(name)
	if broker == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Errorf("camera %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, broker.RecentEvents())
}

type testEventRequest struct {
	Topic     string `json:"topic"`
	ItemName  string `json:"item_name"`
	ItemValue string `json:"item_value"`
}

// handleTestEvent injects a synthetic ONVIF event for one camera, so the whole pipeline
// (this service's PullPoint -> the NVR) can be verified without waiting on real camera
// motion/AI behavior.
func (h *webUIServer) handleTestEvent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	broker := h.status.getEvents(name)
	if broker == nil {
		writeJSONError(w, http.StatusNotFound, fmt.Errorf("camera %q not found", name))
		return
	}

	var req testEventRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Topic == "" {
		req.Topic = "tns1:RuleEngine/CellMotionDetector/Motion"
	}
	if req.ItemName == "" {
		req.ItemName = "IsMotion"
	}
	if req.ItemValue == "" {
		req.ItemValue = "true"
	}

	broker.InjectTest(req.Topic, req.ItemName, req.ItemValue)
	writeJSON(w, http.StatusOK, map[string]string{"status": "sent"})
}

func (h *webUIServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, indexHTML)
}

func (h *webUIServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.status.snapshot(h.store.ProtectServerIP()))
}

type serverStatsView struct {
	UptimeSeconds int64   `json:"uptime_seconds"`
	Goroutines    int     `json:"goroutines"`
	MemAllocMB    float64 `json:"mem_alloc_mb"`
	MemSysMB      float64 `json:"mem_sys_mb"`
	NumGC         uint32  `json:"num_gc"`
	GoVersion     string  `json:"go_version"`
	NumCPU        int     `json:"num_cpu"`
	NumCameras    int     `json:"num_cameras"`
}

func (h *webUIServer) handleServerStats(w http.ResponseWriter, _ *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	writeJSON(w, http.StatusOK, serverStatsView{
		UptimeSeconds: int64(time.Since(processStartTime).Seconds()),
		Goroutines:    runtime.NumGoroutine(),
		MemAllocMB:    float64(mem.Alloc) / (1024 * 1024),
		MemSysMB:      float64(mem.Sys) / (1024 * 1024),
		NumGC:         mem.NumGC,
		GoVersion:     runtime.Version(),
		NumCPU:        runtime.NumCPU(),
		NumCameras:    len(h.store.Cameras()),
	})
}

type settingsView struct {
	ProtectServerIP string `json:"protect_server_ip"`
}

func (h *webUIServer) handleGetSettings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, settingsView{ProtectServerIP: h.store.ProtectServerIP()})
}

// handleUpdateSettings takes effect immediately - unlike camera edits it needs no restart,
// since it's purely informational (the connected/offline indicator), not part of ONVIF/RTSP
// negotiation.
func (h *webUIServer) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var req settingsView
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if err := h.store.SetProtectServerIP(strings.TrimSpace(req.ProtectServerIP)); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, settingsView{ProtectServerIP: h.store.ProtectServerIP()})
}

func (h *webUIServer) handleListCameras(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.store.Cameras())
}

func (h *webUIServer) handleCreateCamera(w http.ResponseWriter, r *http.Request) {
	var cam CameraConfig
	if err := json.NewDecoder(r.Body).Decode(&cam); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}

	created, err := h.store.AddCamera(cam, h.onvifBasePort)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"camera": created, "restart_required": true})
}

func (h *webUIServer) handleUpdateCamera(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var cam CameraConfig
	if err := json.NewDecoder(r.Body).Decode(&cam); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}
	if cam.Name == "" {
		cam.Name = name
	}

	updated, err := h.store.UpdateCamera(name, cam, h.onvifBasePort)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"camera": updated, "restart_required": true})
}

// handleRestart acknowledges the request immediately (so the browser sees a response
// before the connection drops) and triggers the actual shutdown/respawn shortly after, off
// the request goroutine.
func (h *webUIServer) handleRestart(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "restarting"})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	if h.requestRestart == nil {
		return
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		h.requestRestart()
	}()
}

func (h *webUIServer) handleDeleteCamera(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.store.RemoveCamera(name); err != nil {
		writeJSONError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"restart_required": true})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

const indexHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ReolinkProxy</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #f5f6f8; --panel: #ffffff; --border: #d8dbe0; --text: #1a1d21; --muted: #6b7280;
    --accent: #2563eb; --up: #16a34a; --warn: #d97706; --down: #dc2626; --mono: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", Menlo, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #0f1115; --panel: #171a21; --border: #2a2f3a; --text: #e5e7eb; --muted: #8b93a1;
      --accent: #60a5fa; --up: #34d399; --warn: #fbbf24; --down: #f87171; }
  }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 1.5rem; max-width: 1180px; margin-inline: auto; font-size: 13px; }
  h1 { font-size: 1.15rem; margin: 0 0 0.15rem; font-weight: 600; letter-spacing: -0.01em; }
  h3, h4 { font-size: 0.95rem; margin: 0 0 0.6rem; }
  .sub { color: var(--muted); margin-bottom: 1.1rem; font-size: 0.82rem; }
  .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 6px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.45rem 0.6rem; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.03em; color: var(--muted); font-weight: 600; }
  tbody tr:hover { background: color-mix(in srgb, var(--accent) 5%, transparent); }
  .mono { font-family: var(--mono); font-size: 0.82em; }
  .dot { display: inline-block; width: 0.5rem; height: 0.5rem; border-radius: 50%; margin-right: 0.35rem; flex: none; }
  .dot.up { background: var(--up); } .dot.down { background: var(--down); } .dot.warn { background: var(--warn); }
  .pill { display: inline-flex; align-items: center; padding: 0.1rem 0.45rem; border-radius: 999px; font-size: 0.72rem; font-weight: 600; border: 1px solid transparent; }
  .pill.up { color: var(--up); background: color-mix(in srgb, var(--up) 14%, transparent); }
  .pill.down { color: var(--down); background: color-mix(in srgb, var(--down) 14%, transparent); }
  .pill.warn { color: var(--warn); background: color-mix(in srgb, var(--warn) 16%, transparent); }
  .pill.idle { color: var(--muted); background: color-mix(in srgb, var(--muted) 12%, transparent); }
  .muted { color: var(--muted); }
  .ip-line { display: block; white-space: nowrap; }
  .ip-line.match { color: var(--up); font-weight: 600; }
  button { cursor: pointer; font: inherit; background: var(--panel); color: var(--text); border: 1px solid var(--border); border-radius: 4px; padding: 0.3rem 0.6rem; }
  button:hover { border-color: var(--accent); }
  button.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
  .actions button { margin-right: 0.3rem; padding: 0.22rem 0.5rem; font-size: 0.78rem; }
  .toolbar { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin: 0.9rem 0; flex-wrap: wrap; }
  .toolbar .settings { display: flex; align-items: center; gap: 0.5rem; font-size: 0.8rem; }
  .toolbar .settings input { font-family: var(--mono); font-size: 0.8rem; padding: 0.28rem 0.5rem; border-radius: 4px; border: 1px solid var(--border); background: var(--panel); color: var(--text); width: 11rem; }
  #banner { display: none; background: color-mix(in srgb, var(--warn) 20%, var(--panel)); border: 1px solid var(--warn); color: var(--text); padding: 0.5rem 0.8rem; border-radius: 6px; margin-bottom: 1rem; font-size: 0.85rem; }
  #banner.show { display: block; }
  #form { display: none; padding: 1rem; margin-bottom: 1.5rem; }
  #form.show { display: block; }
  #events { display: none; padding: 1rem; margin-top: 1rem; }
  #events.show { display: block; }
  #events table { font-size: 0.8rem; }
  #events input[type=text] { width: 100%; padding: 0.35rem; box-sizing: border-box; margin-top: 0.2rem; font-family: var(--mono); font-size: 0.78rem; }
  #form label { display: block; margin-bottom: 0.6rem; font-size: 0.8rem; color: var(--muted); }
  #form input[type=text], #form input[type=password], #form input[type=number] { width: 100%; padding: 0.35rem; box-sizing: border-box; margin-top: 0.25rem; border: 1px solid var(--border); border-radius: 4px; background: var(--bg); color: var(--text); font: inherit; }
  #form .row { display: flex; gap: 1rem; }
  #form .row > label { flex: 1; }
  #form .checks label { display: inline-block; margin-right: 1rem; color: var(--text); }
  #formError { color: var(--down); font-size: 0.8rem; margin-top: 0.5rem; }
  #serverStats { display: flex; flex-wrap: wrap; gap: 1.5rem; padding: 0.7rem 1rem; font-size: 0.8rem; }
  #serverStats .stat { display: flex; flex-direction: column; gap: 0.15rem; }
  #serverStats .statLabel { color: var(--muted); font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.03em; }
  #serverStats span:not(.statLabel) { font-family: var(--mono); }
</style>
</head>
<body>
<h1>ReolinkProxy</h1>
<div class="sub">Baichuan &rarr; RTSP/ONVIF bridge status and camera config.</div>

<div id="banner">Configuration changed. <button id="restartBtn">Restart now</button> to apply it.</div>

<div class="toolbar">
  <div class="settings">
    <label for="f_protect_ip">NVR / Protect IP</label>
    <input type="text" id="f_protect_ip" placeholder="e.g. 10.0.0.1">
    <button id="saveSettingsBtn">Save</button>
    <span id="settingsSaved" class="muted"></span>
  </div>
  <button id="addBtn" class="primary">+ Add camera</button>
</div>

<div class="panel" id="serverStats">
  <div class="stat"><span class="statLabel">Uptime</span><span id="stat_uptime">-</span></div>
  <div class="stat"><span class="statLabel">Memory</span><span id="stat_mem">-</span></div>
  <div class="stat"><span class="statLabel">Goroutines</span><span id="stat_goroutines">-</span></div>
  <div class="stat"><span class="statLabel">GC runs</span><span id="stat_gc">-</span></div>
  <div class="stat"><span class="statLabel">CPUs</span><span id="stat_cpu">-</span></div>
  <div class="stat"><span class="statLabel">Go</span><span id="stat_goversion">-</span></div>
  <div class="stat"><span class="statLabel">Cameras</span><span id="stat_cameras">-</span></div>
</div>

<div class="panel">
<table id="cameraTable">
  <thead>
    <tr><th>Status</th><th>Camera</th><th>ONVIF</th><th>Streams</th><th>RTSP clients</th><th>Event subscribers</th><th>Motion</th><th>Actions</th></tr>
  </thead>
  <tbody id="cameraRows"></tbody>
</table>
</div>

<div id="form" class="panel">
  <h3 id="formTitle">Add camera</h3>
  <label>Name<input type="text" id="f_name"></label>
  <div class="row">
    <label>Host<input type="text" id="f_host"></label>
    <label>Port<input type="number" id="f_port" value="9000"></label>
  </div>
  <div class="row">
    <label>Username<input type="text" id="f_username"></label>
    <label>Password<input type="password" id="f_password"></label>
  </div>
  <label>ONVIF port (blank = auto)<input type="number" id="f_onvif_port"></label>
  <div class="checks">
    <label><input type="checkbox" id="f_battery_camera"> Battery-powered camera</label>
    <label id="f_pause_on_motion_row"><input type="checkbox" id="f_pause_on_motion"> Pause stream when idle without motion</label>
    <label><input type="checkbox" id="f_sub_uses_extern"> Use Extern stream as Sub (higher quality low tier, if the camera supports it)</label>
  </div>
  <div>
    <button id="saveBtn" class="primary">Save</button>
    <button id="cancelBtn" type="button">Cancel</button>
  </div>
  <div id="formError"></div>
</div>

<div id="events" class="panel">
  <h3 id="eventsTitle">Events</h3>
  <p class="muted">Motion events come from the Baichuan connection. camera-onvif events are forwarded verbatim from the camera's own ONVIF events service (includes smart/AI topics on camera models/firmware that expose them there). test events are synthetic, for verifying the pipeline through to your NVR.</p>
  <table>
    <thead><tr><th>Time</th><th>Source</th><th>Topic</th><th>Item</th><th>Value</th></tr></thead>
    <tbody id="eventRows"></tbody>
  </table>
  <h4>Send a test event</h4>
  <p class="muted">These presets use the real ONVIF topic names this camera itself emits for smart detections (confirmed by capturing its live event feed) - useful for checking whether your NVR reacts to each detection class without waiting for the real thing to walk by.</p>
  <div class="actions">
    <button data-preset="motion-on">Motion (start)</button>
    <button data-preset="motion-off">Motion (stop)</button>
    <button data-preset="person">Person</button>
    <button data-preset="car">Car / vehicle</button>
    <button data-preset="animal">Animal</button>
  </div>
  <h4>Custom</h4>
  <div class="row">
    <label>Topic<input type="text" id="te_topic" value="tns1:RuleEngine/CellMotionDetector/Motion"></label>
    <label>Item name<input type="text" id="te_item"  value="IsMotion"></label>
    <label>Item value<input type="text" id="te_value" value="true"></label>
  </div>
  <button id="sendTestEventBtn">Send custom test event</button>
  <button id="closeEventsBtn" type="button">Close</button>
</div>

<script>
var editing = null; // camera name being edited, or null when adding
var editingCamera = null; // full camera object being edited, for fields not shown in the form (stream/channel)
var protectIP = '';

function apiGetStatus() { return fetch('/api/status').then(function(r) { return r.json(); }); }
function apiGetCameras() { return fetch('/api/cameras').then(function(r) { return r.json(); }); }
function apiGetServerStats() { return fetch('/api/server-stats').then(function(r) { return r.json(); }); }

function fmtUptime(seconds) {
  var d = Math.floor(seconds / 86400);
  var h = Math.floor((seconds % 86400) / 3600);
  var m = Math.floor((seconds % 3600) / 60);
  var s = Math.floor(seconds % 60);
  var parts = [];
  if (d) parts.push(d + 'd');
  if (d || h) parts.push(h + 'h');
  if (d || h || m) parts.push(m + 'm');
  parts.push(s + 's');
  return parts.join(' ');
}

function renderServerStats(stats) {
  document.getElementById('stat_uptime').textContent = fmtUptime(stats.uptime_seconds);
  document.getElementById('stat_mem').textContent = stats.mem_alloc_mb.toFixed(1) + ' MB / ' + stats.mem_sys_mb.toFixed(1) + ' MB sys';
  document.getElementById('stat_goroutines').textContent = stats.goroutines;
  document.getElementById('stat_gc').textContent = stats.num_gc;
  document.getElementById('stat_cpu').textContent = stats.num_cpu;
  document.getElementById('stat_goversion').textContent = stats.go_version;
  document.getElementById('stat_cameras').textContent = stats.num_cameras;
}

function refreshServerStats() {
  apiGetServerStats().then(renderServerStats);
}
function apiGetSettings() { return fetch('/api/settings').then(function(r) { return r.json(); }); }

function showBanner() { document.getElementById('banner').classList.add('show'); }

function dotClass(view) {
  if (!view.connected) return 'down';
  if (view.motion.known && view.motion.active) return 'warn';
  return 'up';
}

function fmtStream(s) {
  var res = (s.width && s.height) ? (s.width + 'x' + s.height) : '?';
  return '<span class="mono">' + s.path + '</span> <span class="muted">' + res + (s.fps ? ' @' + s.fps + 'fps' : '') + ' ' + (s.codec || '') + '</span>';
}

function fmtMotion(m) {
  if (!m.known) return '<span class="pill idle">unknown</span>';
  if (m.unsupported) return '<span class="pill idle">unsupported</span>';
  return m.active ? '<span class="pill warn">active</span>' : '<span class="pill idle">idle</span>';
}

function fmtAgo(t) {
  var d = new Date(t);
  if (isNaN(d.getTime())) return '';
  var secs = Math.max(0, Math.round((Date.now() - d.getTime()) / 1000));
  if (secs < 60) return secs + 's ago';
  if (secs < 3600) return Math.round(secs / 60) + 'm ago';
  return Math.round(secs / 3600) + 'h ago';
}

function ipLine(ip, sinceLabel, since, path) {
  var cls = (protectIP && ip === protectIP) ? 'ip-line mono match' : 'ip-line mono';
  var pathPart = path ? (' <span class="muted">&rarr; ' + path + '</span>') : '';
  var extra = since ? (' <span class="muted">(' + sinceLabel + ' ' + fmtAgo(since) + ')</span>') : '';
  return '<span class="' + cls + '">' + ip + pathPart + extra + '</span>';
}

function fmtClients(view) {
  var lines = (view.rtsp_clients || []).map(function(c) { return ipLine(c.ip, 'since', c.connected_at, c.path); });
  return lines.join('') || '<span class="muted">none</span>';
}

function fmtSubscribers(view) {
  var lines = (view.event_subscriber_list || []).map(function(s) { return ipLine(s.ip, 'last seen', s.last_seen); });
  return lines.join('') || '<span class="muted">none</span>';
}

function statusCell(view) {
  var pill = view.connected ? '<span class="pill up">connected</span>' : '<span class="pill down">disconnected</span>';
  var protectLine = '';
  if (protectIP) {
    protectLine = view.protect_connected
      ? '<div class="ip-line match" style="margin-top:0.2rem">&#9679; Protect connected</div>'
      : '<div class="ip-line muted" style="margin-top:0.2rem">&#9675; Protect not seen</div>';
  }
  return '<span class="dot ' + dotClass(view) + '"></span>' + pill + protectLine;
}

function render(statusList, cameraList) {
  var byName = {};
  cameraList.forEach(function(c) { byName[c.name] = c; });

  var rows = statusList.map(function(view) {
    var streams = (view.streams || []).map(fmtStream).join('<br>') || '<span class="muted">no stream yet</span>';
    var onvif = '<span class="mono">' + view.onvif_authority + '</span><br><span class="muted mono">mac ' + view.onvif_mac + '</span>';
    return '<tr>' +
      '<td>' + statusCell(view) + '</td>' +
      '<td><strong>' + view.name + '</strong><br><span class="muted mono">' + view.host + '</span></td>' +
      '<td>' + onvif + '</td>' +
      '<td>' + streams + '</td>' +
      '<td>' + fmtClients(view) + '</td>' +
      '<td>' + fmtSubscribers(view) + '</td>' +
      '<td>' + fmtMotion(view.motion) + '</td>' +
      '<td class="actions">' +
        '<button data-edit="' + view.name + '">Edit</button>' +
        '<button data-events="' + view.name + '">Events</button>' +
        '<button data-delete="' + view.name + '">Delete</button>' +
      '</td>' +
    '</tr>';
  });

  document.getElementById('cameraRows').innerHTML = rows.join('') || '<tr><td colspan="8" class="muted">No cameras configured yet.</td></tr>';

  document.querySelectorAll('[data-edit]').forEach(function(btn) {
    btn.addEventListener('click', function() { openForm(byName[btn.getAttribute('data-edit')]); });
  });
  document.querySelectorAll('[data-delete]').forEach(function(btn) {
    btn.addEventListener('click', function() { deleteCamera(btn.getAttribute('data-delete')); });
  });
  document.querySelectorAll('[data-events]').forEach(function(btn) {
    btn.addEventListener('click', function() { openEvents(btn.getAttribute('data-events')); });
  });
}

function refresh() {
  Promise.all([apiGetStatus(), apiGetCameras()]).then(function(results) {
    render(results[0], results[1]);
  });
}

function loadSettings() {
  apiGetSettings().then(function(s) {
    protectIP = s.protect_server_ip || '';
    document.getElementById('f_protect_ip').value = protectIP;
  });
}

function saveSettings() {
  var ip = document.getElementById('f_protect_ip').value.trim();
  fetch('/api/settings', { method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ protect_server_ip: ip }) })
    .then(function(r) { return r.json(); })
    .then(function(s) {
      protectIP = s.protect_server_ip || '';
      var saved = document.getElementById('settingsSaved');
      saved.textContent = 'saved';
      setTimeout(function() { saved.textContent = ''; }, 1500);
      refresh();
    });
}

function openForm(camera) {
  document.getElementById('formError').textContent = '';
  editing = camera ? camera.name : null;
  editingCamera = camera || null;
  document.getElementById('formTitle').textContent = camera ? ('Edit ' + camera.name) : 'Add camera';
  document.getElementById('f_name').value = camera ? camera.name : '';
  document.getElementById('f_name').disabled = !!camera;
  document.getElementById('f_host').value = camera ? camera.host : '';
  document.getElementById('f_port').value = camera ? camera.port : 9000;
  document.getElementById('f_username').value = camera ? camera.username : '';
  document.getElementById('f_password').value = camera ? camera.password : '';
  document.getElementById('f_onvif_port').value = camera && camera.onvif_port ? camera.onvif_port : '';
  document.getElementById('f_battery_camera').checked = !!(camera && camera.battery_camera);
  document.getElementById('f_pause_on_motion').checked = !!(camera && camera.pause_on_motion);
  document.getElementById('f_sub_uses_extern').checked = !!(camera && camera.sub_uses_extern);
  updatePauseVisibility();
  document.getElementById('form').classList.add('show');
}

function updatePauseVisibility() {
  var batteryCamera = document.getElementById('f_battery_camera').checked;
  document.getElementById('f_pause_on_motion_row').style.display = batteryCamera ? '' : 'none';
  if (!batteryCamera) { document.getElementById('f_pause_on_motion').checked = false; }
}

function closeForm() {
  document.getElementById('form').classList.remove('show');
  editing = null;
  editingCamera = null;
}

function saveCamera() {
  var body = {
    name: document.getElementById('f_name').value.trim(),
    host: document.getElementById('f_host').value.trim(),
    port: parseInt(document.getElementById('f_port').value, 10) || 9000,
    username: document.getElementById('f_username').value,
    password: document.getElementById('f_password').value,
    battery_camera: document.getElementById('f_battery_camera').checked,
    pause_on_motion: document.getElementById('f_pause_on_motion').checked,
    sub_uses_extern: document.getElementById('f_sub_uses_extern').checked
  };
  // Stream/channel aren't user-configurable - preserve the existing camera's values on
  // edit (defaults apply server-side for a new camera).
  if (editingCamera) {
    body.stream = editingCamera.stream;
    body.channel = editingCamera.channel;
  }
  var onvifPort = document.getElementById('f_onvif_port').value;
  if (onvifPort) { body.onvif_port = parseInt(onvifPort, 10); }

  var url = editing ? ('/api/cameras/' + encodeURIComponent(editing)) : '/api/cameras';
  var method = editing ? 'PUT' : 'POST';

  fetch(url, { method: method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) })
    .then(function(r) { return r.json().then(function(data) { return { ok: r.ok, data: data }; }); })
    .then(function(res) {
      if (!res.ok) {
        document.getElementById('formError').textContent = res.data.error || 'save failed';
        return;
      }
      closeForm();
      showBanner();
      refresh();
    })
    .catch(function(err) { document.getElementById('formError').textContent = String(err); });
}

function deleteCamera(name) {
  if (!confirm('Delete camera "' + name + '"?')) return;
  fetch('/api/cameras/' + encodeURIComponent(name), { method: 'DELETE' })
    .then(function(r) { return r.json().then(function(data) { return { ok: r.ok, data: data }; }); })
    .then(function(res) {
      if (!res.ok) { alert(res.data.error || 'delete failed'); return; }
      showBanner();
      refresh();
    });
}

var eventsCamera = null;
var eventsPoll = null;

function fmtTime(t) {
  var d = new Date(t);
  return isNaN(d.getTime()) ? t : d.toLocaleTimeString();
}

function refreshEvents() {
  if (!eventsCamera) return;
  fetch('/api/cameras/' + encodeURIComponent(eventsCamera) + '/events').then(function(r) { return r.json(); }).then(function(events) {
    var rows = events.slice().reverse().map(function(ev) {
      return '<tr><td>' + fmtTime(ev.time) + '</td><td>' + ev.source + '</td><td><code>' + ev.topic + '</code></td><td>' + ev.item_name + '</td><td>' + ev.item_value + '</td></tr>';
    });
    document.getElementById('eventRows').innerHTML = rows.join('') || '<tr><td colspan="5" class="muted">No events yet.</td></tr>';
  });
}

function openEvents(name) {
  eventsCamera = name;
  document.getElementById('eventsTitle').textContent = 'Events - ' + name;
  document.getElementById('events').classList.add('show');
  refreshEvents();
  if (eventsPoll) clearInterval(eventsPoll);
  eventsPoll = setInterval(refreshEvents, 2000);
}

function closeEvents() {
  document.getElementById('events').classList.remove('show');
  eventsCamera = null;
  if (eventsPoll) { clearInterval(eventsPoll); eventsPoll = null; }
}

var testEventPresets = {
  'motion-on':  { topic: 'tns1:RuleEngine/CellMotionDetector/Motion',   item_name: 'IsMotion', item_value: 'true' },
  'motion-off': { topic: 'tns1:RuleEngine/CellMotionDetector/Motion',   item_name: 'IsMotion', item_value: 'false' },
  'person':     { topic: 'tns1:RuleEngine/MyRuleDetector/PeopleDetect', item_name: 'State',    item_value: 'true' },
  'car':        { topic: 'tns1:RuleEngine/MyRuleDetector/VehicleDetect', item_name: 'State',   item_value: 'true' },
  'animal':     { topic: 'tns1:RuleEngine/MyRuleDetector/DogCatDetect', item_name: 'State',    item_value: 'true' }
};

function sendTestEventBody(body) {
  if (!eventsCamera) return;
  fetch('/api/cameras/' + encodeURIComponent(eventsCamera) + '/test-event', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
  }).then(function() { refreshEvents(); });
}

function sendTestEventPreset(name) {
  var preset = testEventPresets[name];
  if (preset) sendTestEventBody(preset);
}

function sendTestEvent() {
  sendTestEventBody({
    topic: document.getElementById('te_topic').value.trim(),
    item_name: document.getElementById('te_item').value.trim(),
    item_value: document.getElementById('te_value').value.trim()
  });
}

function waitForRestart() {
  var banner = document.getElementById('banner');
  banner.textContent = 'Restarting...';
  var tries = 0;
  var poll = setInterval(function() {
    tries++;
    fetch('/api/status').then(function(r) {
      if (r.ok) { clearInterval(poll); location.reload(); }
    }).catch(function() {
      if (tries > 60) { clearInterval(poll); banner.textContent = 'Still not back - check the process manually.'; }
    });
  }, 1000);
}

function restartNow() {
  if (!confirm('Restart the service now? Live streams will briefly drop.')) return;
  fetch('/api/restart', { method: 'POST' }).then(function() { waitForRestart(); });
}

document.getElementById('addBtn').addEventListener('click', function() { openForm(null); });
document.getElementById('cancelBtn').addEventListener('click', closeForm);
document.getElementById('f_battery_camera').addEventListener('change', updatePauseVisibility);
document.getElementById('saveBtn').addEventListener('click', saveCamera);
document.getElementById('restartBtn').addEventListener('click', restartNow);
document.getElementById('closeEventsBtn').addEventListener('click', closeEvents);
document.getElementById('sendTestEventBtn').addEventListener('click', sendTestEvent);
document.getElementById('saveSettingsBtn').addEventListener('click', saveSettings);
document.querySelectorAll('[data-preset]').forEach(function(btn) {
  btn.addEventListener('click', function() { sendTestEventPreset(btn.getAttribute('data-preset')); });
});

loadSettings();
refresh();
refreshServerStats();
setInterval(refresh, 3000);
setInterval(refreshServerStats, 5000);
</script>
</body>
</html>
`
