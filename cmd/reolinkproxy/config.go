package main

import (
	"crypto/sha1" //#nosec G505
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MQTT    MQTTConfig     `yaml:"mqtt"`
	Server  ServerConfig   `yaml:"server"`
	ONVIF   ONVIFConfig    `yaml:"onvif"`
	Cameras []CameraConfig `yaml:"cameras"`
}

type MQTTConfig struct {
	Broker   string `yaml:"broker"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Topic    string `yaml:"topic"`
}

type ServerConfig struct {
	RTSPAddress string `yaml:"rtsp_address"`
	RTPAddress  string `yaml:"rtp_address"`
	RTCPAddress string `yaml:"rtcp_address"`
	// ONVIFBasePort is the first port used to auto-assign per-camera ONVIF services when a
	// camera doesn't set ONVIFPort explicitly (base port + camera index). Each camera gets
	// its own ONVIF server on its own port - there is no single shared ONVIF address.
	ONVIFBasePort int `yaml:"onvif_base_port"`
	PprofAddress  string `yaml:"pprof_address"`
	AdvertiseHost string `yaml:"advertise_host"`
	LogLevel      string `yaml:"log_level"`
	LogPackets    bool   `yaml:"log_packets"`

	// AudioPacerInitialLatencyMs is the media pacer startup delay for audio (wall clock before
	// the first packet is sent). Default 500ms.
	AudioPacerInitialLatencyMs int `yaml:"audio_pacer_initial_latency_ms"`
	// AudioPacerMaxLeadMs caps how far ahead of wall clock the audio pacer cursor may run;
	// if exceeded, the cursor is reset to now. Default 2s.
	AudioPacerMaxLeadMs int `yaml:"audio_pacer_max_lead_ms"`
	// AudioPacerSnapOnPast, when true, snaps the emission cursor to now if it falls behind
	// wall clock.
	AudioPacerSnapOnPast bool `yaml:"audio_pacer_snap_on_past"`

	// VideoPacerInitialLatencyMs is the media pacer startup delay for video. Default 1500ms.
	VideoPacerInitialLatencyMs int `yaml:"video_pacer_initial_latency_ms"`
	// VideoPacerMaxLeadMs caps how far ahead of wall clock the video pacer cursor may run. Default 3s.
	VideoPacerMaxLeadMs int `yaml:"video_pacer_max_lead_ms"`
	// VideoPacerSnapOnPast, when true, snaps the video pacer cursor to now when behind; default false for video.
	VideoPacerSnapOnPast bool `yaml:"video_pacer_snap_on_past"`

	// DisableRTCPSenderReports suppresses periodic RTCP Sender Reports on published streams (default true).
	// Some receivers (e.g. FFmpeg) re-anchor decode time on each SR, which can cause non-monotonic DTS warnings.
	DisableRTCPSenderReports bool `yaml:"disable_rtcp_sender_reports"`

	// ConfigFile is the path to the YAML file used to persist cameras added/edited/removed
	// through the web UI. Created automatically on first run if it doesn't exist.
	ConfigFile string `yaml:"-"`
	// WebAddress is the status/config web UI listen address.
	WebAddress string `yaml:"-"`
}

type ONVIFConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type CameraConfig struct {
	Name           string        `yaml:"name" json:"name"`
	Host           string        `yaml:"host" json:"host"`
	Port           int           `yaml:"port" json:"port"`
	UID            string        `yaml:"uid" json:"uid"`
	Username       string        `yaml:"username" json:"username"`
	Password       string        `yaml:"password" json:"password"`
	Timeout        time.Duration `yaml:"timeout" json:"timeout"`
	Stream         string        `yaml:"stream" json:"stream"`
	Channel        int           `yaml:"channel" json:"channel"`
	RTSPPath       string        `yaml:"rtsp_path" json:"rtsp_path"`
	TalkProfile    string        `yaml:"talk_profile" json:"talk_profile"`
	TalkVolume     int           `yaml:"talk_volume" json:"talk_volume"`
	TalkEncoder    string        `yaml:"talk_encoder" json:"talk_encoder"`
	TalkEncoderCmd string        `yaml:"talk_encoder_cmd" json:"talk_encoder_cmd"`
	PauseOnMotion  bool          `yaml:"pause_on_motion" json:"pause_on_motion"`
	PauseOnClient  bool          `yaml:"pause_on_client" json:"pause_on_client"`
	PauseTimeout   time.Duration `yaml:"pause_timeout" json:"pause_timeout"`
	IdleDisconnect bool          `yaml:"idle_disconnect" json:"idle_disconnect"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	// SubUsesExtern, when true, serves the "sub" stream role (RTSP path, ONVIF Low
	// profile) from the camera's Baichuan Extern channel instead of its Sub channel. Some
	// models' Extern encoder profile is a distinct, more stable tier than Sub - this only
	// changes which upstream channel is pulled, not the "sub" name/path/ONVIF token exposed
	// to NVRs, so nothing downstream needs to know about it.
	SubUsesExtern bool `yaml:"sub_uses_extern" json:"sub_uses_extern"`

	// ONVIFPort is the TCP port this camera's own virtual ONVIF Device/Media/Events
	// service listens on. Each camera gets its own port so NVRs such as UniFi Protect can
	// adopt it as an independent device. Defaults to ServerConfig.ONVIFBasePort + the
	// camera's index if unset.
	ONVIFPort int `yaml:"onvif_port" json:"onvif_port"`
	// ONVIFMAC is a stable, fake identifier reported in ONVIF GetNetworkInterfaces/
	// discovery scopes for this camera. It is NOT a real network-layer MAC (no macvlan/ARP
	// involved) - it just needs to be unique and stable across restarts. Defaults to a
	// value deterministically derived from Name if unset.
	ONVIFMAC string `yaml:"onvif_mac" json:"onvif_mac"`
	// ONVIFSerial is the serial number reported in ONVIF GetDeviceInformation. Defaults to
	// a value deterministically derived from Name if unset.
	ONVIFSerial string `yaml:"onvif_serial" json:"onvif_serial"`
	// CameraONVIFPort is the port the *camera's own* built-in ONVIF service listens on
	// (used to forward its events/smart-events and fetch real snapshots) - distinct from
	// ONVIFPort, which is this proxy's own virtual ONVIF service for that camera. Defaults
	// to 8000, the common Reolink ONVIF port.
	CameraONVIFPort int `yaml:"camera_onvif_port" json:"camera_onvif_port"`
}

func (c ServerConfig) audioPacerInitialLatency() time.Duration {
	return time.Duration(c.AudioPacerInitialLatencyMs) * time.Millisecond
}

func (c ServerConfig) audioPacerMaxLead() time.Duration {
	return time.Duration(c.AudioPacerMaxLeadMs) * time.Millisecond
}

func (c ServerConfig) videoPacerInitialLatency() time.Duration {
	return time.Duration(c.VideoPacerInitialLatencyMs) * time.Millisecond
}

func (c ServerConfig) videoPacerMaxLead() time.Duration {
	return time.Duration(c.VideoPacerMaxLeadMs) * time.Millisecond
}

func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			RTSPAddress:                ":8554",
			RTPAddress:                 ":8000",
			RTCPAddress:                ":8001",
			ONVIFBasePort:              8102,
			PprofAddress:               "",
			LogLevel:                   "info",
			AudioPacerInitialLatencyMs: 500,
			AudioPacerMaxLeadMs:        2000,
			AudioPacerSnapOnPast:       true,
			VideoPacerInitialLatencyMs: 1500,
			VideoPacerMaxLeadMs:        3000,
			VideoPacerSnapOnPast:       false,
			DisableRTCPSenderReports:   true,
			ConfigFile:                 "config.yml",
			WebAddress:                 ":8080",
		},
		MQTT: MQTTConfig{
			Topic: "reolinkproxy",
		},
	}
}

// loadCamerasFromConfigFile reads just the camera list out of the YAML config file, for the
// healthcheck subcommand's path-discovery fallback - it runs as a separate short-lived
// process (typically Docker's HEALTHCHECK), so it reads the file directly rather than going
// through ConfigStore (which would create the file if missing, a side effect a read-only
// health probe shouldn't have).
func loadCamerasFromConfigFile(path string) ([]CameraConfig, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var fileCfg Config
	if err := yaml.Unmarshal(data, &fileCfg); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}
	return fileCfg.Cameras, nil
}

// mergeServerDefaults fills any zero-valued field in file with the corresponding value from
// fallback (the CLI/env-resolved config), so a config.yml that predates a field - or the
// ConfigFile/WebAddress fields, which are never persisted (see their yaml:"-" tags) - still
// ends up correct after loading. Bool fields are intentionally left alone: a missing field
// and an explicit false are indistinguishable in YAML, and their real default is already
// baked into the file the first time it's written (see newConfigStore).
func mergeServerDefaults(file, fallback ServerConfig) ServerConfig {
	if file.RTSPAddress == "" {
		file.RTSPAddress = fallback.RTSPAddress
	}
	if file.RTPAddress == "" {
		file.RTPAddress = fallback.RTPAddress
	}
	if file.RTCPAddress == "" {
		file.RTCPAddress = fallback.RTCPAddress
	}
	if file.ONVIFBasePort == 0 {
		file.ONVIFBasePort = fallback.ONVIFBasePort
	}
	if file.LogLevel == "" {
		file.LogLevel = fallback.LogLevel
	}
	if file.AudioPacerInitialLatencyMs == 0 {
		file.AudioPacerInitialLatencyMs = fallback.AudioPacerInitialLatencyMs
	}
	if file.AudioPacerMaxLeadMs == 0 {
		file.AudioPacerMaxLeadMs = fallback.AudioPacerMaxLeadMs
	}
	if file.VideoPacerInitialLatencyMs == 0 {
		file.VideoPacerInitialLatencyMs = fallback.VideoPacerInitialLatencyMs
	}
	if file.VideoPacerMaxLeadMs == 0 {
		file.VideoPacerMaxLeadMs = fallback.VideoPacerMaxLeadMs
	}
	file.ConfigFile = fallback.ConfigFile
	file.WebAddress = fallback.WebAddress
	return file
}

func applyCameraDefaults(camera *CameraConfig, index int, onvifBasePort int) {
	if camera.Port == 0 {
		camera.Port = 9000
	}
	// Stream is not user-configurable: it's always main+sub. A wrong value here would
	// silently drop a stream tier, so this ignores whatever is in the env/file and forces
	// the same two profiles on every load. Use SubUsesExtern to swap what "sub" pulls.
	camera.Stream = "main,sub"
	if camera.RTSPPath == "" {
		camera.RTSPPath = camera.Name + "/stream"
	}
	if camera.Timeout == 0 {
		camera.Timeout = 10 * time.Second
	}
	camera.TalkProfile = normalizeCameraProfileName(camera.TalkProfile)
	if camera.TalkVolume == 0 {
		camera.TalkVolume = 100
	}
	if camera.TalkEncoder == "" {
		camera.TalkEncoder = "internal"
	}
	if camera.PauseTimeout == 0 {
		camera.PauseTimeout = time.Second
	}
	if camera.IdleTimeout == 0 {
		camera.IdleTimeout = 30 * time.Second
	}
	if camera.ONVIFPort == 0 {
		camera.ONVIFPort = onvifBasePort + index
	}
	if camera.CameraONVIFPort == 0 {
		camera.CameraONVIFPort = 8000
	}
	if camera.ONVIFMAC == "" {
		camera.ONVIFMAC = deriveFakeMAC(camera.Name)
	}
	if camera.ONVIFSerial == "" {
		camera.ONVIFSerial = deriveSerial(camera.Name)
	}
}

// deriveFakeMAC deterministically derives a stable, fake MAC-style identifier from the
// camera name. It is never used at the network layer (no ARP/macvlan) - ONVIF clients such
// as UniFi Protect only read it out of GetNetworkInterfaces/discovery scopes as a per-device
// identifier, so it only needs to be unique and stable across restarts, not a real NIC
// address. The locally-administered/unicast bit is set on the first octet so it can never
// collide with a real vendor-assigned MAC.
func deriveFakeMAC(name string) string {
	sum := sha1.Sum([]byte("reolinkproxy-onvif-mac:" + name)) //#nosec G401
	b := make([]byte, 6)
	copy(b, sum[:6])
	b[0] = (b[0] &^ 0x01) | 0x02 // clear multicast bit, set locally-administered bit
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", b[0], b[1], b[2], b[3], b[4], b[5])
}

// deriveSerial deterministically derives a stable ONVIF serial number from the camera name.
func deriveSerial(name string) string {
	sum := sha1.Sum([]byte("reolinkproxy-onvif-serial:" + name)) //#nosec G401
	return "rlp-" + hex.EncodeToString(sum[:8])
}

func validateCameraConfig(camera *CameraConfig) error {
	if camera.Name == "" {
		return fmt.Errorf("camera name is required")
	}
	if camera.Host == "" && camera.UID == "" {
		return fmt.Errorf("camera host or uid is required")
	}
	if camera.TalkProfile != "" && !camera.hasStream(camera.TalkProfile) {
		return fmt.Errorf("camera talk_profile %q must be one of configured streams %q", camera.TalkProfile, camera.Stream)
	}
	return nil
}

func splitCameraStreams(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := normalizeCameraProfileName(part)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	return out
}

func normalizeCameraProfileName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (c CameraConfig) hasStream(name string) bool {
	name = normalizeCameraProfileName(name)
	for _, stream := range splitCameraStreams(c.Stream) {
		if stream == name {
			return true
		}
	}
	return false
}

func (c CameraConfig) preferredTalkProfile() string {
	if c.hasStream(c.TalkProfile) {
		return normalizeCameraProfileName(c.TalkProfile)
	}
	return ""
}
