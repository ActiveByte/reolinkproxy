package main

import (
	"crypto/sha1" //#nosec G505
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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
	RTSPAddress   string `yaml:"rtsp_address"`
	RTPAddress    string `yaml:"rtp_address"`
	RTCPAddress   string `yaml:"rtcp_address"`
	ONVIFAddress  string `yaml:"onvif_address"`
	// ONVIFBasePort is the first port used to auto-assign per-camera ONVIF services when a
	// camera doesn't set ONVIFPort explicitly (base port + camera index).
	ONVIFBasePort int    `yaml:"onvif_base_port"`
	PprofAddress  string `yaml:"pprof_address"`
	AdvertiseHost string `yaml:"advertise_host"`
	LogLevel      string `yaml:"log_level"`
	LogPackets    bool   `yaml:"log_packets"`
	// ProtectServerIP is the expected IP address of the UniFi Protect (or other NVR) host.
	// When set, the web UI highlights whether RTSP/ONVIF connections are actually arriving
	// from this address, to make it visible whether Protect is really talking to a camera.
	ProtectServerIP string `yaml:"protect_server_ip" json:"protect_server_ip"`

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
	BatteryCamera  bool          `yaml:"battery_camera" json:"battery_camera"`
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

var (
	cameraEnvKeyRE   = regexp.MustCompile(`^REOLINK_CAMERA_(\d+)_([A-Z0-9_]+)$`)
	cameraConfigType = reflect.TypeOf(CameraConfig{})
	durationType     = reflect.TypeOf(time.Duration(0))
)

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
			ONVIFAddress:               ":8002",
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

func loadCamerasFromEnv() ([]CameraConfig, error) {
	return loadCamerasFromEntries(os.Environ())
}

func loadCamerasFromEntries(entries []string) ([]CameraConfig, error) {
	fieldIndexes := cameraEnvFieldIndexes()
	camerasByIndex := make(map[int]*CameraConfig)

	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		matches := cameraEnvKeyRE.FindStringSubmatch(key)
		if len(matches) != 3 {
			continue
		}

		cameraIndex, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("%s: invalid camera index: %w", key, err)
		}

		fieldIndex, found := fieldIndexes[matches[2]]
		if !found {
			continue
		}

		camera := camerasByIndex[cameraIndex]
		if camera == nil {
			camera = &CameraConfig{}
			camerasByIndex[cameraIndex] = camera
		}

		field := reflect.ValueOf(camera).Elem().Field(fieldIndex)
		if err := setFieldFromEnv(field, value, key); err != nil {
			return nil, err
		}
	}

	if len(camerasByIndex) == 0 {
		return nil, nil
	}

	indexes := make([]int, 0, len(camerasByIndex))
	for cameraIndex := range camerasByIndex {
		indexes = append(indexes, cameraIndex)
	}
	sort.Ints(indexes)

	cameras := make([]CameraConfig, 0, len(indexes))
	for i, cameraIndex := range indexes {
		camera := *camerasByIndex[cameraIndex]
		applyCameraDefaults(&camera, i, defaultConfig().Server.ONVIFBasePort)

		if err := validateCameraConfig(&camera); err != nil {
			return nil, fmt.Errorf("REOLINK_CAMERA_%d_*: %w", cameraIndex, err)
		}

		cameras = append(cameras, camera)
	}

	return cameras, nil
}

func cameraEnvFieldIndexes() map[string]int {
	out := make(map[string]int, cameraConfigType.NumField())

	for i := range cameraConfigType.NumField() {
		tag := strings.Split(cameraConfigType.Field(i).Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		out[strings.ToUpper(tag)] = i
	}

	return out
}

func setFieldFromEnv(field reflect.Value, rawValue string, envKey string) error {
	if field.Type() == durationType {
		duration, err := time.ParseDuration(rawValue)
		if err != nil {
			return fmt.Errorf("%s: invalid duration %q", envKey, rawValue)
		}
		field.SetInt(int64(duration))
		return nil
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(rawValue)
	case reflect.Bool:
		value, err := strconv.ParseBool(rawValue)
		if err != nil {
			return fmt.Errorf("%s: invalid bool %q", envKey, rawValue)
		}
		field.SetBool(value)
	case reflect.Int:
		value, err := strconv.Atoi(rawValue)
		if err != nil {
			return fmt.Errorf("%s: invalid int %q", envKey, rawValue)
		}
		field.SetInt(int64(value))
	default:
		return fmt.Errorf("%s: unsupported field type %s", envKey, field.Type())
	}

	return nil
}

func applyCameraDefaults(camera *CameraConfig, index int, onvifBasePort int) {
	if camera.Port == 0 {
		camera.Port = 9000
	}
	if camera.Stream == "" {
		camera.Stream = "main,sub"
	}
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
