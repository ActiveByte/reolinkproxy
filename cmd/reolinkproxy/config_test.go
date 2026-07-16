package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDeriveFakeMACIsStableAndLocallyAdministered(t *testing.T) {
	t.Parallel()

	a := deriveFakeMAC("front door")
	b := deriveFakeMAC("front door")
	if a != b {
		t.Fatalf("expected deriveFakeMAC to be deterministic, got %q and %q", a, b)
	}

	c := deriveFakeMAC("backyard")
	if a == c {
		t.Fatalf("expected different names to derive different MACs, both got %q", a)
	}

	var firstOctet byte
	if _, err := fmt.Sscanf(a, "%02x:", &firstOctet); err != nil {
		t.Fatalf("failed to parse first octet of %q: %v", a, err)
	}
	if firstOctet&0x01 != 0 {
		t.Fatalf("expected multicast bit to be clear in %q", a)
	}
	if firstOctet&0x02 == 0 {
		t.Fatalf("expected locally-administered bit to be set in %q", a)
	}
}

func TestApplyCameraDefaultsHonorsExplicitONVIFOverrides(t *testing.T) {
	t.Parallel()

	camera := CameraConfig{
		Name:        "front",
		Host:        "192.168.1.10",
		ONVIFPort:   9999,
		ONVIFMAC:    "aa:bb:cc:dd:ee:ff",
		ONVIFSerial: "custom-serial",
	}

	applyCameraDefaults(&camera, 0, 8102)

	if camera.ONVIFPort != 9999 {
		t.Fatalf("expected explicit onvif port to be preserved, got %d", camera.ONVIFPort)
	}
	if camera.ONVIFMAC != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("expected explicit onvif mac to be preserved, got %q", camera.ONVIFMAC)
	}
	if camera.ONVIFSerial != "custom-serial" {
		t.Fatalf("expected explicit onvif serial to be preserved, got %q", camera.ONVIFSerial)
	}
}

func TestLoadCamerasFromConfigFileReadsCamerasAndTreatsMissingFileAsEmpty(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yml")

	cameras, err := loadCamerasFromConfigFile(path)
	if err != nil {
		t.Fatalf("expected a missing config file to be treated as no cameras, got error: %v", err)
	}
	if cameras != nil {
		t.Fatalf("expected no cameras for a missing file, got %+v", cameras)
	}

	yamlDoc := "cameras:\n  - name: front\n    host: 192.168.1.10\n    rtsp_path: front/stream\n"
	if err := os.WriteFile(path, []byte(yamlDoc), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cameras, err = loadCamerasFromConfigFile(path)
	if err != nil {
		t.Fatalf("loadCamerasFromConfigFile: %v", err)
	}
	if len(cameras) != 1 || cameras[0].Name != "front" {
		t.Fatalf("expected one camera named front, got %+v", cameras)
	}
}

func TestMergeServerDefaultsFillsZeroValuedFieldsOnly(t *testing.T) {
	t.Parallel()

	file := ServerConfig{
		RTSPAddress:   ":9554", // explicitly set in config.yml - must survive
		ONVIFBasePort: 9102,    // explicitly set in config.yml - must survive
		// RTPAddress, RTCPAddress, LogLevel, pacer settings, ConfigFile, WebAddress:
		// left zero, as if config.yml predates these fields or never persists them.
	}
	fallback := defaultConfig().Server
	fallback.ConfigFile = "/config.yml"
	fallback.WebAddress = ":8080"

	merged := mergeServerDefaults(file, fallback)

	if merged.RTSPAddress != ":9554" {
		t.Fatalf("expected explicit file value to survive, got %q", merged.RTSPAddress)
	}
	if merged.ONVIFBasePort != 9102 {
		t.Fatalf("expected explicit file value to survive, got %d", merged.ONVIFBasePort)
	}
	if merged.RTPAddress != fallback.RTPAddress {
		t.Fatalf("expected zero-valued rtp_address to fall back to %q, got %q", fallback.RTPAddress, merged.RTPAddress)
	}
	if merged.RTCPAddress != fallback.RTCPAddress {
		t.Fatalf("expected zero-valued rtcp_address to fall back to %q, got %q", fallback.RTCPAddress, merged.RTCPAddress)
	}
	if merged.LogLevel != fallback.LogLevel {
		t.Fatalf("expected zero-valued log_level to fall back to %q, got %q", fallback.LogLevel, merged.LogLevel)
	}
	if merged.VideoPacerInitialLatencyMs != fallback.VideoPacerInitialLatencyMs {
		t.Fatalf("expected zero-valued video pacer latency to fall back to %d, got %d", fallback.VideoPacerInitialLatencyMs, merged.VideoPacerInitialLatencyMs)
	}
	if merged.ConfigFile != "/config.yml" {
		t.Fatalf("expected ConfigFile to always come from CLI/env, got %q", merged.ConfigFile)
	}
	if merged.WebAddress != ":8080" {
		t.Fatalf("expected WebAddress to always come from CLI/env, got %q", merged.WebAddress)
	}
}

