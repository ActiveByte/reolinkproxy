package main

import (
	"path/filepath"
	"testing"
)

func TestConfigStoreCreatesEmptyFileWhenMissing(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yml")
	store, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("newConfigStore: %v", err)
	}
	if len(store.Cameras()) != 0 {
		t.Fatalf("expected no cameras in a freshly created store, got %d", len(store.Cameras()))
	}

	// Reload from disk to confirm the empty file was actually persisted, not just held in memory.
	store2, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("reload newConfigStore: %v", err)
	}
	if len(store2.Cameras()) != 0 {
		t.Fatalf("expected reloaded store to still have no cameras, got %d", len(store2.Cameras()))
	}
}

func TestConfigStoreAddUpdateRemoveRoundTrips(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yml")
	store, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("newConfigStore: %v", err)
	}

	created, err := store.AddCamera(CameraConfig{Name: "front", Host: "192.168.1.10"}, 8102)
	if err != nil {
		t.Fatalf("AddCamera: %v", err)
	}
	if created.ONVIFPort == 0 {
		t.Fatal("expected AddCamera to assign a default onvif port")
	}
	if created.ONVIFMAC == "" || created.ONVIFSerial == "" {
		t.Fatal("expected AddCamera to assign default onvif mac/serial")
	}

	if _, err := store.AddCamera(CameraConfig{Name: "front", Host: "10.0.0.1"}, 8102); err == nil {
		t.Fatal("expected AddCamera to reject a duplicate name")
	}

	// Reload from disk to confirm the add was actually persisted.
	reloaded, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("reload after add: %v", err)
	}
	if len(reloaded.Cameras()) != 1 || reloaded.Cameras()[0].Name != "front" {
		t.Fatalf("expected persisted camera 'front', got %+v", reloaded.Cameras())
	}

	updated, err := store.UpdateCamera("front", CameraConfig{Name: "front", Host: "192.168.1.99", Password: "new"}, 8102)
	if err != nil {
		t.Fatalf("UpdateCamera: %v", err)
	}
	if updated.Host != "192.168.1.99" {
		t.Fatalf("expected updated host, got %q", updated.Host)
	}
	if updated.ONVIFPort != created.ONVIFPort || updated.ONVIFMAC != created.ONVIFMAC {
		t.Fatalf("expected onvif identity to be preserved across an update that didn't set it explicitly, got port=%d mac=%q", updated.ONVIFPort, updated.ONVIFMAC)
	}

	if _, err := store.UpdateCamera("does-not-exist", CameraConfig{Name: "x", Host: "1.2.3.4"}, 8102); err == nil {
		t.Fatal("expected UpdateCamera on unknown camera to fail")
	}

	if err := store.RemoveCamera("front"); err != nil {
		t.Fatalf("RemoveCamera: %v", err)
	}
	if len(store.Cameras()) != 0 {
		t.Fatalf("expected no cameras after remove, got %d", len(store.Cameras()))
	}

	if err := store.RemoveCamera("front"); err == nil {
		t.Fatal("expected RemoveCamera on already-removed camera to fail")
	}

	reloaded2, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("reload after remove: %v", err)
	}
	if len(reloaded2.Cameras()) != 0 {
		t.Fatalf("expected removal to be persisted, got %d cameras", len(reloaded2.Cameras()))
	}
}

func TestConfigStoreSeedsNonCameraSectionsOnFirstCreateAndPreservesThemOnEdit(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yml")
	defaults := Config{
		Server: ServerConfig{RTSPAddress: ":9554", ONVIFBasePort: 9102},
		MQTT:   MQTTConfig{Broker: "tcp://10.0.0.1:1883", Topic: "reolinkproxy"},
		ONVIF:  ONVIFConfig{Username: "admin", Password: "hunter2"},
	}

	store, err := newConfigStore(path, defaults)
	if err != nil {
		t.Fatalf("newConfigStore: %v", err)
	}
	if store.cfg.Server.RTSPAddress != ":9554" || store.cfg.MQTT.Broker != "tcp://10.0.0.1:1883" || store.cfg.ONVIF.Username != "admin" {
		t.Fatalf("expected a freshly created store to be seeded from defaults, got %+v", store.cfg)
	}

	// A camera-only mutation must not blank out the seeded mqtt/server/onvif sections.
	if _, err := store.AddCamera(CameraConfig{Name: "front", Host: "192.168.1.10"}, 9102); err != nil {
		t.Fatalf("AddCamera: %v", err)
	}

	reloaded, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.cfg.Server.RTSPAddress != ":9554" {
		t.Fatalf("expected rtsp_address to survive a camera edit, got %q", reloaded.cfg.Server.RTSPAddress)
	}
	if reloaded.cfg.MQTT.Broker != "tcp://10.0.0.1:1883" {
		t.Fatalf("expected mqtt broker to survive a camera edit, got %q", reloaded.cfg.MQTT.Broker)
	}
	if reloaded.cfg.ONVIF.Password != "hunter2" {
		t.Fatalf("expected onvif password to survive a camera edit, got %q", reloaded.cfg.ONVIF.Password)
	}
}

func TestConfigStoreReplaceCamerasIfEmptyOnlyAppliesOnce(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yml")
	store, err := newConfigStore(path, Config{})
	if err != nil {
		t.Fatalf("newConfigStore: %v", err)
	}

	if err := store.ReplaceCamerasIfEmpty([]CameraConfig{{Name: "env-cam", Host: "10.0.0.5"}}, 8102); err != nil {
		t.Fatalf("ReplaceCamerasIfEmpty: %v", err)
	}
	if len(store.Cameras()) != 1 {
		t.Fatalf("expected env bootstrap to seed one camera, got %d", len(store.Cameras()))
	}

	// A second bootstrap attempt (e.g. next process start with the same env vars) must not
	// override cameras that have since been edited via the store/web UI.
	if err := store.ReplaceCamerasIfEmpty([]CameraConfig{{Name: "other", Host: "10.0.0.6"}}, 8102); err != nil {
		t.Fatalf("ReplaceCamerasIfEmpty (second call): %v", err)
	}
	if len(store.Cameras()) != 1 || store.Cameras()[0].Name != "env-cam" {
		t.Fatalf("expected first bootstrap to stick, got %+v", store.Cameras())
	}
}
