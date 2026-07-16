package main

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

// ConfigStore is the on-disk YAML config file backing the web UI's camera add/edit/remove
// flow. Cameras are the only thing the web UI mutates; other sections (mqtt/server/onvif)
// are preserved as loaded so a hand-edited file never gets silently clobbered by a save
// triggered from the UI.
type ConfigStore struct {
	mu   sync.Mutex
	path string
	cfg  Config
}

// newConfigStore loads path if it exists, or creates one seeded from defaults (the
// CLI-flag/env-resolved config) so a first-run file reflects real effective settings
// instead of blank ones, and so persisting a later camera-only edit doesn't clobber
// mqtt/server/onvif sections back to zero values. defaults.Cameras is ignored; cameras are
// only ever set through ReplaceCamerasIfEmpty/AddCamera/UpdateCamera/RemoveCamera.
func newConfigStore(path string, defaults Config) (*ConfigStore, error) {
	s := &ConfigStore{path: path}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		s.cfg = defaults
		s.cfg.Cameras = nil
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var fileCfg Config
	if err := yaml.Unmarshal(data, &fileCfg); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}
	s.cfg = fileCfg
	return s, nil
}

// Cameras returns a copy of the current camera list.
func (s *ConfigStore) Cameras() []CameraConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]CameraConfig(nil), s.cfg.Cameras...)
}

// ReplaceCamerasIfEmpty seeds the store from cams (e.g. REOLINK_CAMERA_* env vars) only if
// the file doesn't already define any cameras, so the file - once populated - always wins
// on subsequent runs and env vars behave as a first-run bootstrap only.
func (s *ConfigStore) ReplaceCamerasIfEmpty(cams []CameraConfig, onvifBasePort int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cfg.Cameras) > 0 || len(cams) == 0 {
		return nil
	}

	for i := range cams {
		applyCameraDefaults(&cams[i], i, onvifBasePort)
		if err := validateCameraConfig(&cams[i]); err != nil {
			return fmt.Errorf("camera %d: %w", i, err)
		}
	}
	s.cfg.Cameras = cams
	return s.persistLocked()
}

// AddCamera validates, defaults, appends, and persists a new camera.
func (s *ConfigStore) AddCamera(cam CameraConfig, onvifBasePort int) (CameraConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, existing := range s.cfg.Cameras {
		if existing.Name == cam.Name {
			return CameraConfig{}, fmt.Errorf("camera %q already exists", cam.Name)
		}
	}

	applyCameraDefaults(&cam, len(s.cfg.Cameras), onvifBasePort)
	if err := validateCameraConfig(&cam); err != nil {
		return CameraConfig{}, err
	}

	s.cfg.Cameras = append(s.cfg.Cameras, cam)
	if err := s.persistLocked(); err != nil {
		return CameraConfig{}, err
	}
	return cam, nil
}

// UpdateCamera replaces the named camera's fields and persists the change. If the update
// leaves ONVIFPort/ONVIFMAC/ONVIFSerial unset, the existing values are kept so editing e.g.
// a password doesn't silently reassign identity Protect has already adopted. Stream/Channel
// are always kept from the existing record regardless of what the update carries - they
// aren't exposed as editable in the web UI (getting them wrong silently drops a stream
// tier, e.g. Low/Sub), so a caller has no legitimate way to change them through this path.
func (s *ConfigStore) UpdateCamera(name string, updated CameraConfig, onvifBasePort int) (CameraConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := -1
	for i, existing := range s.cfg.Cameras {
		if existing.Name == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return CameraConfig{}, fmt.Errorf("camera %q not found", name)
	}

	if updated.ONVIFPort == 0 {
		updated.ONVIFPort = s.cfg.Cameras[idx].ONVIFPort
	}
	if updated.ONVIFMAC == "" {
		updated.ONVIFMAC = s.cfg.Cameras[idx].ONVIFMAC
	}
	if updated.ONVIFSerial == "" {
		updated.ONVIFSerial = s.cfg.Cameras[idx].ONVIFSerial
	}
	updated.Stream = s.cfg.Cameras[idx].Stream
	updated.Channel = s.cfg.Cameras[idx].Channel

	applyCameraDefaults(&updated, idx, onvifBasePort)
	if err := validateCameraConfig(&updated); err != nil {
		return CameraConfig{}, err
	}

	s.cfg.Cameras[idx] = updated
	if err := s.persistLocked(); err != nil {
		return CameraConfig{}, err
	}
	return updated, nil
}

// ProtectServerIP returns the currently configured expected NVR IP address.
func (s *ConfigStore) ProtectServerIP() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.Server.ProtectServerIP
}

// SetProtectServerIP updates and persists the expected NVR IP address used by the web UI's
// connected/offline indicator. Unlike camera edits this takes effect immediately - it's
// purely informational (no ONVIF/RTSP negotiation depends on it) so it doesn't require a
// restart.
func (s *ConfigStore) SetProtectServerIP(ip string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg.Server.ProtectServerIP = ip
	return s.persistLocked()
}

// RemoveCamera deletes the named camera and persists the change.
func (s *ConfigStore) RemoveCamera(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := s.cfg.Cameras[:0]
	found := false
	for _, existing := range s.cfg.Cameras {
		if existing.Name == name {
			found = true
			continue
		}
		out = append(out, existing)
	}
	if !found {
		return fmt.Errorf("camera %q not found", name)
	}

	s.cfg.Cameras = out
	return s.persistLocked()
}

// persistLocked writes the config atomically (write to a temp file, then rename) so a crash
// mid-write never corrupts the file the web UI and the next process start both depend on.
func (s *ConfigStore) persistLocked() error {
	data, err := yaml.Marshal(&s.cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write temp config file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace config file %s: %w", s.path, err)
	}
	return nil
}
