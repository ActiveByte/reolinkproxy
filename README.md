# Reolink Proxy

A lightweight Go proxy that translates Reolink's proprietary Baichuan protocol into standard RTSP streams and a compliant per-camera ONVIF API.

> This is a fork of [shareed2k/reolinkproxy](https://github.com/Shareed2k/reolinkproxy), rebuilt around per-camera ONVIF identity, a transparent Media2 reverse-proxy, real camera ONVIF event forwarding, and a persisted config with a status/config web UI - see [Background](#background) for why.

## Background

Reolink cameras' own RTSP/ONVIF stream is unreliable for continuous NVR ingestion - it stalls, drops frames, or desyncs under sustained pull. The Baichuan protocol (TCP port 9000, proprietary, used internally by the official Reolink app/NVR) is far more stable, so this proxy speaks Baichuan to the camera instead and re-exposes it as a standard RTSP + ONVIF source.

This was built specifically to get Reolink cameras adopted cleanly by UniFi Protect, which doesn't do its own motion detection for third-party cameras and expects one ONVIF device per camera rather than a shared multi-channel "NVR". That shaped the two biggest design choices:

## Features

* Connects to cameras by local IP.
* Repackages H.264/H.265 video to RTSP without transcoding, with steady, evenly-paced RTP timing.
* Transcodes Reolink ADPCM audio to PCMA and passes AAC through.
* Per-camera ONVIF service, transparently reverse-proxied to the camera's own ONVIF service - including its real events (motion, smart detections) forwarded to NVRs.
* Proxies real camera snapshots/thumbnails for ONVIF.
* Broadcasts WS-Discovery for local ONVIF discovery.
* Optionally pulls the camera's higher-quality extern channel in place of sub for the sub stream.
* Publishes MQTT motion and control topics for Home Assistant and similar systems.
* Status/config web UI: add/edit/monitor cameras, live server stats, scrollable events log, adjustable log level.

## Configuration

Cameras are managed through the web UI

## Building from Source

```bash
git clone https://github.com/ActiveByte/reolinkproxy.git
cd reolinkproxy
go build -o reolinkproxy ./cmd/reolinkproxy
./reolinkproxy
```

## License

MIT. See [LICENSE](LICENSE).