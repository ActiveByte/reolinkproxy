FROM --platform=$BUILDPLATFORM golang:1.26.3-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./

RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=none

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X main.Version=${VERSION} -X main.Commit=${COMMIT}" -o /out/reolinkproxy ./cmd/reolinkproxy

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gstreamer1.0-tools \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-bad \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin reolinkproxy

COPY --from=build /out/reolinkproxy /usr/local/bin/reolinkproxy

# /data holds config.yml, persisted via a volume. Pre-created and owned by the runtime
# user here so a named volume (which inherits an image directory's ownership on first
# mount) works out of the box; a host bind mount still needs to be chowned to 65532:65532
# manually since it replaces this directory's ownership with the host path's own.
RUN mkdir -p /data && chown 65532:65532 /data
ENV REOLINK_CONFIG_FILE=/data/config.yml

USER 65532:65532

EXPOSE 8554/tcp
EXPOSE 8000/udp
EXPOSE 8001/udp
EXPOSE 8002/tcp
EXPOSE 3702/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 CMD ["/usr/local/bin/reolinkproxy", "healthcheck", "--rtsp-only"]

ENTRYPOINT ["/usr/local/bin/reolinkproxy"]
