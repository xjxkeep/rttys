# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rttys is the server component of the [rtty](https://github.com/zhaojh329/rtty) remote terminal system. It's a Go server with an embedded Vue 3 SPA frontend that manages remote device connections, provides browser-based terminal sessions via WebSocket, HTTP proxying through devices, and command execution.

## Build Commands

### Frontend (ui/)
```bash
cd ui && npm install && npm run build   # Production build → ui/dist/
cd ui && npm run dev                     # Dev server with hot reload (proxies to :5913)
```

### Backend
```bash
# Copy ui/dist/ to assets/dist/ first, then:
./build.sh linux amd64                   # Cross-compile (linux|windows, amd64|arm64)
./build-deb.sh amd64                     # Build Debian package

# Docker (multi-stage: builds frontend + backend)
docker build -t rttys .
```

The backend embeds frontend assets via `embed.FS` (`embed.go`). The build script sets `-ldflags` with `GitCommit` and `BuildTime`, uses `CGO_ENABLED=0` for static binaries.

### Tests
```bash
go test -v ./... -run TestRttysStress -timeout 10m
```
Only stress tests exist (`rttys_stress_test.go`), testing multi-terminal sessions and HTTP proxying. Configurable via constants: `testTermPerDev`, `testAckBlkSize`, `testHttp`.

## Architecture

**Three concurrent listeners form the core:**

1. **Device Listener** (`ListenDevices()`, default `:5912`) — Accepts TCP/TLS connections from rtty clients on remote devices using RTTY protocol v3. Supports mTLS.

2. **API/User Listener** (`ListenAPI()`, default `:5913`) — Gin-based HTTP server serving the embedded frontend, REST API, and WebSocket connections for terminal sessions. Cookie-based session auth.

3. **HTTP Proxy Listener** (`ListenHttpProxy()`, auto-assigned port) — Proxies HTTP requests through connected devices with session-based routing.

**Central struct:** `RttyServer` (in `server.go`) orchestrates everything, storing devices and groups in `sync.Map` for thread safety.

**Key files:**
- `main.go` — CLI entry point (urfave/cli)
- `server.go` — Server orchestration, device/group management
- `device.go` — Device connection lifecycle, RTTY protocol handling
- `user.go` — User/terminal WebSocket sessions
- `api.go` — REST endpoints, auth middleware, static file serving
- `http.go` — HTTP proxy implementation
- `command.go` — Remote command execution on devices
- `config.go` — YAML + CLI flag configuration parsing

**Data flow:** Remote device → TCP/TLS → Device struct → WebSocket → Browser (xterm.js)

**Frontend (ui/src/):**
- Vue 3 + Vue Router + Element Plus + vue-i18n
- `RttyTerm.vue` — Core terminal component (xterm.js)
- `Home.vue` — Device list with group filtering
- Routes: `/login`, `/` (home), `/rtty/:devid` (terminal), `/error/:err`
- Auth guard checks `/alive` endpoint before navigation

## Configuration

See `rttys.conf` for all options. Key settings: `addr-dev`, `addr-user`, `token` (device auth), `password` (web UI), `dev-hook-url`/`user-hook-url` (webhook callbacks), TLS certs for mTLS.
