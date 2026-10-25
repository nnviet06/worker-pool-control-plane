# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- 2026-09-18: design document (`docs/design.md`) and roadmap
  (`docs/roadmap.md`).
- 2026-10-24: Go module, `cmd/wpcp` skeleton, Makefile (`make vet`,
  `make test` with the race detector) and CI workflow (phase 0).
- 2026-10-25: prototype. Admission control with a bounded queue, P2C
  placement, gRPC transport, health checks, ejection, failover under a
  retry budget, fakeworker and k6 scripts.
