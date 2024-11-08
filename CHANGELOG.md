# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/8xFF/atm0s-small-p2p/compare/v0.1.0...v0.2.0) - 2024-11-08

### Added

- pubsub ([#12](https://github.com/8xFF/atm0s-small-p2p/pull/12))

### Fixed

- *(deps)* update rust crate thiserror to v2 ([#17](https://github.com/8xFF/atm0s-small-p2p/pull/17))
- clippy warns ([#19](https://github.com/8xFF/atm0s-small-p2p/pull/19))
- alias don't switch from check to scan after timeout ([#18](https://github.com/8xFF/atm0s-small-p2p/pull/18))

### Other

- release v0.1.0 ([#14](https://github.com/8xFF/atm0s-small-p2p/pull/14))

## [0.1.0](https://github.com/8xFF/atm0s-small-p2p/releases/tag/v0.1.0) - 2024-10-23

### Added

- secure HandshakeProtocol trait and simple SharedKeyHandshake ([#9](https://github.com/8xFF/atm0s-small-p2p/pull/9))
- auto update rtt each 1 seconds ([#7](https://github.com/8xFF/atm0s-small-p2p/pull/7))
- added config seeds for avoiding manual process with seeds ([#6](https://github.com/8xFF/atm0s-small-p2p/pull/6))
- switch to use peer_id for send and recv ([#5](https://github.com/8xFF/atm0s-small-p2p/pull/5))
- visualization service ([#3](https://github.com/8xFF/atm0s-small-p2p/pull/3))
- minimum working version with service pkt and stream

### Fixed

- verify handshake when different timestamp with 2 node ([#10](https://github.com/8xFF/atm0s-small-p2p/pull/10))
- fix wrong unwrap ([#8](https://github.com/8xFF/atm0s-small-p2p/pull/8))
- quic connection closed with error failed to fill whole buffer ([#4](https://github.com/8xFF/atm0s-small-p2p/pull/4))

### Other

- *(deps)* update actions/cache action to v4 ([#13](https://github.com/8xFF/atm0s-small-p2p/pull/13))
- config github actions
- add renovate.json ([#1](https://github.com/8xFF/atm0s-small-p2p/pull/1))
- rust CI
- expose P2pQuicStream and make Requester cloneable ([#2](https://github.com/8xFF/atm0s-small-p2p/pull/2))
