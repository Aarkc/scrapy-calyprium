# [1.14.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.13.2...v1.14.0) (2026-04-21)


### Features

* add VEIL_PROVIDER setting for upstream provider selection ([ad542bd](https://github.com/Aarkc/scrapy-calyprium/commit/ad542bda8ba9d76aaabb7615b9a1578e4b7b84b9))

## [1.13.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.13.1...v1.13.2) (2026-04-20)


### Performance Improvements

* **local:** skip httpcloak light probe for known-cookies domains ([54fe9c9](https://github.com/Aarkc/scrapy-calyprium/commit/54fe9c9eba17a5a5c6f5e493f57c0b9e3bb82f51))

## [1.13.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.13.0...v1.13.1) (2026-04-20)


### Bug Fixes

* **request_tracer:** syntax error — with/else is not valid Python ([046f544](https://github.com/Aarkc/scrapy-calyprium/commit/046f544212576ff697bff98b8c4d9cf6a3a7f16e))

# [1.13.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.12.0...v1.13.0) (2026-04-20)


### Features

* **extensions:** CalypriumRequestTracer — per-URL trace spans ([18d4af0](https://github.com/Aarkc/scrapy-calyprium/commit/18d4af068f7f5da9cf62af3a97e83a9c92aa40cd))

# [1.12.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.7...v1.12.0) (2026-04-15)


### Features

* **extensions:** CalypriumRunStats — per-run telemetry to Forge ([68b7973](https://github.com/Aarkc/scrapy-calyprium/commit/68b79735d7ea53b88167b6dd2e9cf77a5af371e0))


### Reverts

* **local:** MAX_SLOT_FAILURES back to 3 — 1 made block rate worse ([db66e10](https://github.com/Aarkc/scrapy-calyprium/commit/db66e101b30cab3168f610aec76c86a2834ccfd9))

## [1.11.7](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.6...v1.11.7) (2026-04-14)


### Performance Improvements

* **local:** reduce bandwidth waste — MAX_SLOT_FAILURES=1, hard-cap RPM ([d38b9d2](https://github.com/Aarkc/scrapy-calyprium/commit/d38b9d2cd3e88ae456992c9f1312149821b7d98f))

## [1.11.6](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.5...v1.11.6) (2026-04-13)


### Bug Fixes

* **prism-sitemap:** separate download slot for Prism pagination ([75818e2](https://github.com/Aarkc/scrapy-calyprium/commit/75818e29aa0e58120e4fd6dd5e9009092c75a5ae))

## [1.11.5](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.4...v1.11.5) (2026-04-11)


### Bug Fixes

* **local:** suppress HEAVY promotion on all_slots_exhausted ([b0a30e5](https://github.com/Aarkc/scrapy-calyprium/commit/b0a30e5235e38549d4174ef7a11642a6394c5f40))

## [1.11.4](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.3...v1.11.4) (2026-04-10)


### Bug Fixes

* **prism-sitemap:** revert filter split + fast-skip fresh prefix ([e537a7a](https://github.com/Aarkc/scrapy-calyprium/commit/e537a7a1fd4f7b88d0043c2073d820c3e93ca10d))

## [1.11.3](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.2...v1.11.3) (2026-04-10)


### Bug Fixes

* **prism-sitemap:** separate freshness tracking from freshness filtering ([daed5e3](https://github.com/Aarkc/scrapy-calyprium/commit/daed5e3d04c1b0a1fe5c59f58143065323b5e449))

## [1.11.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.1...v1.11.2) (2026-04-09)


### Bug Fixes

* **local:** cold-start burst refill + 1s default interval ([cc4266c](https://github.com/Aarkc/scrapy-calyprium/commit/cc4266caa003f9bcf5bcc239f9feacaf7451d9d8))

## [1.11.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.11.0...v1.11.1) (2026-04-09)


### Bug Fixes

* **local:** drive refill from fetch hot path, not background asyncio task ([aa24820](https://github.com/Aarkc/scrapy-calyprium/commit/aa24820d6a2a67ccc134072535835ae04bb72ec4))

# [1.11.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.10.0...v1.11.0) (2026-04-09)


### Features

* **local:** proactive cookie pool refill (Phase 5) ([1d51b11](https://github.com/Aarkc/scrapy-calyprium/commit/1d51b11523605f5e942ae1151528a4abfbadd022))

# [1.10.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.9.0...v1.10.0) (2026-04-09)


### Features

* **local:** end-to-end egress_ip feedback to Mimic per-(domain, IP) reputation ([8d8f03b](https://github.com/Aarkc/scrapy-calyprium/commit/8d8f03bf4f62c90cd006077df6a63a3edb309a80))

# [1.9.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.8.2...v1.9.0) (2026-04-08)


### Features

* **local:** silent-failure feedback channel from spider parse callbacks ([fd757cc](https://github.com/Aarkc/scrapy-calyprium/commit/fd757ccf575d7ce2af641b4d7b7f909174f9e2f9))

## [1.8.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.8.1...v1.8.2) (2026-04-08)


### Bug Fixes

* **prism-sitemap:** pass + track prism_offset cursor through recrawl batches ([131f17d](https://github.com/Aarkc/scrapy-calyprium/commit/131f17d2710b469bb4cd3e1a3f52c9c02a2c59ca))

## [1.8.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.8.0...v1.8.1) (2026-04-08)


### Bug Fixes

* **local:** wrap text/html responses in HtmlResponse, not Response (AAR-17) ([5366a05](https://github.com/Aarkc/scrapy-calyprium/commit/5366a052956796eb1b5c41918f2136efae0f9096))

# [1.8.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.7.4...v1.8.0) (2026-04-08)


### Features

* **local:** adaptive per-slot rate cap + slot-stats reporter (AAR-17 follow-up) ([fe3b851](https://github.com/Aarkc/scrapy-calyprium/commit/fe3b85164e0e7ae78832dbfa09464ae6cbce40a9))

## [1.7.4](https://github.com/Aarkc/scrapy-calyprium/compare/v1.7.3...v1.7.4) (2026-04-08)


### Bug Fixes

* **spectre:** tighten block detector to use real challenge markers (AAR-5) ([9d60ec2](https://github.com/Aarkc/scrapy-calyprium/commit/9d60ec2aa1eab81ba48ca0b2a88e46240f6df68e))

## [1.7.3](https://github.com/Aarkc/scrapy-calyprium/compare/v1.7.2...v1.7.3) (2026-04-08)


### Bug Fixes

* **mimic:** tighten block detector to use real challenge markers (AAR-5) ([f5f595d](https://github.com/Aarkc/scrapy-calyprium/commit/f5f595dbaa680fe4b1e1efabf3c06a29d8c93a12))

## [1.7.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.7.1...v1.7.2) (2026-04-08)


### Bug Fixes

* **local-fetch:** strip Content-Encoding/Length + coalesce concurrent solves (AAR-17) ([98df68e](https://github.com/Aarkc/scrapy-calyprium/commit/98df68ed2118ecbbd22f091a85be5774b38a0796))

## [1.7.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.7.0...v1.7.1) (2026-04-08)


### Bug Fixes

* **local-fetch:** flatten list-valued httpcloak headers (AAR-17) ([416159c](https://github.com/Aarkc/scrapy-calyprium/commit/416159cc5d581f4941f096b088fbaad743b383f1))

# [1.7.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.6.0...v1.7.0) (2026-04-07)


### Features

* local-first auto-routing (AAR-15 + AAR-17) ([16bfb7a](https://github.com/Aarkc/scrapy-calyprium/commit/16bfb7a51b9f39f968fdc0be33faf29a2f71c2b7))

# [1.6.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.5.1...v1.6.0) (2026-04-05)


### Features

* add crawl targets pipeline and targets:// URL source ([eb1ba0b](https://github.com/Aarkc/scrapy-calyprium/commit/eb1ba0b0d05687b41e8e7b78ba66cc39b6ee4673))

## [1.5.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.5.0...v1.5.1) (2026-04-04)


### Bug Fixes

* advance Prism offset by raw batch size, not filtered count ([bdab72f](https://github.com/Aarkc/scrapy-calyprium/commit/bdab72fdf7e2f0dda11aedc2bb63fd446bea9a3b))

# [1.5.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.6...v1.5.0) (2026-04-04)


### Features

* filter fresh URLs from Prism batches when recrawl tracking is enabled ([4f9e6eb](https://github.com/Aarkc/scrapy-calyprium/commit/4f9e6ebf6627abd42618c1fe97e3bda3ce2d133a))

## [1.4.6](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.5...v1.4.6) (2026-04-02)


### Bug Fixes

* replace em dash with ASCII to fix UTF-8 encoding error on some platforms ([de33d5f](https://github.com/Aarkc/scrapy-calyprium/commit/de33d5fb5f624bce193f813f37f6960739a53515))

## [1.4.5](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.4...v1.4.5) (2026-04-02)


### Bug Fixes

* lazy loading for recrawl:// URL source (fetch one batch at a time) ([d479da7](https://github.com/Aarkc/scrapy-calyprium/commit/d479da7c3434ef0eb67250cc5becba54bae81c70))

## [1.4.4](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.3...v1.4.4) (2026-04-02)


### Bug Fixes

* use FORGE_SERVICE_SECRET for Forge API auth (not CALYPRIUM_API_KEY) ([5395584](https://github.com/Aarkc/scrapy-calyprium/commit/539558417ad415be35400e0a7897b6f177bce972))

## [1.4.3](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.2...v1.4.3) (2026-04-02)


### Bug Fixes

* remove stray paren causing SyntaxError in recrawl pipeline ([87602ad](https://github.com/Aarkc/scrapy-calyprium/commit/87602add26b382fe20a5bbd1133183d1a8913170))

## [1.4.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.1...v1.4.2) (2026-04-02)


### Bug Fixes

* use real user_id for Forge API auth in recrawl spider and pipeline ([2728fc6](https://github.com/Aarkc/scrapy-calyprium/commit/2728fc669a4fe2a0cc51ec1817cdba52b048fe21))

## [1.4.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.4.0...v1.4.1) (2026-04-02)


### Bug Fixes

* use direct HTTP for recrawl:// URL source (auth + proxy bypass) ([fc06668](https://github.com/Aarkc/scrapy-calyprium/commit/fc0666881b971200c6aa062b8e1c29d96a14939f))

# [1.4.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.3.4...v1.4.0) (2026-04-02)


### Features

* add RecrawlTrackingPipeline and recrawl:// URL source ([afa8b79](https://github.com/Aarkc/scrapy-calyprium/commit/afa8b790df9695cd1539686a855bab6393536cae))

## [1.3.4](https://github.com/Aarkc/scrapy-calyprium/compare/v1.3.3...v1.3.4) (2026-03-31)


### Bug Fixes

* prevent cascading refills with _refill_in_flight flag ([2f23ddb](https://github.com/Aarkc/scrapy-calyprium/commit/2f23ddb03e77dd3ab7beaf8d5a6d2cd9e75d042f))

## [1.3.3](https://github.com/Aarkc/scrapy-calyprium/compare/v1.3.2...v1.3.3) (2026-03-31)


### Bug Fixes

* truly lazy Prism pagination via callback-driven refill ([5101512](https://github.com/Aarkc/scrapy-calyprium/commit/510151222d1758897c2e3216f92c5ae47504e331))

## [1.3.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.3.1...v1.3.2) (2026-03-31)


### Bug Fixes

* use low priority for Prism chain requests to reduce queue buildup ([28a67a9](https://github.com/Aarkc/scrapy-calyprium/commit/28a67a9566180b68fef307ef3d84515f5f8fef09))

## [1.3.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.3.0...v1.3.1) (2026-03-31)


### Bug Fixes

* set 120s download_timeout on Prism API chain requests ([531bcba](https://github.com/Aarkc/scrapy-calyprium/commit/531bcba35f8affd0aa23b27f0541bbc488e4cfad))

# [1.3.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.2.0...v1.3.0) (2026-03-30)


### Features

* lazy Prism pagination and _internal request bypass ([6b9b49b](https://github.com/Aarkc/scrapy-calyprium/commit/6b9b49b14ecf5d5b0d1eeaffac75bc83ce7701ac))

# [1.2.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.1.0...v1.2.0) (2026-03-29)


### Features

* add auto-routing mode to MimicBrowserMiddleware ([919c16e](https://github.com/Aarkc/scrapy-calyprium/commit/919c16ed666fd25ec40ac11a178052b8e0646646))

# [1.1.0](https://github.com/Aarkc/scrapy-calyprium/compare/v1.0.3...v1.1.0) (2026-03-29)


### Features

* add SpectreMiddleware and S3BatchPipeline ([e6c8ab4](https://github.com/Aarkc/scrapy-calyprium/commit/e6c8ab4ef8033adad0e9e9afa60ad5dc0814aed7))

## [1.0.3](https://github.com/Aarkc/scrapy-calyprium/compare/v1.0.2...v1.0.3) (2026-03-29)


### Bug Fixes

* **ci:** use cycjimmy/semantic-release-action for proper step outputs ([a248689](https://github.com/Aarkc/scrapy-calyprium/commit/a2486895211fd8690e3c69eadd588a89c65d5a77))

## [1.0.2](https://github.com/Aarkc/scrapy-calyprium/compare/v1.0.1...v1.0.2) (2026-03-29)


### Bug Fixes

* re-trigger PyPI publish ([0af0e43](https://github.com/Aarkc/scrapy-calyprium/commit/0af0e43eecbcb0fdfae91ae5fda4c91eaa5781fe))

## [1.0.1](https://github.com/Aarkc/scrapy-calyprium/compare/v1.0.0...v1.0.1) (2026-03-29)


### Bug Fixes

* trigger initial PyPI publish ([965b72f](https://github.com/Aarkc/scrapy-calyprium/commit/965b72f4966907cb39608d2d50599ececcaa5e70))
* trigger initial PyPI publish ([d5aee5c](https://github.com/Aarkc/scrapy-calyprium/commit/d5aee5c90fab414a9f903ec728909e56374d2f8c))

# 1.0.0 (2026-03-29)


### Bug Fixes

* remove Docker defaults, MINIO fallbacks, and internal URLs ([ee56245](https://github.com/Aarkc/scrapy-calyprium/commit/ee5624544426b9d8f6685a59a1f45fc5f1ce642f))
* remove internal storage module from public SDK ([2f2790f](https://github.com/Aarkc/scrapy-calyprium/commit/2f2790f62900fdf878b914f16cd2b24d043799c8))
* remove per-service API key fields, single CALYPRIUM_API_KEY only ([3d6005b](https://github.com/Aarkc/scrapy-calyprium/commit/3d6005b15d709b1634cbacccd47b2e569dea4c70))


### Features

* add S3 feed storage via Forge gateway ([32f13ea](https://github.com/Aarkc/scrapy-calyprium/commit/32f13ea5fa3c771a9b580a359a4a2dd15263a7d3))
* initial release of scrapy-calyprium ([0016597](https://github.com/Aarkc/scrapy-calyprium/commit/001659761222119631c9b6fa71885b2ac1bdd1c0))
