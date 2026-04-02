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
