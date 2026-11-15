# pocket-signal-assistant

Android application for market analysis and short-term UP/DOWN recommendations.

## Current status

The repository contains a FastAPI/SQLite backend and a Kotlin/Jetpack Compose
Android client. Implemented workflows include provider catalog and candle
loading, live tick streaming, candle formation, technical analysis, signal
records, sessions, results, statistics, chronological backtesting, and a
label-gated model-training pipeline. A signal is not promoted to UP/DOWN until
the current data and a temporally validated model meet the configured checks.
Real provider credentials, an authorized history dataset, deployment security,
release signing, and on-device validation are still required before real-world
use.

OTC instruments and prices will only be enabled when an official or otherwise
authorized Pocket Option OTC source is available. Classic FX quotes from a third
party will remain labeled with their actual provider and will not be represented
as Pocket Option prices.

## Run the backend

Requires Python 3.12 or newer.

```sh
cd backend
python -m venv .venv
. .venv/bin/activate
pip install -e ".[test]"
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Deploy on Render Free

The backend includes a root `render.yaml` Blueprint and `backend/Dockerfile`.
The Blueprint uses Render Free and does not request or configure a persistent
disk or paid plan. `DATABASE_PATH` points to
`/tmp/pocket-signal/signals.db`; the Docker image creates this directory for the
non-root application user, and the backend initializes a new SQLite database
and schema at startup if none exists.

The backend operator may set `TWELVE_DATA_API_KEY` in the existing Render web
service's Environment settings as a server-wide fallback. The Blueprint
declares it as a secret prompt (`sync: false`); its value is not stored in this
repository. After saving the environment variable, Render restarts the service
and refreshes the Twelve Data Forex catalog. Android users can instead enter,
replace, verify, and delete their own key in **Settings → Data Sources and API
Keys**. The app encrypts that key with Android Keystore and sends it to the
backend only in an HTTPS header for Twelve Data requests; the backend does not
persist or log the per-device key. Never put a real key in source control or
the APK.

For automated Render deployments from GitHub, add two secret values in the
repository settings: `RENDER_API_KEY` (Render API key) and
`RENDER_SERVICE_ID` (the existing service ID, such as the current Render web
service ID). The workflow in `.github/workflows/render-deploy.yml` reads these
values and triggers a deploy on every push to `main` or a manual dispatch,
without exposing the secrets in code or logs. It waits for the Render deploy
and then checks backend health, source credentials, the Twelve Data catalog and
history endpoints, and the WebSocket handshake. These checks fail the workflow
when production is unhealthy or `TWELVE_DATA_API_KEY` is missing from the
existing Render service's Environment settings.

Render Free has an ephemeral filesystem. SQLite data, including downloaded
HistData candles and sync cursors, can be lost when the service restarts,
redeploys, or spins down. The local development database is not copied to
Render. On an empty database, the startup HistData worker discovers available
full-year archives and downloads them in yearly batches; incomplete downloads
are retried on a later start. The locally verified 372,024 EUR/USD 2025 candles
can therefore be downloaded again, but are not guaranteed to persist on Free.
Check `GET /api/v1/sources/status` and `GET /api/v1/history/instruments` after
deployment to confirm current status and coverage. A successful health check
alone does not confirm that the updated API is deployed.

The backend has no API authentication, so do not expose it with user trading
history or use it for real trading. Check `GET /health` and
`GET /api/v1/catalog`; interactive API documentation is available at `/docs`.

Run backend tests from `backend/`:

```sh
pytest
```

## Android app

The Android client uses Kotlin/Jetpack Compose and connects to
`https://pocket-signal-assistant.onrender.com` by default. Set the
`SERVER_BASE_URL` environment variable or `-PserverBaseUrl=https://...` Gradle
property to override the server for a build. For local backend testing from the
Android emulator, use:

```sh
./gradlew assembleDebug -PapiBaseUrl=http://10.0.2.2:8000
```

The `apiBaseUrl` property takes precedence for debug builds; release builds use
`releaseApiBaseUrl`, then `SERVER_BASE_URL`, then the Render default.

Build a debug APK with an installed Android SDK and JDK 17+:

```sh
./gradlew assembleDebug
```

The APK is written to `app/build/outputs/apk/debug/app-debug.apk`. The client
includes instrument/market/expiry/timeframe selection, a live/historical
candlestick chart, signal analysis, sessions, results, history, statistics,
and device-encrypted Twelve Data key settings. The connection check refreshes
the real Twelve Data Forex catalog; the key is not stored on the backend. OTC
selection remains unavailable under the server's data-source policy unless a
real authorized OTC feed is implemented.

The Android Actions workflow builds and uploads the `PocketSignal-debug-apk`
artifact on pushes to `main` and manual `workflow_dispatch` runs. It also runs
for `v*` tag pushes; those tagged runs additionally build and publish a signed
release APK when release-signing secrets are configured.

## Market data and API

The existing Twelve Data adapter requires an API key, supplied either by the
backend operator or by an Android user in the app's encrypted settings. No
provider key is embedded in the APK. Twelve Data currently lists a no-cost Basic tier with real-time Forex,
8 API credits per minute (800 per day), and 8 trial WebSocket credits. This
still requires an account/API key, is quota-limited, and availability and
permitted use are governed by the provider's terms. Without a configured key,
the adapter stays unavailable and the app must not produce directional signals.
The backend refreshes its Forex catalog at startup and operators can also
trigger a refresh with `POST /api/v1/provider/twelvedata/catalog`. The app's
`POST /api/v1/provider/twelvedata/check` endpoint validates the supplied key by
retrieving the real provider catalog. Per-device keys are sent in the
`X-Twelve-Data-Api-Key` HTTPS header and are neither stored nor logged by the
backend. Historical
OHLC candles are available for supported intervals through
`POST /api/v1/history/twelvedata?symbol=EUR%2FUSD&interval_seconds=60`; the
Android Market Chart screen requests them for the selected pair and timeframe.
Twelve Data's WebSocket price endpoint is relayed at
`/api/v1/stream?symbol=EUR%2FUSD`. It records provider ticks and forms 1-second
and 1-minute live candles; the chart also updates its selected-timeframe candle
from the real-time tick stream. Provider plan limits, permitted instruments,
and latency still apply; provider quota errors are reported as HTTP 429. Check
`GET /api/v1/sources/status` for `online.credentials_configured` and
`online.connection_status` (`not_configured`, `configured_no_recent_quote`, or
`receiving_quotes`). This status reports only whether the backend has a
configured key and recent stream data; it never returns the key or makes an
extra provider request. The app never labels these prices as Pocket Option
quotes.

### Dukascopy historical data (TESTING preparation)

Dukascopy's public hourly tick archive can be requested without an API key.
The backend supports EUR/USD, GBP/USD, USD/JPY, USD/CHF, USD/CAD, AUD/USD, and
NZD/USD. List supported symbols and locally imported coverage with
`GET /api/v1/history/dukascopy/instruments`. Import at most three consecutive
UTC hours per request:

```sh
curl -X POST 'https://your-backend/api/v1/history/dukascopy/import?symbol=EUR%2FUSD&date=2025-01-02&start_hour=0&end_hour=3&interval_seconds=1'
```

`end_hour` is exclusive; `24` can be used for a range ending at UTC midnight,
but each request may span no more than three hours. Requests are sequential.
HTTP 429 receives at most two retries, uses `Retry-After` when supplied, and
stops rather than retrying early if the requested wait exceeds 30 seconds.
Missing files (HTTP 404) and intervals are not filled or interpolated. The
archive uses compressed binary `.bi5` tick files; the backend validates their
record structure, time order, bid/ask values, and volumes. It saves raw ticks
separately at their 1 ms timestamp resolution and separately creates candles
at the requested interval. Read raw ticks through
`GET /api/v1/history/dukascopy/ticks`; candle responses include their source
time resolution. The Android TESTING screen can import 1, 3, or 7 UTC calendar
days in sequential three-hour requests and reports downloaded/saved ticks,
candles, missing files, and gaps. It does not substitute candles for stored
ticks.

Run historical tests for a selected period and one or more expirations with
`GET /api/v1/backtest/dukascopy?symbol=EUR%2FUSD&period_days=1&expiry_seconds=60&expiry_seconds=300`.
The response includes the candle resolution, counts and gaps plus trade,
win/loss, and Win Rate results for each expiration. 5–30 second expirations
are offered only when one-second candles derived from real ticks are present;
minute candles only support minute-aligned expirations of at least one minute.
The backtest uses a chronological holdout, forms each signal only from
completed candles before entry, and enters at the next candle's open. It reports
insufficient data/signals without inventing a Win Rate. Bid-only candle results
are not adjusted for spread, commission, or slippage and must not be interpreted
as cost-adjusted or live performance. Gap counts include market closures and
weekends; there is no Forex trading-hours calendar filter.

This is historical data for the TESTING/backtest stage, not a live feed. The
archive can have missing hours and access/rate limits; a recent request returned
HTTP 429, so successful real-file ingestion is not currently confirmed. Its bid
candles do not include spread in the stored OHLC. It must not be presented as a
free ONLINE quote stream or as Pocket Option OTC data.

### HistData public M1 archives (TESTING)

HistData publishes downloadable Generic ASCII M1 ZIP archives without an API
key. The backend downloads annual archives for the seven supported pairs using
the provider's public page/form flow, validates the ZIP and every OHLC row, and
saves idempotently as `provider=HistData`, `interval_seconds=60`, and
`source_resolution_ms=60000`. The CSV timestamps are fixed EST (UTC-5) without
daylight-saving changes; conversion to UTC uses that fixed offset. These are
bid-only minute bars, not tick data.

At backend startup the historical worker checks the provider's actual full-year
archive listing. It downloads the newest available complete year first, then
continues backward one year per pair and cycle to resume a persistent backfill.
The newest complete annual archive is rechecked weekly; a newly published full
year is detected from the listing. HistData has offered complete M1 years
2000–2025 for EUR/USD, GBP/USD, USD/JPY, USD/CHF, USD/CAD, and AUD/USD, and
2005–2025 for NZD/USD at the time checked. The public page also lists partial
current-year monthly archives, but those were not verifiably downloadable
through the automated form flow and are not treated as an update source.

`GET /api/v1/history/instruments` reports source-specific coverage and
resolutions. `GET /api/v1/backtest/history` uses the latest available historical date as the
end of the selected period, so stale archives can still be tested without being
mislabeled as current. M1-only history allows minute-aligned expirations of at
least 60 seconds; second-based expirations require actual one-second candles
derived from imported Dukascopy ticks. HistData does not provide a free keyless
ONLINE quote stream.

For genuinely keyless data, [Frankfurter](https://www.frankfurter.dev/) offers
historical reference rates derived from central-bank sources. At backend
startup, a background job automatically retrieves and stores history from
1999 onward for the seven supported pairs, then refreshes the recent range
twice daily. Its values are daily reference observations, stored separately
from candles and ticks, and are not suitable for intraday TESTING, short
expirations, or ONLINE signals. Read coverage using
`GET /api/v1/history/reference/instruments` and rates using
`GET /api/v1/history/reference?symbol=EUR%2FUSD`.

The backend also makes a bounded automatic Dukascopy scan of recent hourly
files and advances its per-pair scan cursor after successful requests; the
Android TESTING importer can request a selected recent period. Dukascopy
returned HTTP 429 during the last real check, so a successful real tick file
has not yet been downloaded in this environment. No requests bypass its rate
limits. Check provider and synchronization state at
`GET /api/v1/sources/status`.

Import licensed historical CSV data at `POST /api/v1/history/import` using the
columns `timestamp,open,high,low,close` and optional `volume`. Timestamps should
be ISO-8601; naive timestamps are interpreted as UTC. Invalid rows are reported
and skipped; duplicate timestamps are counted; missing intervals are reported,
not interpolated. Historical and live candles are stored and queried separately.

`POST /api/v1/analyze` only emits a directional signal when the current tick is
fresh, the observed stream rate is adequate, the technical candidate is strong,
and an active model has passed temporal validation. Otherwise it records and
returns `NO_SIGNAL`, with no invented probabilities. ONLINE uses the existing
Twelve Data WebSocket with the operator's configured key or the app user's
per-device key. There is no verified public keyless Forex source with a trading-grade
update frequency; the daily reference-rate source is never used as a live-feed
fallback. `/api/v1/sources/status` reports the latest quote and why ONLINE is
unavailable.

Session, result, and statistics APIs are `POST/GET /api/v1/sessions`,
`POST /api/v1/sessions/{id}/finish`, `GET /api/v1/signals`,
`PATCH /api/v1/signals/{id}/outcome`, and `GET /api/v1/statistics`.
`POST /api/v1/models/train` only considers completed WON/LOST directional trades,
requires at least 150 labeled outcomes, fits on the oldest 60%, calibrates on
the next 20%, and validates on the newest 20%. It requires 25 additional
outcomes before retraining an active model and does not activate a candidate
that fails the holdout or same-holdout comparison.

The minute-candle backtest generates each decision from candles completed
before the entry candle and evaluates entry at that next candle's open, avoiding
use of the entry candle's close when forming its signal.

This backend currently has no authentication or TLS termination; run it only
behind a trusted private network/reverse proxy. Never expose the development
server directly to the public internet.

A successful `assembleDebug` output is a debug build, not a distributable
release. Rebuild it after source changes. Live-provider checks, device
installation, release signing, and a production security review remain
unverified for the deployment environment.

## Data-source plan

- Frankfurter's no-key API provides official-source daily reference rates and
  historical series. Backend startup synchronization stores these in a
  dedicated table and API, never as synthetic candles or live prices.
- Dukascopy exposes public hourly `.bi5` archive URLs without an API-key
  parameter, but a recent real request returned HTTP 429. The backend now has a
  bounded, `Retry-After`-aware retry policy, automatic bounded scans, raw tick
  storage, and small-range imports; successful real-file ingestion remains
  unverified until the provider permits a request.
- The existing Twelve Data adapter supports catalog/history/WebSocket only when
  the backend operator configures a provider key. Its current Basic tier lists
  limited free real-time Forex access, but still requires registration/API key;
  no keyless live quote provider has been verified for production use.
- OTC live data: unavailable until an authorized Pocket Option-compatible source
  and instrument catalog are confirmed.

Historical prices are for research and backtesting, not live trading.
Second-based expirations require actual one-second historical candles created
from real source ticks; minute and daily reference observations are not
substitutes. Render Free uses an ephemeral filesystem, so downloaded history
and application records can be lost on restart, redeploy, or spin-down.

## Release automation and deployment blockers

The Android release workflow runs backend tests, Android lint/unit-test tasks,
builds a signed APK, stores it as a workflow artifact, and attaches it to a
tagged GitHub Release. It uses the repository's `SERVER_BASE_URL` variable when
set, otherwise the configured Render URL. Release signing still requires these
Actions secrets: `RELEASE_KEYSTORE_BASE64`, `RELEASE_STORE_PASSWORD`,
`RELEASE_KEY_ALIAS`, and `RELEASE_KEY_PASSWORD`. The keystore is never stored
in the repository.

The Render hostname responds to `/health`, but the currently deployed OpenAPI
does not include the new historical routes: `/api/v1/sources/status`,
`/api/v1/history/instruments`, and `/api/v1/backtest/history` return 404. The
updated Backend is not published until Render syncs this Blueprint and the
deployment is confirmed against those routes. A signed public APK Release is
also unavailable until the signing secrets are configured.
