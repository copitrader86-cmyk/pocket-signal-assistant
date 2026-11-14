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

### Prepare a free Render deployment

The backend includes a root `render.yaml` Blueprint and `backend/Dockerfile`.
After connecting this repository to a Render account, the Blueprint can create
a free web service with `/health` as its health check. Render assigns the public
HTTPS hostname after deployment; do not put a guessed hostname in the Android
build.

This free configuration is only suitable for a temporary connectivity check:
Render free services sleep after 15 minutes without inbound traffic and their
local filesystem is ephemeral. The SQLite database at `/tmp/pocket-signal` will
not persist across restarts or spin-downs. The backend also has no API
authentication, and no market-data provider is configured, so it must not be
used for real trading or exposed with user trading history. A persistent
deployment requires an authenticated API and durable database; these are not
provided by this free SQLite setup.

Set `DATABASE_PATH` to choose a persistent SQLite database location. Check
`GET /health` and `GET /api/v1/catalog`; the interactive API documentation is
available at `/docs`.

Run backend tests from `backend/`:

```sh
pytest
```

## Android app

The Android client uses Kotlin/Jetpack Compose and connects to the API configured
at build time. The default URL (`http://10.0.2.2:8000`) is for the Android
emulator reaching a backend on the same host. For a deployed private HTTPS API,
pass `-PapiBaseUrl=https://your-api-host`.

Build a debug APK with an installed Android SDK and JDK 17+:

```sh
./gradlew assembleDebug
```

The APK is written to `app/build/outputs/apk/debug/app-debug.apk`. The current
client includes instrument/market/expiry selection, analysis, live tick chart,
session controls, results, history, and statistics. OTC selection remains
disabled by the server's data-source policy unless a real authorized OTC feed
is implemented.

## Market data and API

The existing Twelve Data adapter is optional and requires a provider key. No
key or paid subscription is required from the user; without a configured key,
the adapter stays unavailable and the app must not produce directional signals.
If an operator has separately licensed access, refresh the provider's Forex
catalog with `POST /api/v1/provider/twelvedata/catalog`, then request up to
5,000 provider historical candles with
`POST /api/v1/history/twelvedata?symbol=EUR%2FUSD&interval_seconds=60`.
Twelve Data's WebSocket price endpoint is relayed at
`/api/v1/stream?symbol=EUR%2FUSD`. It records provider ticks and forms 1-second
and 1-minute live candles. Provider plan limits, permitted instruments, and
latency still apply; the app never labels these prices as Pocket Option quotes.

Import licensed historical CSV data at `POST /api/v1/history/import` using the
columns `timestamp,open,high,low,close` and optional `volume`. Timestamps should
be ISO-8601; naive timestamps are interpreted as UTC. Invalid rows are reported
and skipped; duplicate timestamps are counted; missing intervals are reported,
not interpolated. Historical and live candles are stored and queried separately.

`POST /api/v1/analyze` only emits a directional signal when the current tick is
fresh, the observed stream rate is adequate, the technical candidate is strong,
and an active model has passed temporal validation. Otherwise it records and
returns `NO_SIGNAL`, with no invented probabilities. The current backtest
(`GET /api/v1/backtest`) uses a chronological holdout on 1-minute candles and
explicitly rejects sub-minute expirations. It does not validate seconds.

Session, result, and statistics APIs are `POST/GET /api/v1/sessions`,
`POST /api/v1/sessions/{id}/finish`, `GET /api/v1/signals`,
`PATCH /api/v1/signals/{id}/outcome`, and `GET /api/v1/statistics`.
`POST /api/v1/models/train` only considers completed WON/LOST directional trades,
requires at least 150 labeled outcomes, fits on the oldest 60%, calibrates on
the next 20%, and validates on the newest 20%. It requires 25 additional
outcomes before retraining an active model and does not activate a candidate
that fails the holdout or same-holdout comparison.

This backend currently has no authentication or TLS termination; run it only
behind a trusted private network/reverse proxy. Never expose the development
server directly to the public internet.

A successful `assembleDebug` output is a debug build, not a distributable
release. Rebuild it after source changes. Live-provider checks, device
installation, release signing, and a production security review remain
unverified for the deployment environment.

## Data-source plan

- Frankfurter's no-key API was reachable during verification and provides
  official-source reference exchange rates and daily history, not intraday
  OHLC or executable/live quotes. It is not suitable for these short expirations
  and is not used to create candles or signals.
- A no-key Dukascopy hourly tick archive returned binary data in one probe, but
  a repeat request timed out. Its binary ingestion, coverage, license terms, and
  ongoing availability have not been validated; it is not wired into the app.
- The existing Twelve Data adapter supports catalog/history/WebSocket only when
  separately configured with a key. No key is requested from the user, and no
  live quote provider without registration has been verified for production use.
- OTC live data: unavailable until an authorized Pocket Option-compatible source
  and instrument catalog are confirmed.

Historical prices are for research and backtesting, not live trading. Second-based
expirations will only be analyzed when the live tick rate and timestamps support
them; minute candles are not a substitute.

## Release automation and deployment blockers

The Android release workflow runs backend tests, Android lint/unit-test tasks,
builds a signed APK, stores it as a workflow artifact, and attaches it to a
tagged GitHub Release. It intentionally fails until the repository has a
`SERVER_BASE_URL` variable containing the deployed HTTPS API and these Actions
secrets: `RELEASE_KEYSTORE_BASE64`, `RELEASE_STORE_PASSWORD`,
`RELEASE_KEY_ALIAS`, and `RELEASE_KEY_PASSWORD`. The keystore is never stored
in the repository.

There is no deployed API address configured in this repository. A free Render
web service is not suitable as a permanent live-quote server: its free service
spins down after 15 minutes without inbound traffic and loses local files on
restart/spin-down; its free PostgreSQL database expires after 30 days. See
[Render free-instance limits](https://render.com/docs/free). Creating a real
internet deployment also requires authorization to an external hosting account
and a persistent database choice. Until those prerequisites exist, a Release
APK cannot be truthfully configured to auto-connect, and no downloadable public
APK Release is available.
