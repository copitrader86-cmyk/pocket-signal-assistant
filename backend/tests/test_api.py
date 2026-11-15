import sqlite3
import asyncio
import json
import time
from pathlib import Path

import pytest
import httpx
from starlette.websockets import WebSocketDisconnect
from fastapi.testclient import TestClient

from app.database import connect, initialize_database
from app.historical_import import import_historical_csv
from app.learning import MINIMUM_LABELED_SIGNALS, predict_win_probability, train_model
from app.main import _store_live_tick, create_app
from app.analytics import analyze_candles
from app.backtesting import BacktestError, run_backtest
from app.market_data import Candle, MarketDataError, TwelveDataClient


def test_health_initializes_database(tmp_path: Path) -> None:
    database_path = tmp_path / "nested" / "signals.db"

    with TestClient(create_app(database_path)) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert isinstance(response.json()["server_time_ms"], int)
    assert database_path.exists()


def test_catalog_does_not_claim_unconfigured_markets(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.get("/api/v1/catalog")

    assert response.status_code == 200
    assert response.json() == {
        "instruments": [],
        "classic_available": False,
        "otc_available": False,
    }


def test_catalog_and_history_status_work_without_server_twelvedata_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO candles (
                provider, market_type, symbol, interval_seconds, timestamp_ms,
                open, high, low, close, data_kind, source_resolution_ms
            ) VALUES ('HistData', 'CLASSIC', 'EUR/USD', 60, 1700000000000,
                1.1, 1.2, 1.0, 1.15, 'HISTORICAL', 60000)
            """
        )

    with TestClient(create_app(database_path)) as client:
        catalog = client.get("/api/v1/catalog")
        status = client.get("/api/v1/sources/status")

    assert catalog.status_code == 200
    assert catalog.json()["instruments"] == [
        {
            "provider": "HistData",
            "market_type": "CLASSIC",
            "symbol": "EUR/USD",
            "display_name": "EUR/USD",
        }
    ]
    assert status.status_code == 200
    assert status.json()["historical"]["histdata_candles"] == 1
    assert status.json()["online"]["credentials_configured"] is False


def test_database_schema_rejects_unsupported_expiration(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)

    with connect(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            connection.execute(
                """
                INSERT INTO signals (
                    id, provider, market_type, symbol, direction, expiry_seconds,
                    issued_at_ms, expires_at_ms, quote_timestamp_ms, quote_latency_ms,
                    market_snapshot_json, analytics_json, explanation
                ) VALUES (
                    's1', 'provider', 'CLASSIC', 'EUR/USD', 'UP', 7,
                    1000, 8000, 1000, 0, '{}', '{}', 'test'
                )
                """
            )


def test_schema_upgrade_preserves_signals(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO signals (
                id, provider, market_type, symbol, direction, expiry_seconds,
                issued_at_ms, expires_at_ms, market_snapshot_json, analytics_json,
                explanation
            ) VALUES ('keep-me', 'TwelveData', 'CLASSIC', 'EUR/USD', 'NO_SIGNAL',
                60, 1000, 61000, '{}', '{}', 'missing quote')
            """
        )
        connection.execute("DROP TABLE market_ticks")
        connection.execute("DROP TABLE reference_rates")
        connection.execute("DROP TABLE public_history_sync")
        connection.execute("DROP INDEX candles_historical_lookup")
        connection.execute("DROP INDEX candles_history_coverage")
        connection.execute("ALTER TABLE candles DROP COLUMN source_resolution_ms")
        connection.execute("PRAGMA user_version = 2")

    initialize_database(database_path)

    with connect(database_path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        signal = connection.execute(
            "SELECT id, quote_timestamp_ms FROM signals WHERE id = 'keep-me'"
        ).fetchone()

    assert version == 13
    assert tuple(signal) == ("keep-me", None)


def test_historical_csv_import_reports_gaps_and_skips_bad_rows(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    csv_text = "\n".join(
        [
            "timestamp,open,high,low,close,volume",
            "2025-01-01T00:00:00Z,1.1,1.2,1.0,1.15,10",
            "2025-01-01T00:00:00Z,1.1,1.2,1.0,1.15,10",
            "2025-01-01T00:03:00Z,1.15,1.25,1.1,1.2,",
            "bad-timestamp,1,1,1,1,",
            "2025-01-01T00:04:00Z,1,0.9,1,1,",
        ]
    )

    with connect(database_path) as connection:
        report = import_historical_csv(
            connection,
            csv_text,
            provider="Dukascopy",
            symbol="EUR/USD",
            interval_seconds=60,
        )
        repeated = import_historical_csv(
            connection,
            csv_text,
            provider="Dukascopy",
            symbol="EUR/USD",
            interval_seconds=60,
        )
        count = connection.execute("SELECT COUNT(*) FROM candles").fetchone()[0]

    assert report.rows_read == 5
    assert report.rows_imported == 2
    assert report.invalid_rows == 2
    assert report.duplicate_rows == 1
    assert report.missing_intervals == 2
    assert repeated.rows_imported == 0
    assert count == 2


def test_twelvedata_candle_response_is_normalized_and_sorted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/time_series")
        return httpx.Response(
            200,
            json={
                "meta": {"symbol": "EUR/USD"},
                "values": [
                    {
                        "datetime": "2025-01-01 00:01:00",
                        "open": "1.1",
                        "high": "1.2",
                        "low": "1.0",
                        "close": "1.15",
                        "volume": "",
                    },
                    {
                        "datetime": "2025-01-01 00:00:00",
                        "open": "1.0",
                        "high": "1.1",
                        "low": "0.9",
                        "close": "1.05",
                        "volume": "",
                    },
                ],
            },
        )

    async def fetch() -> list[Candle]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            async with TwelveDataClient("test-key", client=client) as provider:
                return await provider.time_series("EUR/USD", 60, 10)

    candles = asyncio.run(fetch())

    assert len(candles) == 2
    assert candles[0].timestamp_ms < candles[1].timestamp_ms
    assert candles[0].close == 1.05
    assert candles[0].volume is None


def test_twelvedata_missing_key_fails_explicitly() -> None:
    async def fetch() -> None:
        async with TwelveDataClient(api_key="") as provider:
            await provider.forex_pairs()

    with pytest.raises(MarketDataError, match="not configured"):
        asyncio.run(fetch())


def test_twelvedata_forex_pairs_are_loaded_from_provider_catalog() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/forex_pairs")
        assert request.url.params["apikey"] == "test-key"
        return httpx.Response(
            200,
            json={"data": [{"symbol": "EUR/USD"}, {"symbol": "GBP/USD"}]},
        )

    async def fetch() -> list[dict[str, str]]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            async with TwelveDataClient("test-key", client=client) as provider:
                return await provider.forex_pairs()

    instruments = asyncio.run(fetch())

    assert [instrument["symbol"] for instrument in instruments] == [
        "EUR/USD",
        "GBP/USD",
    ]
    assert all(instrument["market_type"] == "CLASSIC" for instrument in instruments)


def test_twelvedata_rate_limit_is_reported_without_provider_details(
    tmp_path: Path,
) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "error",
                "code": 429,
                "message": "API credits exhausted",
            },
        )

    async def fetch() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            async with TwelveDataClient("test-key", client=client) as provider:
                await provider.forex_pairs()

    with pytest.raises(MarketDataError, match="rate limit") as error:
        asyncio.run(fetch())
    assert error.value.status_code == 429
    assert "test-key" not in str(error.value)


def test_twelvedata_catalog_route_preserves_provider_rate_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class RateLimitedTwelveDataClient:
        async def __aenter__(self) -> "RateLimitedTwelveDataClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def forex_pairs(self) -> list[dict[str, str]]:
            raise MarketDataError("Twelve Data rate limit exceeded", status_code=429)

    monkeypatch.setattr("app.main.TwelveDataClient", RateLimitedTwelveDataClient)
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.post("/api/v1/provider/twelvedata/catalog")

    assert response.status_code == 429
    assert response.json()["detail"] == "Twelve Data rate limit exceeded"


def test_twelvedata_status_reports_missing_key_without_exposing_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.get("/api/v1/sources/status")

    assert response.status_code == 200
    online = response.json()["online"]
    assert online["credentials_configured"] is False
    assert online["connection_status"] == "not_configured"
    assert "api_key" not in online


def test_twelvedata_status_reports_configured_key_without_returning_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "local-test-secret"
    monkeypatch.setenv("TWELVE_DATA_API_KEY", secret)

    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.get("/api/v1/sources/status")

    assert response.status_code == 200
    online = response.json()["online"]
    assert online["credentials_configured"] is True
    assert online["connection_status"] == "configured_no_recent_quote"
    assert secret not in response.text


def test_twelvedata_status_accepts_device_key_without_returning_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "device-only-test-secret"
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.get(
            "/api/v1/sources/status",
            headers={"X-Twelve-Data-Api-Key": secret},
        )

    assert response.status_code == 200
    assert response.json()["online"]["credentials_configured"] is True
    assert secret not in response.text


def test_twelvedata_websocket_stream_persists_provider_quotes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO market_instruments (
                provider, market_type, symbol, display_name, updated_at_ms
            ) VALUES ('TwelveData', 'CLASSIC', 'EUR/USD', 'EUR/USD', 1)
            """
        )
    api_key = "websocket-test-secret"
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)

    class FakeUpstream:
        def __init__(self) -> None:
            self.sent_tick = False

        async def __aenter__(self) -> "FakeUpstream":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def send(self, message: str) -> None:
            assert json.loads(message) == {
                "action": "subscribe",
                "params": {"symbols": "EUR/USD"},
            }

        async def recv(self) -> str:
            if not self.sent_tick:
                self.sent_tick = True
                return json.dumps(
                    {
                        "event": "price",
                        "symbol": "EUR/USD",
                        "price": "1.1",
                        "timestamp": 1735689600,
                    }
                )
            raise WebSocketDisconnect(code=1000)

    def fake_connect(url: str, **kwargs: object) -> FakeUpstream:
        assert url == "wss://ws.twelvedata.com/v1/quotes/price"
        assert kwargs["additional_headers"] == {"X-TD-APIKEY": api_key}
        return FakeUpstream()

    monkeypatch.setattr("app.main.websockets.connect", fake_connect)
    with TestClient(create_app(database_path)) as client:
        with client.websocket_connect(
            "/api/v1/stream?symbol=EUR%2FUSD",
            headers={"X-Twelve-Data-Api-Key": api_key},
        ) as socket:
            tick = socket.receive_json()
        candles = client.get(
            "/api/v1/candles",
            params={
                "provider": "TwelveData",
                "symbol": "EUR/USD",
                "interval_seconds": 1,
                "data_kind": "LIVE",
            },
        )

    assert tick["event"] == "price"
    assert candles.status_code == 200
    assert candles.json()["candles"][0]["close"] == 1.1


def test_twelvedata_catalog_refresh_saves_provider_instruments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeTwelveDataClient:
        def __init__(self, api_key: str | None = None) -> None:
            assert api_key is None

        async def __aenter__(self) -> "FakeTwelveDataClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def forex_pairs(self) -> list[dict[str, str]]:
            return [
                {
                    "provider": "TwelveData",
                    "market_type": "CLASSIC",
                    "symbol": "EUR/USD",
                    "display_name": "EUR/USD",
                }
            ]

    monkeypatch.setattr("app.main.TwelveDataClient", FakeTwelveDataClient)
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        refreshed = client.post("/api/v1/provider/twelvedata/catalog")
        catalog = client.get("/api/v1/catalog")

    assert refreshed.status_code == 200
    assert refreshed.json()["instruments_updated"] == 1
    assert catalog.json()["instruments"] == [
        {
            "provider": "TwelveData",
            "market_type": "CLASSIC",
            "symbol": "EUR/USD",
            "display_name": "EUR/USD",
        }
    ]


def test_twelvedata_connection_check_uses_device_key_and_refreshes_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "device-only-test-secret"

    class FakeTwelveDataClient:
        def __init__(self, api_key: str | None = None) -> None:
            assert api_key == secret

        async def __aenter__(self) -> "FakeTwelveDataClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def forex_pairs(self) -> list[dict[str, str]]:
            return [
                {
                    "provider": "TwelveData",
                    "market_type": "CLASSIC",
                    "symbol": "EUR/USD",
                    "display_name": "EUR/USD",
                }
            ]

    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    monkeypatch.setattr("app.main.TwelveDataClient", FakeTwelveDataClient)
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.post(
            "/api/v1/provider/twelvedata/check",
            headers={"X-Twelve-Data-Api-Key": secret},
        )

    assert response.status_code == 200
    assert response.json() == {
        "provider": "TwelveData",
        "status": "connected",
        "instruments_updated": 1,
    }
    assert secret not in response.text


def test_twelvedata_history_api_persists_provider_candles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "history-test-secret"

    class FakeTwelveDataClient:
        def __init__(self, api_key: str | None = None) -> None:
            assert api_key == secret

        async def __aenter__(self) -> "FakeTwelveDataClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def time_series(
            self, symbol: str, interval_seconds: int, outputsize: int
        ) -> list[Candle]:
            assert (symbol, interval_seconds, outputsize) == ("EUR/USD", 60, 1)
            return [
                Candle(
                    timestamp_ms=1_735_689_600_000,
                    open=1.1,
                    high=1.2,
                    low=1.0,
                    close=1.15,
                )
            ]

    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    monkeypatch.setattr("app.main.TwelveDataClient", FakeTwelveDataClient)
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.post(
            "/api/v1/history/twelvedata",
            params={"symbol": "EUR/USD", "interval_seconds": 60, "outputsize": 1},
            headers={"X-Twelve-Data-Api-Key": secret},
        )

    assert response.status_code == 200
    assert response.json()["rows_received"] == 1
    assert response.json()["rows_imported"] == 1


def test_live_ticks_update_live_candles_without_touching_history(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        _store_live_tick(
            connection,
            "EUR/USD",
            {"timestamp": 1_735_689_600, "price": "1.1"},
        )
        _store_live_tick(
            connection,
            "EUR/USD",
            {"timestamp": 1_735_689_600.5, "price": "1.2"},
        )
        live = connection.execute(
            """
            SELECT open, high, low, close
            FROM candles
            WHERE provider = 'TwelveData' AND symbol = 'EUR/USD'
                AND interval_seconds = 1 AND data_kind = 'LIVE'
            """
        ).fetchone()
        historical_count = connection.execute(
            "SELECT COUNT(*) FROM candles WHERE data_kind = 'HISTORICAL'"
        ).fetchone()[0]

    assert tuple(live) == (1.1, 1.2, 1.1, 1.2)
    assert historical_count == 0


def test_history_api_keeps_live_and_historical_series_separate(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO candles (
                provider, market_type, symbol, interval_seconds, timestamp_ms,
                open, high, low, close, data_kind
            ) VALUES ('Dukascopy', 'CLASSIC', 'EUR/USD', 60, 1000, 1, 2, 0.5, 1.5, 'HISTORICAL')
            """
        )

    with TestClient(create_app(database_path)) as client:
        historical = client.get(
            "/api/v1/candles",
            params={
                "provider": "Dukascopy",
                "symbol": "EUR/USD",
                "interval_seconds": 60,
                "data_kind": "HISTORICAL",
            },
        )
        live = client.get(
            "/api/v1/candles",
            params={
                "provider": "Dukascopy",
                "symbol": "EUR/USD",
                "interval_seconds": 60,
                "data_kind": "LIVE",
            },
        )

    assert len(historical.json()["candles"]) == 1
    assert live.json()["candles"] == []


def test_analysis_with_missing_quote_is_saved_as_no_signal_without_fake_probability(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO market_instruments (
                provider, market_type, symbol, display_name, updated_at_ms
            ) VALUES ('TwelveData', 'CLASSIC', 'EUR/USD', 'EUR/USD', 1)
            """
        )

    with TestClient(create_app(database_path)) as client:
        response = client.post(
            "/api/v1/analyze",
            json={
                "symbol": "EUR/USD",
                "market_type": "CLASSIC",
                "expiry_seconds": 3,
            },
        )
        signal_id = response.json()["id"]
        details = client.get("/api/v1/signals").json()[0]
        outcome = client.patch(
            f"/api/v1/signals/{signal_id}/outcome",
            json={"outcome": "WON"},
        )

    assert response.status_code == 200
    assert response.json()["direction"] == "NO_SIGNAL"
    assert response.json()["probability_up"] is None
    assert response.json()["quote_timestamp_ms"] is None
    assert details["quote_timestamp_ms"] is None
    assert outcome.status_code == 409


def test_otc_analysis_is_rejected_instead_of_using_fx_quotes(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.post(
            "/api/v1/analyze",
            json={
                "symbol": "EUR/USD",
                "market_type": "OTC",
                "expiry_seconds": 30,
            },
        )

    assert response.status_code == 409
    assert "not OTC" in response.json()["detail"]


def test_session_and_statistics_track_wins_losses_and_missed_signals(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            "INSERT INTO trading_sessions (id, started_at_ms) VALUES ('session-1', 1)"
        )
        for signal_id, outcome in (
            ("signal-won", "WON"),
            ("signal-lost", "LOST"),
            ("signal-draw", "DRAW"),
            ("signal-missed", "MISSED"),
            ("signal-open", "PENDING"),
        ):
            connection.execute(
                """
                INSERT INTO signals (
                    id, session_id, provider, market_type, symbol, direction,
                    expiry_seconds, issued_at_ms, expires_at_ms, market_snapshot_json,
                    analytics_json, explanation, outcome
                ) VALUES (?, 'session-1', 'TwelveData', 'CLASSIC', 'EUR/USD',
                    'UP', 60, ?, ?, '{}', '{}', 'test', ?)
                """,
                (signal_id, int(time.time() * 1000), int(time.time() * 1000) + 60_000, outcome),
            )

    with TestClient(create_app(database_path)) as client:
        summary = client.get(
            "/api/v1/statistics",
            params={"period": "all", "session_id": "session-1"},
        ).json()
        history = client.get(
            "/api/v1/signals",
            params={"session_id": "session-1", "limit": 10},
        ).json()

    assert summary["total_signals"] == 5
    assert summary["wins"] == 1
    assert summary["losses"] == 1
    assert summary["draws"] == 1
    assert summary["missed"] == 1
    assert summary["pending"] == 1
    assert summary["win_rate_percent"] == 50
    assert summary["by_pair"]["EUR/USD"]["total"] == 5
    assert summary["by_pair"]["EUR/USD"]["draw"] == 1
    assert len(history) == 5


def test_technical_analysis_is_deterministic_and_requires_warmup() -> None:
    short_history = [
        Candle(timestamp_ms=index * 1000, open=1, high=1.1, low=0.9, close=1)
        for index in range(20)
    ]
    trend = [
        Candle(
            timestamp_ms=index * 1000,
            open=1 + index * 0.001,
            high=1.001 + index * 0.001,
            low=0.999 + index * 0.001,
            close=1.001 + index * 0.001,
        )
        for index in range(80)
    ]

    no_warmup = analyze_candles(short_history)
    first = analyze_candles(trend)
    second = analyze_candles(trend)

    assert no_warmup.candidate_direction == "NO_SIGNAL"
    assert first.candidate_direction == "UP"
    assert first.features == second.features
    assert first.score == second.score


def test_backtest_uses_temporal_holdout_and_rejects_seconds_on_minute_bars() -> None:
    candles = [
        Candle(
            timestamp_ms=index * 60_000,
            open=1 + index * 0.001,
            high=1.001 + index * 0.001,
            low=0.999 + index * 0.001,
            close=1.001 + index * 0.001,
        )
        for index in range(120)
    ]

    result = run_backtest(
        candles,
        provider="Dukascopy",
        symbol="EUR/USD",
        candle_interval_seconds=60,
        expiry_seconds=60,
    )

    assert result.sample_count == 24
    assert result.trade_count == 24
    assert result.wins == 24
    assert result.accuracy_percent == 100
    assert result.out_of_sample_start_ms == 96 * 60_000
    with pytest.raises(BacktestError, match="second-based expirations"):
        run_backtest(
            candles,
            provider="Dukascopy",
            symbol="EUR/USD",
            candle_interval_seconds=60,
            expiry_seconds=3,
        )


def test_learning_requires_minimum_labels_and_uses_chronological_holdouts() -> None:
    records = [
        {
            "issued_at_ms": index * 1000,
            "direction": "UP",
            "expiry_seconds": 60,
            "outcome": "WON" if index % 2 == 0 else "LOST",
            "analytics_json": json.dumps(
                {
                    "score": 2 if index % 2 == 0 else -2,
                    "rsi_14": 65 if index % 2 == 0 else 35,
                    "macd_histogram": 0.01 if index % 2 == 0 else -0.01,
                    "momentum_5": 0.001 if index % 2 == 0 else -0.001,
                    "volatility_20": 0.001,
                    "buyer_seller_pressure_20": 0.5 if index % 2 == 0 else -0.5,
                }
            ),
            "market_snapshot_json": json.dumps({"latest_price": 1}),
        }
        for index in range(MINIMUM_LABELED_SIGNALS)
    ]

    insufficient = train_model(records[: MINIMUM_LABELED_SIGNALS - 1])
    trained = train_model(records)

    assert insufficient.eligible is False
    assert trained.validation_samples == 30
    assert trained.eligible is True
    assert trained.artifact is not None
    assert trained.artifact["calibration"]
    assert trained.metrics["brier_score"] < trained.metrics["baseline_brier_score"]
    probability = predict_win_probability(
        trained.artifact,
        json.loads(records[-2]["analytics_json"]),
        {"latest_price": 1},
        "UP",
        60,
    )
    assert probability > 0.5
