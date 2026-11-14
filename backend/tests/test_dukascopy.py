import asyncio
import lzma
import struct
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.database import connect, initialize_database
from app.dukascopy import (
    HOUR_MS,
    INSTRUMENTS,
    DukascopyClient,
    HistoricalBatch,
    Tick,
    parse_bi5,
)
from app.main import create_app
from app.historical_import import save_historical_candles
from app.market_data import Candle, MarketDataError


def _bi5(records: list[tuple[int, int, int, float, float]]) -> bytes:
    raw = b"".join(struct.pack(">IIIff", *record) for record in records)
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def test_supported_instruments_and_bi5_prices_are_decoded() -> None:
    assert tuple(INSTRUMENTS) == (
        "EUR/USD",
        "GBP/USD",
        "USD/JPY",
        "USD/CHF",
        "USD/CAD",
        "AUD/USD",
        "NZD/USD",
    )
    hour_start = 1_700_000_000_000
    ticks = parse_bi5(
        _bi5(
            [
                (0, 110_010, 110_000, 1.0, 2.0),
                (500, 110_030, 110_020, 1.0, 2.0),
            ]
        ),
        hour_start_ms=hour_start,
        price_scale=100_000,
    )

    assert ticks == [
        Tick(hour_start, 1.1, 1.1001),
        Tick(hour_start + 500, 1.1002, 1.1003),
    ]


def test_bi5_uses_jpy_scale_and_rejects_bad_files() -> None:
    ticks = parse_bi5(
        _bi5([(0, 150_010, 150_000, 1.0, 1.0)]),
        hour_start_ms=0,
        price_scale=1_000,
    )
    assert ticks[0].bid == 150
    assert ticks[0].ask == 150.01

    valid_payload = _bi5([(0, 110_010, 110_000, 1.0, 1.0)])
    for payload in (
        b"",
        b"not compressed",
        valid_payload[:-1],
        _bi5([(HOUR_MS, 110_010, 110_000, 1.0, 1.0)]),
    ):
        with pytest.raises(MarketDataError):
            parse_bi5(payload, hour_start_ms=0, price_scale=100_000)


def test_bi5_rejects_unordered_ticks_and_invalid_quotes() -> None:
    cases = (
        [(500, 110_010, 110_000, 1.0, 1.0), (400, 110_010, 110_000, 1.0, 1.0)],
        [(0, 110_000, 110_010, 1.0, 1.0)],
        [(0, 110_010, 110_000, float("nan"), 1.0)],
    )
    for records in cases:
        with pytest.raises(MarketDataError):
            parse_bi5(
                _bi5(records),
                hour_start_ms=0,
                price_scale=100_000,
            )

    with pytest.raises(MarketDataError, match="record length"):
        parse_bi5(
            lzma.compress(b"partial", format=lzma.FORMAT_ALONE),
            hour_start_ms=0,
            price_scale=100_000,
        )


def test_dukascopy_fetch_uses_utc_path_and_reports_missing_files() -> None:
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        if request.url.path.endswith("/1h_ticks.bi5"):
            return httpx.Response(404)
        return httpx.Response(
            200,
            content=_bi5(
                [
                    (0, 110_010, 110_000, 1.0, 1.0),
                    (30_000, 110_030, 110_020, 1.0, 1.0),
                    (120_000, 110_050, 110_040, 1.0, 1.0),
                ]
            ),
        )

    async def fetch() -> tuple[list[Tick] | None, object]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            async with DukascopyClient(client=http) as provider:
                missing = await provider.fetch_hour("EUR/USD", date(2025, 1, 2), 1)
                batch = await provider.fetch_candles(
                    "EUR/USD",
                    date(2025, 1, 2),
                    start_hour=0,
                    end_hour=2,
                    interval_seconds=60,
                )
                return missing, batch

    missing, batch = asyncio.run(fetch())

    assert missing is None
    assert batch.files_requested == 2
    assert batch.files_missing == 1
    assert batch.ticks_received == 3
    assert batch.missing_intervals == 1
    assert len(batch.candles) == 2
    assert batch.candles[0].open == 1.1
    assert batch.candles[0].close == 1.1002
    assert batch.candles[1].timestamp_ms == batch.candles[0].timestamp_ms + 120_000
    assert batch.candles[1].close == 1.1004
    assert "/2025/00/02/1h_ticks.bi5" in requested_urls[0]


def test_dukascopy_file_parsing_keeps_event_loop_responsive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = [0]
    ticks_during_parse: list[int] = []
    original_parser = parse_bi5

    def slow_parser(
        payload: bytes, *, hour_start_ms: int, price_scale: int
    ) -> list[Tick]:
        ticks_before_parse = ticks[0]
        time.sleep(0.1)
        ticks_during_parse.append(ticks[0] - ticks_before_parse)
        return original_parser(
            payload, hour_start_ms=hour_start_ms, price_scale=price_scale
        )

    monkeypatch.setattr("app.dukascopy.parse_bi5", slow_parser)

    async def exercise() -> None:
        async def fetch() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200,
                        content=_bi5([(0, 110_010, 110_000, 1.0, 1.0)]),
                    )
                )
            ) as http:
                async with DukascopyClient(client=http) as provider:
                    await provider.fetch_hour("EUR/USD", date(2025, 1, 2), 0)

        async def ticker() -> None:
            while not ticks_during_parse:
                ticks[0] += 1
                await asyncio.sleep(0.005)

        await asyncio.gather(fetch(), ticker())

    asyncio.run(exercise())

    assert ticks_during_parse[0] > 0


def test_dukascopy_retries_429_then_decodes_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def no_wait(delay: float) -> None:
        delays.append(delay)

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(
            200,
            content=_bi5([(0, 110_010, 110_000, 1.0, 1.0)]),
        )

    monkeypatch.setattr("app.dukascopy.asyncio.sleep", no_wait)

    async def fetch() -> list[Tick] | None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http:
            async with DukascopyClient(client=http) as provider:
                return await provider.fetch_hour("EUR/USD", date(2025, 1, 2), 0)

    ticks = asyncio.run(fetch())

    assert attempts == 2
    assert delays == [1.0]
    assert ticks is not None
    assert len(ticks) == 1


def test_dukascopy_stops_after_bounded_429_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def no_wait(delay: float) -> None:
        delays.append(delay)

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(429, text="rate limited")

    monkeypatch.setattr("app.dukascopy.asyncio.sleep", no_wait)

    async def fetch() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http:
            async with DukascopyClient(client=http) as provider:
                await provider.fetch_hour("EUR/USD", date(2025, 1, 2), 0)

    with pytest.raises(MarketDataError, match="after three attempts"):
        asyncio.run(fetch())
    assert attempts == 3
    assert delays == [1.0, 2.0]


def test_dukascopy_honors_long_retry_after_without_early_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def no_wait(delay: float) -> None:
        delays.append(delay)

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(429, headers={"Retry-After": "31"})

    monkeypatch.setattr("app.dukascopy.asyncio.sleep", no_wait)

    async def fetch() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http:
            async with DukascopyClient(client=http) as provider:
                await provider.fetch_hour("EUR/USD", date(2025, 1, 2), 0)

    with pytest.raises(MarketDataError, match="over 30 seconds"):
        asyncio.run(fetch())
    assert attempts == 1
    assert delays == []


def test_dukascopy_limits_imports_to_three_hours_per_request() -> None:
    async def fetch() -> None:
        async with DukascopyClient() as provider:
            await provider.fetch_candles(
                "EUR/USD",
                date(2025, 1, 2),
                start_hour=0,
                end_hour=4,
                interval_seconds=60,
            )

    with pytest.raises(ValueError, match="at most 3 UTC hours"):
        asyncio.run(fetch())


def test_dukascopy_import_persists_history_and_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    batch = HistoricalBatch(
        symbol="EUR/USD",
        date=date(2025, 1, 2),
        start_hour=0,
        end_hour=1,
        files_requested=1,
        files_missing=0,
        ticks_received=2,
        missing_intervals=0,
        candles=(
            Candle(
                timestamp_ms=1_735_776_000_000,
                open=1.1,
                high=1.11,
                low=1.09,
                close=1.105,
            ),
        ),
    )

    class FakeDukascopyClient:
        async def __aenter__(self) -> "FakeDukascopyClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def fetch_candles(self, *_: object, **__: object) -> HistoricalBatch:
            return batch

    monkeypatch.setattr("app.main.DukascopyClient", FakeDukascopyClient)
    database_path = tmp_path / "signals.db"
    with TestClient(create_app(database_path)) as client:
        instruments = client.get("/api/v1/history/dukascopy/instruments")
        imported = client.post(
            "/api/v1/history/dukascopy/import",
            params={
                "symbol": "EUR/USD",
                "date": "2025-01-02",
                "start_hour": 0,
                "end_hour": 1,
            },
        )
        catalog = client.get("/api/v1/catalog")
        coverage = client.get("/api/v1/history/dukascopy/instruments")
        history = client.get(
            "/api/v1/candles",
            params={
                "provider": "Dukascopy",
                "symbol": "EUR/USD",
                "interval_seconds": 60,
                "data_kind": "HISTORICAL",
            },
        )

    assert instruments.status_code == 200
    assert len(instruments.json()["instruments"]) == 7
    assert imported.status_code == 200
    assert imported.json()["candles_imported"] == 1
    assert imported.json()["ticks_received"] == 2
    eurusd = next(
        item
        for item in coverage.json()["instruments"]
        if item["symbol"] == "EUR/USD"
    )
    assert eurusd["history_available"] is True
    assert eurusd["candles_count"] == 1
    assert catalog.json()["classic_available"] is True
    assert {
        (item["provider"], item["symbol"])
        for item in catalog.json()["instruments"]
    } == {("Dukascopy", "EUR/USD")}
    assert history.json()["candles"][0]["close"] == 1.105


def test_dukascopy_import_read_and_backtest_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    candles = tuple(
        Candle(
            timestamp_ms=now_ms - (120 - index) * 60_000,
            open=1 + index * 0.001,
            high=1.001 + index * 0.001,
            low=0.999 + index * 0.001,
            close=1.001 + index * 0.001,
        )
        for index in range(120)
    )
    trading_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    batch = HistoricalBatch(
        symbol="EUR/USD",
        date=trading_date,
        start_hour=0,
        end_hour=3,
        files_requested=3,
        files_missing=0,
        ticks_received=120,
        missing_intervals=0,
        candles=candles,
        ticks=(
            Tick(now_ms - 60_000, 1.1, 1.1001),
            Tick(now_ms - 59_999, 1.1001, 1.1002),
        ),
    )

    class FakeDukascopyClient:
        async def __aenter__(self) -> "FakeDukascopyClient":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def fetch_candles(self, *_: object, **__: object) -> HistoricalBatch:
            return batch

    monkeypatch.setattr("app.main.DukascopyClient", FakeDukascopyClient)
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        imported = client.post(
            "/api/v1/history/dukascopy/import",
            params={
                "symbol": "EUR/USD",
                "date": trading_date.isoformat(),
                "start_hour": 0,
                "end_hour": 3,
            },
        )
        history = client.get(
            "/api/v1/candles",
            params={
                "provider": "Dukascopy",
                "symbol": "EUR/USD",
                "interval_seconds": 60,
                "data_kind": "HISTORICAL",
                "limit": 500,
            },
        )
        report = client.get(
            "/api/v1/backtest/dukascopy",
            params=[
                ("symbol", "EUR/USD"),
                ("period_days", 2),
                ("expiry_seconds", 60),
                ("expiry_seconds", 300),
            ],
        )
        ticks = client.get(
            "/api/v1/history/dukascopy/ticks",
            params={"symbol": "EUR/USD", "limit": 10},
        )
        coverage = client.get("/api/v1/history/dukascopy/instruments")

    assert imported.status_code == 200
    assert imported.json()["candles_imported"] == 120
    assert imported.json()["ticks_imported"] == 2
    assert history.status_code == 200
    assert len(history.json()["candles"]) == 120
    assert report.status_code == 200
    assert report.json()["candle_count"] == 120
    assert [item["expiry_seconds"] for item in report.json()["results"]] == [60, 300]
    assert all(item["status"] == "ok" for item in report.json()["results"])
    assert ticks.status_code == 200
    assert len(ticks.json()["ticks"]) == 2
    eurusd = next(
        item for item in coverage.json()["instruments"] if item["symbol"] == "EUR/USD"
    )
    assert eurusd["ticks_count"] == 2
    assert eurusd["available_intervals_seconds"] == [60]
    assert eurusd["available_expirations_seconds"] == [60, 120, 180, 240, 300, 600, 900]


def test_dukascopy_import_rejects_future_hours_and_unknown_pairs(tmp_path: Path) -> None:
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        unknown = client.post(
            "/api/v1/history/dukascopy/import",
            params={"symbol": "EUR/JPY", "date": "2025-01-02"},
        )
        future = client.post(
            "/api/v1/history/dukascopy/import",
            params={
                "symbol": "EUR/USD",
                "date": "2999-01-02",
                "start_hour": 0,
                "end_hour": 1,
            },
        )
        oversized_range = client.post(
            "/api/v1/history/dukascopy/import",
            params={
                "symbol": "EUR/USD",
                "date": "2025-01-02",
                "start_hour": 0,
                "end_hour": 4,
            },
        )

    assert unknown.status_code == 422
    assert future.status_code == 422
    assert oversized_range.status_code == 422


def test_catalog_includes_previously_loaded_historical_instruments(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO candles (
                provider, market_type, symbol, interval_seconds, timestamp_ms,
                open, high, low, close, data_kind
            ) VALUES ('Dukascopy', 'CLASSIC', 'GBP/USD', 60, 1000, 1, 2, 0.5, 1.5, 'HISTORICAL')
            """
        )

    with TestClient(create_app(database_path)) as client:
        response = client.get("/api/v1/catalog")

    assert response.status_code == 200
    assert response.json()["instruments"] == [
        {
            "provider": "Dukascopy",
            "market_type": "CLASSIC",
            "symbol": "GBP/USD",
            "display_name": "GBP/USD",
        }
    ]


def test_backtest_analysis_window_does_not_read_future_candles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.backtesting as backtesting

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
    observed_windows: list[tuple[int, ...]] = []
    real_analyze = backtesting.analyze_candles

    def capture_window(values: list[Candle]):
        observed_windows.append(tuple(candle.timestamp_ms for candle in values))
        return real_analyze(values)

    monkeypatch.setattr(backtesting, "analyze_candles", capture_window)
    backtesting.run_backtest(
        candles,
        provider="Dukascopy",
        symbol="EUR/USD",
        candle_interval_seconds=60,
        expiry_seconds=60,
    )

    assert observed_windows
    assert observed_windows[0] == tuple(candle.timestamp_ms for candle in candles[:96])
    assert all(
        window[-1] < candles[index].timestamp_ms
        for index, window in enumerate(observed_windows, start=96)
    )


def test_dukascopy_backtest_api_reads_saved_history_and_returns_expiry_results(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    now_ms = int(time.time() * 1000)
    candles = [
        Candle(
            timestamp_ms=now_ms - (120 - index) * 60_000,
            open=1 + index * 0.001,
            high=1.001 + index * 0.001,
            low=0.999 + index * 0.001,
            close=1.001 + index * 0.001,
        )
        for index in range(120)
    ]
    with connect(database_path) as connection:
        save_historical_candles(
            connection,
            candles,
            provider="Dukascopy",
            symbol="EUR/USD",
            interval_seconds=60,
        )

    with TestClient(create_app(database_path)) as client:
        response = client.get(
            "/api/v1/backtest/dukascopy",
            params=[
                ("symbol", "EUR/USD"),
                ("period_days", 1),
                ("expiry_seconds", 60),
                ("expiry_seconds", 300),
                ("expiry_seconds", 900),
            ],
        )

    assert response.status_code == 200
    report = response.json()
    assert report["provider"] == "Dukascopy"
    assert report["candle_count"] == 120
    assert report["missing_intervals"] == 0
    assert report["cost_adjusted"] is False
    assert len(report["results"]) == 3
    assert [item["expiry_seconds"] for item in report["results"]] == [60, 300, 900]
    assert all(item["status"] == "ok" for item in report["results"])
    assert all(item["trade_count"] > 0 for item in report["results"])
    assert all(
        item["wins"] + item["losses"] == item["trade_count"]
        for item in report["results"]
    )


def test_dukascopy_backtest_api_does_not_report_accuracy_without_history(
    tmp_path: Path,
) -> None:
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.get(
            "/api/v1/backtest/dukascopy",
            params={"symbol": "EUR/USD", "expiry_seconds": 300},
        )

    assert response.status_code == 200
    report = response.json()
    assert report["candle_count"] == 0
    assert report["results"][0]["status"] == "insufficient_data"
    assert report["results"][0]["win_rate_percent"] is None


def test_dukascopy_backtest_api_rejects_unvalidated_expiration(
    tmp_path: Path,
) -> None:
    with TestClient(create_app(tmp_path / "signals.db")) as client:
        response = client.get(
            "/api/v1/backtest/dukascopy",
            params={"symbol": "EUR/USD", "expiry_seconds": 7},
        )

    assert response.status_code == 422


def test_dukascopy_backtest_supports_short_expiry_only_with_second_candles(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    now_ms = int(time.time() * 1000)
    candles = [
        Candle(
            timestamp_ms=now_ms - (120 - index) * 1000,
            open=1 + index * 0.0001,
            high=1.0001 + index * 0.0001,
            low=0.9999 + index * 0.0001,
            close=1.0001 + index * 0.0001,
        )
        for index in range(120)
    ]
    with connect(database_path) as connection:
        save_historical_candles(
            connection,
            candles,
            provider="Dukascopy",
            symbol="EUR/USD",
            interval_seconds=1,
        )

    with TestClient(create_app(database_path)) as client:
        response = client.get(
            "/api/v1/backtest/dukascopy",
            params={"symbol": "EUR/USD", "period_days": 1, "expiry_seconds": 5},
        )

    assert response.status_code == 200
    report = response.json()
    assert report["available_intervals_seconds"] == [1]
    assert 5 in report["available_expirations_seconds"]
    assert report["results"][0]["resolution_seconds"] == 1
    assert report["results"][0]["status"] == "ok"
