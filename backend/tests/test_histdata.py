import asyncio
import io
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

import app.main as main
from app.database import connect, initialize_database
from app.histdata import HistDataClient, PROVIDER_NAME, parse_m1_csv
from app.historical_import import save_historical_candles
from app.main import create_app
from app.market_data import Candle
from fastapi.testclient import TestClient


def _archive(symbol_code: str, year: int, csv_text: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"DAT_ASCII_{symbol_code}_M1_{year}.csv", csv_text)
    return output.getvalue()


def test_parse_m1_csv_converts_fixed_est_and_reports_invalid_rows() -> None:
    batch = parse_m1_csv(
        "\ufeff20240101 170000;1.10000;1.10020;1.09990;1.10010;0\n"
        "20240101 170200;1.10010;1.10030;1.10000;1.10020;0\n"
        "20240101 170300;bad;1.1;1.0;1.05;0\n",
        symbol="EUR/USD",
        year=2024,
    )

    assert batch.rows_read == 3
    assert batch.invalid_rows == 1
    assert batch.duplicate_rows == 0
    assert batch.missing_intervals == 1
    assert batch.candles[0].timestamp_ms == int(
        datetime(2024, 1, 1, 22, 0, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert batch.candles[0].high == 1.1002


def test_histdata_fetch_uses_official_annual_form_and_decodes_archive() -> None:
    requested: list[httpx.Request] = []
    csv_text = (
        "20240101 170000;1.10000;1.10020;1.09990;1.10010;0\n"
        "20240101 170100;1.10010;1.10030;1.10000;1.10020;0\n"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                text=(
                    '<form id="file_down"><input name="tk" value="token">'
                    '<input name="date" value="2024">'
                    '<input name="datemonth" value="2024">'
                    '<input name="platform" value="ASCII">'
                    '<input name="timeframe" value="M1">'
                    '<input name="fxpair" value="EURUSD"></form>'
                ),
            )
        return httpx.Response(
            200,
            content=_archive("EURUSD", 2024, csv_text),
            headers={"Content-Type": "application/octet-stream"},
        )

    async def fetch() -> object:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://www.histdata.com",
        ) as http:
            async with HistDataClient(client=http) as provider:
                return await provider.fetch_year("EUR/USD", 2024)

    batch = asyncio.run(fetch())

    assert requested[0].method == "GET"
    assert requested[1].method == "POST"
    assert requested[1].url.path == "/get.php"
    assert b"tk=token" in requested[1].content
    assert batch.symbol == "EUR/USD"
    assert batch.rows_read == 2
    assert len(batch.candles) == 2


def test_available_years_only_returns_full_year_archives() -> None:
    html = (
        '<a href="/download-free-forex-historical-data/?/ascii/'
        '1-minute-bar-quotes/eurusd/2025"><strong>2025 - .Zip with Full '
        '2025 Year Data</strong></a>'
        '<a href="/download-free-forex-historical-data/?/ascii/'
        '1-minute-bar-quotes/eurusd/2026/11"><strong>2026 / November</strong></a>'
    )

    async def get_years() -> tuple[int, ...]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, text=html)
            ),
        ) as http:
            async with HistDataClient(client=http) as provider:
                return await provider.available_years("EUR/USD")

    assert asyncio.run(get_years()) == (2025,)


def test_histdata_candles_persist_idempotently_as_minute_resolution(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    batch = parse_m1_csv(
        "20240101 170000;1.10000;1.10020;1.09990;1.10010;0\n"
        "20240101 170100;1.10010;1.10030;1.10000;1.10020;0\n",
        symbol="EUR/USD",
        year=2024,
    )

    connection = connect(database_path)
    try:
        with connection:
            first = save_historical_candles(
                connection,
                batch.candles,
                provider=PROVIDER_NAME,
                symbol=batch.symbol,
                interval_seconds=60,
                source_resolution_ms=60_000,
            )
        with connection:
            second = save_historical_candles(
                connection,
                batch.candles,
                provider=PROVIDER_NAME,
                symbol=batch.symbol,
                interval_seconds=60,
                source_resolution_ms=60_000,
            )
        rows = connection.execute(
            "SELECT COUNT(*), MIN(source_resolution_ms) FROM candles"
        ).fetchone()
    finally:
        connection.close()

    assert (first, second) == (2, 0)
    assert rows[0] == 2
    assert rows[1] == 60_000


def test_testing_api_uses_histdata_m1_and_rejects_second_expirations(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    start_ms = int(datetime(2024, 1, 1, 22, tzinfo=timezone.utc).timestamp() * 1000)
    candles = tuple(
        Candle(
            timestamp_ms=start_ms + index * 60_000,
            open=1.1 + index * 0.00001,
            high=1.101 + index * 0.00001,
            low=1.099 + index * 0.00001,
            close=1.1005 + index * 0.00001,
        )
        for index in range(120)
    )
    connection = connect(database_path)
    try:
        with connection:
            save_historical_candles(
                connection,
                candles,
                provider=PROVIDER_NAME,
                symbol="EUR/USD",
                interval_seconds=60,
                source_resolution_ms=60_000,
            )
    finally:
        connection.close()

    with TestClient(create_app(database_path)) as client:
        availability = client.get(
            "/api/v1/backtest/dukascopy",
            params={
                "symbol": "EUR/USD",
                "period_days": 1,
                "expiry_seconds": 60,
                "availability_only": "true",
            },
        )
        seconds = client.get(
            "/api/v1/backtest/dukascopy",
            params={
                "symbol": "EUR/USD",
                "period_days": 1,
                "expiry_seconds": 5,
            },
        )

    assert availability.status_code == 200
    assert availability.json()["provider"] == PROVIDER_NAME
    assert availability.json()["available_intervals_seconds"] == [60]
    assert 60 in availability.json()["available_expirations_seconds"]
    assert 5 not in availability.json()["available_expirations_seconds"]
    assert availability.json()["results"] == []
    assert seconds.json()["results"][0]["status"] == "unsupported_resolution"


def test_histdata_sync_worker_starts_again_after_backend_restart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "persistent" / "signals.db"
    monkeypatch.setenv("DATABASE_PATH", str(database_path))
    monkeypatch.delenv("TWELVE_DATA_API_KEY", raising=False)
    started = threading.Event()
    worker_paths: list[Path] = []

    async def idle_worker(_: Path) -> None:
        await asyncio.Event().wait()

    async def histdata_worker(path: Path) -> None:
        worker_paths.append(path)
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(main, "_sync_public_reference_history", idle_worker)
    monkeypatch.setattr(main, "_sync_recent_dukascopy_history", idle_worker)
    monkeypatch.setattr(main, "_sync_histdata_history", histdata_worker)

    for _ in range(2):
        started.clear()
        with TestClient(main.create_app()) as client:
            assert client.get("/health").status_code == 200
            assert client.get("/api/v1/catalog").json()["instruments"] == []
            history = client.get("/api/v1/history/instruments")
            sources = client.get("/api/v1/sources/status")
            backtest = client.get(
                "/api/v1/backtest/history",
                params={
                    "symbol": "EUR/USD",
                    "availability_only": "true",
                },
            )
            assert history.status_code == 200
            assert sources.status_code == 200
            assert sources.json()["online"]["ready"] is False
            assert backtest.status_code == 200
            assert backtest.json()["available_intervals_seconds"] == []
            assert started.wait(timeout=5)

    assert worker_paths == [database_path, database_path]
    assert database_path.is_file()
