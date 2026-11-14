import asyncio
from datetime import date
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from app.database import connect, initialize_database
from app.main import create_app
from app.reference_data import (
    PROVIDER_NAME,
    FrankfurterClient,
    ReferenceRate,
    save_reference_rates,
)


def test_frankfurter_fetches_real_daily_reference_rate_series() -> None:
    requested: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request)
        return httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "EUR",
                "start_date": "2024-01-02",
                "end_date": "2024-01-03",
                "rates": {
                    "2024-01-03": {"USD": 1.095},
                    "2024-01-02": {"USD": 1.09},
                },
            },
        )

    async def fetch() -> list[ReferenceRate]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http:
            async with FrankfurterClient(client=http) as provider:
                return await provider.history(
                    "EUR/USD",
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                )

    values = asyncio.run(fetch())

    assert str(requested[0].url).startswith(
        "https://api.frankfurter.dev/v1/2024-01-02..2024-01-03"
    )
    assert requested[0].url.params["base"] == "EUR"
    assert requested[0].url.params["symbols"] == "USD"
    assert values == [
        ReferenceRate(date(2024, 1, 2), 1.09),
        ReferenceRate(date(2024, 1, 3), 1.095),
    ]


def test_reference_rates_are_idempotently_saved_with_source_and_date(
    tmp_path: Path,
) -> None:
    path: Path = tmp_path / "rates.db"
    initialize_database(path)
    values = [
        ReferenceRate(date(2024, 1, 2), 1.09),
        ReferenceRate(date(2024, 1, 3), 1.095),
    ]
    with connect(path) as connection:
        with connection:
            first = save_reference_rates(connection, "EUR/USD", values)
            second = save_reference_rates(connection, "EUR/USD", values)
        rows = connection.execute(
            """
            SELECT provider, base_currency, quote_currency, rate_date, rate
            FROM reference_rates
            ORDER BY rate_date
            """
        ).fetchall()

    assert first == second == 2
    assert [(row["provider"], row["rate_date"], row["rate"]) for row in rows] == [
        (PROVIDER_NAME, "2024-01-02", 1.09),
        (PROVIDER_NAME, "2024-01-03", 1.095),
    ]


def test_dukascopy_tick_storage_preserves_same_millisecond_quotes(
    tmp_path: Path,
) -> None:
    from app.dukascopy import Tick
    from app.historical_import import save_historical_ticks

    path = tmp_path / "ticks.db"
    initialize_database(path)
    ticks = (
        Tick(1_700_000_000_000, 1.1, 1.1001),
        Tick(1_700_000_000_000, 1.1001, 1.1002),
    )
    with connect(path) as connection:
        with connection:
            inserted = save_historical_ticks(connection, ticks, symbol="EUR/USD")
            repeated = save_historical_ticks(connection, ticks, symbol="EUR/USD")
        rows = connection.execute(
            """
            SELECT timestamp_ms, timestamp_resolution_ms, bid, ask
            FROM market_ticks ORDER BY tick_sequence
            """
        ).fetchall()

    assert inserted == 2
    assert repeated == 0
    assert len(rows) == 2
    assert all(row["timestamp_resolution_ms"] == 1 for row in rows)


def test_reference_rates_are_exposed_as_daily_reference_not_candles(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "rates-api.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        with connection:
            save_reference_rates(
                connection,
                "EUR/USD",
                [ReferenceRate(date(2024, 1, 2), 1.09)],
            )

    with TestClient(create_app(database_path)) as client:
        history = client.get(
            "/api/v1/history/reference",
            params={"symbol": "EUR/USD"},
        )
        coverage = client.get("/api/v1/history/reference/instruments")
        status = client.get("/api/v1/sources/status")

    assert history.status_code == 200
    assert history.json()["data_kind"] == "DAILY_REFERENCE_RATE"
    assert history.json()["resolution_seconds"] == 86400
    assert history.json()["rates"] == [{"date": "2024-01-02", "rate": 1.09}]
    assert coverage.status_code == 200
    eurusd = next(
        item
        for item in coverage.json()["instruments"]
        if item["symbol"] == "EUR/USD"
    )
    assert eurusd["observations"] == 1
    assert status.status_code == 200
    assert status.json()["historical"]["reference_status"] == (
        "daily_reference_only_not_intraday"
    )
