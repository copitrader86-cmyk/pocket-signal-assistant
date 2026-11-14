from __future__ import annotations

import sqlite3
import time
import math
from dataclasses import dataclass
from datetime import date

import httpx


PROVIDER_NAME = "Frankfurter"
BASE_URL = "https://api.frankfurter.dev/v1"
EARLIEST_DATE = date(1999, 1, 4)
SUPPORTED_PAIRS = (
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "USD/CHF",
    "USD/CAD",
    "AUD/USD",
    "NZD/USD",
)


@dataclass(frozen=True)
class ReferenceRate:
    rate_date: date
    rate: float


class FrankfurterClient:
    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "FrankfurterClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30, connect=10),
                headers={"User-Agent": "PocketSignalAssistant/0.1"},
            )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def history(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> list[ReferenceRate]:
        if symbol not in SUPPORTED_PAIRS:
            raise ValueError("Unsupported reference-rate pair")
        if start > end:
            raise ValueError("Reference-rate start date must not follow end date")
        if self._client is None:
            raise RuntimeError(
                "FrankfurterClient must be used as an async context manager"
            )

        base, quote = symbol.split("/")
        try:
            response = await self._client.get(
                f"{BASE_URL}/{start.isoformat()}..{end.isoformat()}",
                params={"base": base, "symbols": quote},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise RuntimeError("Frankfurter historical request failed") from error

        rates = payload.get("rates") if isinstance(payload, dict) else None
        if not isinstance(rates, dict):
            raise RuntimeError("Frankfurter returned an invalid historical response")

        values: list[ReferenceRate] = []
        for date_text, rate_map in rates.items():
            if not isinstance(rate_map, dict) or quote not in rate_map:
                continue
            try:
                rate_date = date.fromisoformat(date_text)
                rate = float(rate_map[quote])
            except (TypeError, ValueError, OverflowError) as error:
                raise RuntimeError(
                    "Frankfurter returned an invalid reference rate"
                ) from error
            if not math.isfinite(rate) or rate <= 0:
                raise RuntimeError("Frankfurter returned an invalid reference rate")
            values.append(ReferenceRate(rate_date=rate_date, rate=rate))
        return sorted(values, key=lambda value: value.rate_date)


def save_reference_rates(
    connection: sqlite3.Connection,
    symbol: str,
    rates: list[ReferenceRate],
) -> int:
    if symbol not in SUPPORTED_PAIRS:
        raise ValueError("Unsupported reference-rate pair")
    if not rates:
        return 0
    base, quote = symbol.split("/")
    retrieved_at_ms = int(time.time() * 1000)
    connection.executemany(
        """
        INSERT INTO reference_rates (
            provider, base_currency, quote_currency, rate_date, rate, retrieved_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (provider, base_currency, quote_currency, rate_date)
        DO UPDATE SET rate = excluded.rate, retrieved_at_ms = excluded.retrieved_at_ms
        """,
        [
            (
                PROVIDER_NAME,
                base,
                quote,
                item.rate_date.isoformat(),
                item.rate,
                retrieved_at_ms,
            )
            for item in rates
        ],
    )
    return len(rates)
