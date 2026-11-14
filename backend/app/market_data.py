import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx


PROVIDER_NAME = "TwelveData"
PROVIDER_BASE_URL = "https://api.twelvedata.com"
INTERVALS_BY_SECONDS = {
    60: "1min",
    300: "5min",
    900: "15min",
    1800: "30min",
    2700: "45min",
    3600: "1h",
    7200: "2h",
    14400: "4h",
    86400: "1day",
}


class MarketDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class Candle:
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None

    def __post_init__(self) -> None:
        values = (self.open, self.high, self.low, self.close)
        if not all(math.isfinite(value) and value > 0 for value in values):
            raise ValueError("OHLC values must be finite positive numbers")
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("Candle high is inconsistent with OHLC values")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("Candle low is inconsistent with OHLC values")
        if self.volume is not None and (
            not math.isfinite(self.volume) or self.volume < 0
        ):
            raise ValueError("Volume must be finite and non-negative")


def parse_provider_timestamp(value: str) -> int:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return int(timestamp.timestamp() * 1000)


class TwelveDataClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.getenv("TWELVE_DATA_API_KEY")
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "TwelveDataClient":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=15)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get(self, endpoint: str, **params: str | int) -> dict[str, Any]:
        if not self.api_key:
            raise MarketDataError("Twelve Data is not configured; set TWELVE_DATA_API_KEY")
        if self._client is None:
            raise RuntimeError("TwelveDataClient must be used as an async context manager")

        try:
            response = await self._client.get(
                f"{PROVIDER_BASE_URL}/{endpoint}",
                params={"apikey": self.api_key, **params},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise MarketDataError("Twelve Data request failed") from error

        if not isinstance(payload, dict):
            raise MarketDataError("Twelve Data returned an invalid response")
        if payload.get("status") == "error" or (
            "code" in payload and "message" in payload
        ):
            message = str(payload.get("message", "Provider rejected the request"))
            raise MarketDataError(message)
        return payload

    async def forex_pairs(self) -> list[dict[str, str]]:
        payload = await self._get("forex_pairs")
        entries = payload.get("data")
        if not isinstance(entries, list):
            raise MarketDataError("Twelve Data returned an invalid Forex catalog")

        instruments: list[dict[str, str]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            symbol = entry.get("symbol")
            if not isinstance(symbol, str) or "/" not in symbol:
                continue
            instruments.append(
                {
                    "provider": PROVIDER_NAME,
                    "market_type": "CLASSIC",
                    "symbol": symbol,
                    "display_name": symbol,
                }
            )
        return instruments

    async def time_series(
        self, symbol: str, interval_seconds: int, outputsize: int
    ) -> list[Candle]:
        interval = INTERVALS_BY_SECONDS.get(interval_seconds)
        if interval is None:
            raise ValueError("Twelve Data historical bars require a supported interval")
        if not 1 <= outputsize <= 5000:
            raise ValueError("outputsize must be between 1 and 5000")

        payload = await self._get(
            "time_series",
            symbol=symbol,
            interval=interval,
            outputsize=outputsize,
            timezone="UTC",
            order="ASC",
        )
        values = payload.get("values")
        if not isinstance(values, list):
            raise MarketDataError("Twelve Data returned no time-series values")

        candles: list[Candle] = []
        for value in values:
            if not isinstance(value, dict):
                continue
            try:
                timestamp = parse_provider_timestamp(str(value["datetime"]))
                candle = Candle(
                    timestamp_ms=timestamp,
                    open=float(value["open"]),
                    high=float(value["high"]),
                    low=float(value["low"]),
                    close=float(value["close"]),
                    volume=(
                        float(value["volume"])
                        if value.get("volume") not in (None, "")
                        else None
                    ),
                )
            except (KeyError, TypeError, ValueError) as error:
                raise MarketDataError("Twelve Data returned an invalid candle") from error
            candles.append(candle)

        candles.sort(key=lambda candle: candle.timestamp_ms)
        if len({candle.timestamp_ms for candle in candles}) != len(candles):
            raise MarketDataError("Twelve Data returned duplicate candle timestamps")
        return candles


def normalize_symbol(symbol: str) -> str:
    if not symbol or len(symbol) > 24 or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/_-"
        for character in symbol
    ):
        raise ValueError("Invalid instrument symbol")
    return symbol
