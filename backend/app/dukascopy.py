import asyncio
import lzma
import math
import struct
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from app.market_data import Candle, MarketDataError


PROVIDER_NAME = "Dukascopy"
BASE_URL = "https://datafeed.dukascopy.com/datafeed"
HOUR_MS = 60 * 60 * 1000
RECORD = struct.Struct(">IIIff")
MAX_FILE_BYTES = 25 * 1024 * 1024
MAX_TICKS_PER_REQUEST = 2_000_000
MAX_DECOMPRESSED_FILE_BYTES = (MAX_TICKS_PER_REQUEST + 1) * RECORD.size
MAX_HOURS_PER_REQUEST = 3
MAX_RATE_LIMIT_RETRIES = 2
MAX_RETRY_AFTER_SECONDS = 30

INSTRUMENTS = {
    "EUR/USD": "EURUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY",
    "USD/CHF": "USDCHF",
    "USD/CAD": "USDCAD",
    "AUD/USD": "AUDUSD",
    "NZD/USD": "NZDUSD",
}


@dataclass(frozen=True)
class Tick:
    timestamp_ms: int
    bid: float
    ask: float


@dataclass(frozen=True)
class HistoricalBatch:
    symbol: str
    date: date
    start_hour: int
    end_hour: int
    files_requested: int
    files_missing: int
    ticks_received: int
    missing_intervals: int
    candles: tuple[Candle, ...]
    ticks: tuple[Tick, ...] = ()


def parse_bi5(payload: bytes, *, hour_start_ms: int, price_scale: int) -> list[Tick]:
    if not payload:
        raise MarketDataError("Dukascopy returned an empty tick file")
    if price_scale <= 0:
        raise ValueError("Price scale must be positive")
    if len(payload) > MAX_FILE_BYTES:
        raise MarketDataError("Dukascopy tick file exceeds the 25 MiB limit")

    try:
        decoder = lzma.LZMADecompressor(
            format=lzma.FORMAT_AUTO,
            memlimit=64 * 1024 * 1024,
        )
        raw = decoder.decompress(
            payload,
            max_length=MAX_DECOMPRESSED_FILE_BYTES,
        )
    except (lzma.LZMAError, EOFError) as error:
        raise MarketDataError("Dukascopy tick file has invalid LZMA data") from error
    if (
        not decoder.eof
        or decoder.unused_data
        or len(raw) > MAX_TICKS_PER_REQUEST * RECORD.size
    ):
        raise MarketDataError("Dukascopy tick file is truncated or exceeds the tick limit")
    if not raw or len(raw) % RECORD.size:
        raise MarketDataError("Dukascopy tick file has an invalid record length")

    ticks: list[Tick] = []
    previous_offset = -1
    for offset, ask_raw, bid_raw, ask_volume, bid_volume in RECORD.iter_unpack(raw):
        if offset >= HOUR_MS or offset < previous_offset:
            raise MarketDataError("Dukascopy tick timestamps are invalid or unordered")
        if bid_raw <= 0 or ask_raw < bid_raw:
            raise MarketDataError("Dukascopy tick contains an invalid bid/ask quote")
        if not (
            math.isfinite(ask_volume)
            and math.isfinite(bid_volume)
            and ask_volume >= 0
            and bid_volume >= 0
        ):
            raise MarketDataError("Dukascopy tick contains invalid volume fields")
        ticks.append(
            Tick(
                timestamp_ms=hour_start_ms + offset,
                bid=bid_raw / price_scale,
                ask=ask_raw / price_scale,
            )
        )
        previous_offset = offset
    return ticks


def _price_scale(symbol: str) -> int:
    return 1_000 if symbol.endswith("/JPY") else 100_000


def _aggregate_bid_candles(ticks: list[Tick], interval_seconds: int) -> tuple[Candle, ...]:
    interval_ms = interval_seconds * 1000
    values: dict[int, list[float]] = {}
    for tick in ticks:
        bucket = tick.timestamp_ms // interval_ms * interval_ms
        candle = values.get(bucket)
        if candle is None:
            values[bucket] = [tick.bid, tick.bid, tick.bid, tick.bid]
        else:
            candle[1] = max(candle[1], tick.bid)
            candle[2] = min(candle[2], tick.bid)
            candle[3] = tick.bid
    return tuple(
        Candle(
            timestamp_ms=timestamp,
            open=values[timestamp][0],
            high=values[timestamp][1],
            low=values[timestamp][2],
            close=values[timestamp][3],
        )
        for timestamp in sorted(values)
    )


def _missing_intervals(candles: tuple[Candle, ...], interval_seconds: int) -> int:
    interval_ms = interval_seconds * 1000
    return sum(
        max(0, (right.timestamp_ms - left.timestamp_ms) // interval_ms - 1)
        for left, right in zip(candles, candles[1:])
    )


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


class DukascopyClient:
    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "DukascopyClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(20, connect=8),
                follow_redirects=True,
            )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def fetch_hour(
        self, symbol: str, trading_date: date, hour: int
    ) -> list[Tick] | None:
        instrument = INSTRUMENTS.get(symbol)
        if instrument is None:
            raise ValueError("Unsupported Dukascopy instrument")
        if not 0 <= hour <= 23:
            raise ValueError("Hour must be between 0 and 23 UTC")
        if self._client is None:
            raise RuntimeError("DukascopyClient must be used as an async context manager")

        url = (
            f"{BASE_URL}/{instrument}/{trading_date.year}/"
            f"{trading_date.month - 1:02d}/{trading_date.day:02d}/"
            f"{hour}h_ticks.bi5"
        )
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            try:
                response = await self._client.get(url)
            except httpx.HTTPError as error:
                raise MarketDataError("Dukascopy historical request failed") from error
            if response.status_code != 429:
                break
            if attempt == MAX_RATE_LIMIT_RETRIES:
                raise MarketDataError(
                    "Dukascopy rate limit remained after three attempts"
                )
            retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
            delay = max(float(2**attempt), retry_after or 0.0)
            if delay > MAX_RETRY_AFTER_SECONDS:
                raise MarketDataError(
                    "Dukascopy requested a retry delay over 30 seconds; "
                    "stopped without retrying early"
                )
            await asyncio.sleep(delay)
        if response.status_code == 404:
            return None
        try:
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise MarketDataError(
                f"Dukascopy returned HTTP {response.status_code}"
            ) from error

        hour_start_ms = int(
            datetime(
                trading_date.year,
                trading_date.month,
                trading_date.day,
                hour,
                tzinfo=timezone.utc,
            ).timestamp()
            * 1000
        )
        return await asyncio.to_thread(
            parse_bi5,
            response.content,
            hour_start_ms=hour_start_ms,
            price_scale=_price_scale(symbol),
        )

    async def fetch_candles(
        self,
        symbol: str,
        trading_date: date,
        *,
        start_hour: int,
        end_hour: int,
        interval_seconds: int = 60,
    ) -> HistoricalBatch:
        if symbol not in INSTRUMENTS:
            raise ValueError("Unsupported Dukascopy instrument")
        if not 0 <= start_hour < end_hour <= 24:
            raise ValueError("Hour range must satisfy 0 <= start_hour < end_hour <= 24")
        if end_hour - start_hour > MAX_HOURS_PER_REQUEST:
            raise ValueError(
                f"Request at most {MAX_HOURS_PER_REQUEST} UTC hours at a time"
            )
        if not 1 <= interval_seconds <= 3600:
            raise ValueError("Candle interval must be between 1 and 3600 seconds")
        if self._client is None:
            raise RuntimeError("DukascopyClient must be used as an async context manager")

        ticks: list[Tick] = []
        files_missing = 0
        for hour in range(start_hour, end_hour):
            hour_ticks = await self.fetch_hour(symbol, trading_date, hour)
            if hour_ticks is None:
                files_missing += 1
                continue
            ticks.extend(hour_ticks)
            if len(ticks) > MAX_TICKS_PER_REQUEST:
                raise MarketDataError("Dukascopy request exceeds the 2 million tick limit")

        candles = await asyncio.to_thread(
            _aggregate_bid_candles, ticks, interval_seconds
        )
        missing_intervals = await asyncio.to_thread(
            _missing_intervals, candles, interval_seconds
        )
        return HistoricalBatch(
            symbol=symbol,
            date=trading_date,
            start_hour=start_hour,
            end_hour=end_hour,
            files_requested=end_hour - start_hour,
            files_missing=files_missing,
            ticks_received=len(ticks),
            missing_intervals=missing_intervals,
            candles=candles,
            ticks=tuple(ticks),
        )
