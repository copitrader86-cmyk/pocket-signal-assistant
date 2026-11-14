import csv
import io
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from app.market_data import Candle


CSV_COLUMNS = {"timestamp", "open", "high", "low", "close"}
MAX_CSV_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True)
class ImportReport:
    rows_read: int
    rows_imported: int
    invalid_rows: int
    duplicate_rows: int
    missing_intervals: int
    interval_seconds: int


def _timestamp_ms(value: str) -> int:
    timestamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return int(timestamp.timestamp() * 1000)


def import_historical_csv(
    connection: sqlite3.Connection,
    csv_text: str,
    *,
    provider: str,
    symbol: str,
    interval_seconds: int,
) -> ImportReport:
    if len(csv_text.encode("utf-8")) > MAX_CSV_BYTES:
        raise ValueError("CSV file exceeds 10 MiB limit")
    if not provider.strip() or len(provider) > 80:
        raise ValueError("Provider name is required and must be at most 80 characters")
    if not symbol.strip() or len(symbol) > 24:
        raise ValueError("Symbol is required and must be at most 24 characters")
    if interval_seconds <= 0:
        raise ValueError("Interval must be positive")

    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames is None or not CSV_COLUMNS.issubset(reader.fieldnames):
        raise ValueError("CSV must include timestamp, open, high, low, and close columns")

    candles_by_timestamp: dict[int, Candle] = {}
    rows_read = 0
    invalid_rows = 0
    duplicate_rows = 0

    for row in reader:
        rows_read += 1
        try:
            candle = Candle(
                timestamp_ms=_timestamp_ms(row["timestamp"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=(
                    float(row["volume"])
                    if row.get("volume") not in (None, "")
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError, OverflowError):
            invalid_rows += 1
            continue

        if candle.timestamp_ms in candles_by_timestamp:
            duplicate_rows += 1
            continue
        candles_by_timestamp[candle.timestamp_ms] = candle

    candles = sorted(candles_by_timestamp.values(), key=lambda candle: candle.timestamp_ms)
    if len(candles) < 2:
        missing_intervals = 0
    else:
        interval_ms = interval_seconds * 1000
        missing_intervals = sum(
            max(0, (right.timestamp_ms - left.timestamp_ms) // interval_ms - 1)
            for left, right in zip(candles, candles[1:])
        )

    with connection:
        cursor = connection.executemany(
            """
            INSERT INTO candles (
                provider, market_type, symbol, interval_seconds, timestamp_ms,
                open, high, low, close, volume, data_kind
            ) VALUES (?, 'CLASSIC', ?, ?, ?, ?, ?, ?, ?, ?, 'HISTORICAL')
            ON CONFLICT (provider, symbol, interval_seconds, timestamp_ms, data_kind)
            DO NOTHING
            """,
            [
                (
                    provider,
                    symbol,
                    interval_seconds,
                    candle.timestamp_ms,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                )
                for candle in candles
            ],
        )
        inserted = cursor.rowcount

    return ImportReport(
        rows_read=rows_read,
        rows_imported=inserted,
        invalid_rows=invalid_rows,
        duplicate_rows=duplicate_rows,
        missing_intervals=missing_intervals,
        interval_seconds=interval_seconds,
    )
