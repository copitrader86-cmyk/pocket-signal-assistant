import asyncio
import os
import sqlite3
import json
import logging
import math
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date as Date, datetime, time as DateTime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Iterator

import websockets
from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from app.analytics import analyze_candles
from app.backtesting import BacktestError, run_backtest
from app.database import connect, initialize_database
from app.dukascopy import (
    MAX_HOURS_PER_REQUEST,
    DukascopyClient,
    HistoricalBatch,
    INSTRUMENTS as DUKASCOPY_INSTRUMENTS,
)
from app.dukascopy import PROVIDER_NAME as DUKASCOPY_PROVIDER
from app.historical_import import (
    MAX_CSV_BYTES,
    import_historical_csv,
    save_historical_candles,
    save_historical_ticks,
)
from app.histdata import (
    PROVIDER_NAME as HISTDATA_PROVIDER,
    SOURCE_RESOLUTION_MS as HISTDATA_RESOLUTION_MS,
    HistDataClient,
    HistDataBatch,
)
from app.learning import evaluate_artifact, predict_win_probability, train_model
from app.market_data import (
    Candle,
    INTERVALS_BY_SECONDS,
    PROVIDER_NAME,
    MarketDataError,
    TwelveDataClient,
    normalize_symbol,
)
from app.reference_data import (
    EARLIEST_DATE as REFERENCE_EARLIEST_DATE,
    PROVIDER_NAME as REFERENCE_PROVIDER,
    SUPPORTED_PAIRS as REFERENCE_PAIRS,
    FrankfurterClient,
    save_reference_rates,
)


logger = logging.getLogger(__name__)


def _set_reference_sync_status(
    database_path: Path,
    symbol: str,
    status: str,
    *,
    provider: str = REFERENCE_PROVIDER,
    error_message: str | None = None,
    successful: bool = False,
    scanned_until_ms: int | None = None,
) -> None:
    now_ms = int(time.time() * 1000)
    connection = connect(database_path)
    try:
        with connection:
            connection.execute(
                """
                INSERT INTO public_history_sync (
                    provider, symbol, status, started_at_ms, updated_at_ms,
                    last_success_at_ms, error_message, last_scanned_timestamp_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (provider, symbol) DO UPDATE SET
                    status = excluded.status,
                    started_at_ms = CASE
                        WHEN excluded.status = 'loading' THEN excluded.started_at_ms
                        ELSE public_history_sync.started_at_ms
                    END,
                    updated_at_ms = excluded.updated_at_ms,
                    last_success_at_ms = COALESCE(
                        excluded.last_success_at_ms,
                        public_history_sync.last_success_at_ms
                    ),
                    error_message = excluded.error_message,
                    last_scanned_timestamp_ms = COALESCE(
                        excluded.last_scanned_timestamp_ms,
                        public_history_sync.last_scanned_timestamp_ms
                    )
                """,
                (
                    provider,
                    symbol,
                    status,
                    now_ms if status == "loading" else None,
                    now_ms,
                    now_ms if successful else None,
                    error_message,
                    scanned_until_ms,
                ),
            )
    finally:
        connection.close()


def _persist_dukascopy_batch(
    database_path: Path,
    batch: HistoricalBatch,
    *,
    interval_seconds: int,
) -> tuple[int, int]:
    if not batch.candles and not batch.ticks:
        return 0, 0
    connection = connect(database_path)
    try:
        with connection:
            candles_inserted = save_historical_candles(
                connection,
                batch.candles,
                provider=DUKASCOPY_PROVIDER,
                symbol=batch.symbol,
                interval_seconds=interval_seconds,
                source_resolution_ms=1,
            )
            ticks_inserted = save_historical_ticks(
                connection,
                batch.ticks,
                symbol=batch.symbol,
            )
            connection.execute(
                """
                INSERT INTO market_instruments (
                    provider, market_type, symbol, display_name, updated_at_ms
                ) VALUES (?, 'CLASSIC', ?, ?, ?)
                ON CONFLICT (provider, market_type, symbol) DO NOTHING
                """,
                (
                    DUKASCOPY_PROVIDER,
                    batch.symbol,
                    batch.symbol,
                    int(time.time() * 1000),
                ),
            )
        return candles_inserted, ticks_inserted
    finally:
        connection.close()


def _persist_histdata_batch(database_path: Path, batch: HistDataBatch) -> int:
    if not batch.candles:
        return 0
    year_start = int(
        datetime(batch.year, 1, 1, tzinfo=timezone(timedelta(hours=-5)))
        .astimezone(timezone.utc)
        .timestamp()
        * 1000
    )
    next_year_start = int(
        datetime(batch.year + 1, 1, 1, tzinfo=timezone(timedelta(hours=-5)))
        .astimezone(timezone.utc)
        .timestamp()
        * 1000
    )
    connection = connect(database_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM candles
                WHERE provider = ? AND symbol = ? AND interval_seconds = 60
                    AND timestamp_ms >= ? AND timestamp_ms < ?
                """,
                (HISTDATA_PROVIDER, batch.symbol, year_start, next_year_start),
            )
            inserted = save_historical_candles(
                connection,
                batch.candles,
                provider=HISTDATA_PROVIDER,
                symbol=batch.symbol,
                interval_seconds=60,
                source_resolution_ms=HISTDATA_RESOLUTION_MS,
            )
            connection.execute(
                """
                INSERT INTO market_instruments (
                    provider, market_type, symbol, display_name, updated_at_ms
                ) VALUES (?, 'CLASSIC', ?, ?, ?)
                ON CONFLICT (provider, market_type, symbol) DO UPDATE SET
                    enabled = 1,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    HISTDATA_PROVIDER,
                    batch.symbol,
                    batch.symbol,
                    int(time.time() * 1000),
                ),
            )
        return inserted
    finally:
        connection.close()


def _histdata_sync_state(
    database_path: Path, symbol: str
) -> tuple[tuple[int | None, int | None] | None, int | None]:
    connection = connect(database_path)
    try:
        sync_row = connection.execute(
            """
            SELECT last_scanned_timestamp_ms, last_success_at_ms
            FROM public_history_sync
            WHERE provider = ? AND symbol = ?
            """,
            (HISTDATA_PROVIDER, symbol),
        ).fetchone()
        latest_row = connection.execute(
            """
            SELECT MAX(timestamp_ms) AS latest_timestamp_ms
            FROM candles
            WHERE provider = ? AND symbol = ? AND interval_seconds = 60
                AND data_kind = 'HISTORICAL'
            """,
            (HISTDATA_PROVIDER, symbol),
        ).fetchone()
        return (
            (sync_row["last_scanned_timestamp_ms"], sync_row["last_success_at_ms"])
            if sync_row
            else None,
            latest_row["latest_timestamp_ms"],
        )
    finally:
        connection.close()


async def _sync_public_reference_history(database_path: Path) -> None:
    while True:
        end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
        async with FrankfurterClient() as provider:
            for symbol in REFERENCE_PAIRS:
                _set_reference_sync_status(database_path, symbol, "loading")
                connection = connect(database_path)
                try:
                    coverage = connection.execute(
                        """
                        SELECT COUNT(*) AS rate_count
                        FROM reference_rates
                        WHERE provider = ? AND base_currency = ? AND quote_currency = ?
                        """,
                        (
                            REFERENCE_PROVIDER,
                            *symbol.split("/"),
                        ),
                    ).fetchone()
                    start_date = (
                        REFERENCE_EARLIEST_DATE
                        if coverage["rate_count"] == 0
                        else max(
                            REFERENCE_EARLIEST_DATE,
                            end_date - timedelta(days=30),
                        )
                    )
                    if start_date <= end_date:
                        try:
                            rates = await provider.history(symbol, start_date, end_date)
                            if not rates:
                                raise RuntimeError(
                                    "Frankfurter returned no data for a non-empty history range"
                                )
                            with connection:
                                save_reference_rates(connection, symbol, rates)
                            _set_reference_sync_status(
                                database_path,
                                symbol,
                                "ready",
                                successful=True,
                            )
                        except (RuntimeError, sqlite3.Error) as error:
                            _set_reference_sync_status(
                                database_path,
                                symbol,
                                "error",
                                error_message=str(error)[:500],
                            )
                            logger.exception(
                                "Public reference-history synchronization failed for %s",
                                symbol,
                            )
                finally:
                    connection.close()
                await asyncio.sleep(0.25)
        await asyncio.sleep(12 * 60 * 60)


async def _sync_histdata_history(database_path: Path) -> None:
    while True:
        made_progress = False
        now_ms = int(time.time() * 1000)
        for symbol in DUKASCOPY_INSTRUMENTS:
            try:
                async with HistDataClient() as provider:
                    years = await provider.available_years(symbol)
                    sync_state, latest_timestamp_ms = await asyncio.to_thread(
                        _histdata_sync_state, database_path, symbol
                    )

                    newest_year = years[-1]
                    oldest_year = years[0]
                    latest_saved_year = (
                        datetime.fromtimestamp(
                            latest_timestamp_ms / 1000,
                            tz=timezone(timedelta(hours=-5)),
                        ).year
                        if latest_timestamp_ms is not None
                        else None
                    )
                    cursor = sync_state[0] if sync_state else None
                    if latest_saved_year is None or latest_saved_year < newest_year:
                        target_year = newest_year
                    elif cursor is None:
                        target_year = newest_year
                    elif cursor > 0:
                        last_year = datetime.fromtimestamp(
                            cursor / 1000, tz=timezone.utc
                        ).year
                        target_year = last_year - 1 if last_year > oldest_year else None
                    elif (
                        cursor == 0
                        and sync_state is not None
                        and sync_state[1] is not None
                        and now_ms - sync_state[1]
                        >= 7 * 24 * 60 * 60 * 1000
                    ):
                        target_year = newest_year
                    else:
                        target_year = None

                    if target_year is None or target_year not in years:
                        continue

                    await asyncio.to_thread(
                        _set_reference_sync_status,
                        database_path,
                        symbol,
                        "loading",
                        provider=HISTDATA_PROVIDER,
                    )
                    batch = await provider.fetch_year(symbol, target_year)
                    inserted = await asyncio.to_thread(
                        _persist_histdata_batch, database_path, batch
                    )
                    scan_cursor = (
                        0
                        if target_year == oldest_year or cursor == 0
                        else int(
                            datetime(
                                target_year, 1, 1, tzinfo=timezone.utc
                            ).timestamp()
                            * 1000
                        )
                    )
                    await asyncio.to_thread(
                        _set_reference_sync_status,
                        database_path,
                        symbol,
                        "ready",
                        provider=HISTDATA_PROVIDER,
                        successful=True,
                        scanned_until_ms=scan_cursor,
                    )
                    made_progress = True
                    logger.info(
                        "HistData imported %s %s: %s M1 candles (%s inserted, %s gaps)",
                        symbol,
                        target_year,
                        len(batch.candles),
                        inserted,
                        batch.missing_intervals,
                    )
            except (MarketDataError, sqlite3.Error, ValueError) as error:
                await asyncio.to_thread(
                    _set_reference_sync_status,
                    database_path,
                    symbol,
                    "error",
                    provider=HISTDATA_PROVIDER,
                    error_message=str(error)[:500],
                )
                logger.warning("Automatic HistData sync failed for %s: %s", symbol, error)
            await asyncio.sleep(2)
        await asyncio.sleep(60 if made_progress else 24 * 60 * 60)


async def _sync_recent_dukascopy_history(database_path: Path) -> None:
    while True:
        now = datetime.now(timezone.utc)
        completed_hour = now.replace(minute=0, second=0, microsecond=0)
        for symbol in DUKASCOPY_INSTRUMENTS:
            connection = connect(database_path)
            try:
                row = connection.execute(
                    """
                    SELECT MAX(timestamp_ms) AS latest_timestamp_ms
                    FROM market_ticks WHERE provider = ? AND symbol = ?
                    """,
                    (DUKASCOPY_PROVIDER, symbol),
                ).fetchone()
                sync_row = connection.execute(
                    """
                    SELECT last_scanned_timestamp_ms FROM public_history_sync
                    WHERE provider = ? AND symbol = ?
                    """,
                    (DUKASCOPY_PROVIDER, symbol),
                ).fetchone()
            finally:
                connection.close()
            latest_scanned_ms = max(
                (
                    value
                    for value in (
                        row["latest_timestamp_ms"],
                        sync_row["last_scanned_timestamp_ms"] if sync_row else None,
                    )
                    if value is not None
                ),
                default=None,
            )
            if latest_scanned_ms is None:
                window_start = completed_hour - timedelta(hours=MAX_HOURS_PER_REQUEST)
            else:
                latest_scanned_time = datetime.fromtimestamp(
                    latest_scanned_ms / 1000,
                    tz=timezone.utc,
                )
                window_start = latest_scanned_time.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
            if window_start >= completed_hour:
                continue
            window_end = min(
                window_start + timedelta(hours=MAX_HOURS_PER_REQUEST),
                completed_hour,
                window_start.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                + timedelta(days=1),
            )
            if window_end <= window_start:
                continue
            start_hour = window_start.hour
            end_hour = 24 if window_end.date() != window_start.date() else window_end.hour
            if start_hour >= end_hour:
                continue
            _set_reference_sync_status(
                database_path,
                symbol,
                "loading",
                provider=DUKASCOPY_PROVIDER,
            )
            try:
                async with DukascopyClient() as provider:
                    batch = await provider.fetch_candles(
                        symbol,
                        window_start.date(),
                        start_hour=start_hour,
                        end_hour=end_hour,
                        interval_seconds=1,
                    )
                await asyncio.to_thread(
                    _persist_dukascopy_batch,
                    database_path,
                    batch,
                    interval_seconds=1,
                )
                _set_reference_sync_status(
                    database_path,
                    symbol,
                    "ready",
                    provider=DUKASCOPY_PROVIDER,
                    successful=True,
                    scanned_until_ms=int(window_end.timestamp() * 1000),
                )
            except (MarketDataError, sqlite3.Error, ValueError) as error:
                _set_reference_sync_status(
                    database_path,
                    symbol,
                    "error",
                    provider=DUKASCOPY_PROVIDER,
                    error_message=str(error)[:500],
                )
                logger.warning(
                    "Automatic Dukascopy sync failed for %s: %s", symbol, error
                )
            await asyncio.sleep(1)
        await asyncio.sleep(24 * 60 * 60)

class Instrument(BaseModel):
    provider: str
    market_type: str
    symbol: str
    display_name: str


class CatalogResponse(BaseModel):
    instruments: list[Instrument]
    classic_available: bool
    otc_available: bool


class HistoricalImportRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=80)
    symbol: str = Field(min_length=1, max_length=24)
    interval_seconds: int = Field(gt=0, le=86400)
    csv: str = Field(max_length=MAX_CSV_BYTES)


class CandleResponse(BaseModel):
    timestamp_ms: int
    source_resolution_ms: int | None = None
    open: float
    high: float
    low: float
    close: float
    volume: float | None


class CandleSeriesResponse(BaseModel):
    provider: str
    market_type: str
    symbol: str
    interval_seconds: int
    data_kind: str
    candles: list[CandleResponse]


class AnalysisRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=24)
    market_type: str = Field(pattern="^(CLASSIC|OTC)$")
    expiry_seconds: int
    session_id: str | None = None


class AnalysisResponse(BaseModel):
    id: str
    symbol: str
    provider: str
    market_type: str
    direction: str
    candidate_direction: str
    expiry_seconds: int
    issued_at_ms: int
    recommended_entry_at_ms: int | None
    expires_at_ms: int
    quote_timestamp_ms: int | None
    quote_latency_ms: int | None
    probability_up: float | None
    probability_down: float | None
    analytics: dict[str, float | str | None]
    explanation: str
    analysis_latency_ms: int


class OutcomeRequest(BaseModel):
    outcome: str = Field(pattern="^(WON|LOST|MISSED)$")


class SessionRequest(BaseModel):
    started_at_ms: int | None = None


def _provider_http_error(error: MarketDataError) -> HTTPException:
    if "not configured" in str(error):
        return HTTPException(status_code=503, detail=str(error))
    return HTTPException(status_code=502, detail=f"Market-data provider error: {error}")


def create_app(database_path: Path | None = None) -> FastAPI:
    automatic_sync = database_path is None
    resolved_database_path = database_path or Path(
        os.environ.get("DATABASE_PATH", "data/signals.db")
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        initialize_database(resolved_database_path)
        sync_tasks = (
            [
                asyncio.create_task(
                    _sync_public_reference_history(resolved_database_path),
                    name="public-reference-history-sync",
                ),
                asyncio.create_task(
                    _sync_recent_dukascopy_history(resolved_database_path),
                    name="dukascopy-history-sync",
                ),
                asyncio.create_task(
                    _sync_histdata_history(resolved_database_path),
                    name="histdata-history-sync",
                ),
            ]
            if automatic_sync
            else []
        )
        try:
            yield
        finally:
            for sync_task in sync_tasks:
                sync_task.cancel()
            for sync_task in sync_tasks:
                try:
                    await sync_task
                except asyncio.CancelledError:
                    pass

    application = FastAPI(
        title="Pocket Signal Assistant API",
        version="0.1.0",
        lifespan=lifespan,
    )

    def get_connection() -> Iterator[sqlite3.Connection]:
        try:
            connection = connect(resolved_database_path)
        except sqlite3.Error as error:
            raise HTTPException(
                status_code=503, detail="Database is unavailable"
            ) from error
        try:
            yield connection
        finally:
            connection.close()

    @application.get("/health")
    def health(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, int | str]:
        connection.execute("SELECT 1")
        return {"status": "ok", "server_time_ms": int(time.time() * 1000)}

    @application.get("/api/v1/catalog", response_model=CatalogResponse)
    def catalog(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> CatalogResponse:
        rows = connection.execute(
            """
            SELECT provider, market_type, symbol, display_name
            FROM market_instruments
            WHERE enabled = 1
            UNION
            SELECT DISTINCT candles.provider, candles.market_type,
                candles.symbol, candles.symbol AS display_name
            FROM candles
            WHERE candles.data_kind = 'HISTORICAL'
                AND candles.market_type = 'CLASSIC'
                AND NOT EXISTS (
                    SELECT 1 FROM market_instruments
                    WHERE market_instruments.provider = candles.provider
                        AND market_instruments.market_type = candles.market_type
                        AND market_instruments.symbol = candles.symbol
                )
            ORDER BY market_type, symbol
            """
        ).fetchall()
        instruments = [Instrument.model_validate(dict(row)) for row in rows]
        return CatalogResponse(
            instruments=instruments,
            classic_available=any(
                instrument.market_type == "CLASSIC" for instrument in instruments
            ),
            otc_available=any(
                instrument.market_type == "OTC" for instrument in instruments
            ),
        )

    @application.get("/api/v1/history/instruments")
    @application.get("/api/v1/history/dukascopy/instruments")
    def dukascopy_instruments(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, object]:
        coverage: dict[str, dict[str, int]] = {}
        interval_coverage: dict[str, set[int]] = {}
        source_intervals: dict[tuple[str, str], set[int]] = {}
        source_resolutions: dict[str, set[int]] = {}
        for row in connection.execute(
            """
            SELECT symbol, provider, interval_seconds, source_resolution_ms,
                MIN(timestamp_ms) AS first_timestamp_ms,
                MAX(timestamp_ms) AS last_timestamp_ms, COUNT(*) AS candles_count
            FROM candles
            WHERE provider IN (?, ?) AND data_kind = 'HISTORICAL'
            GROUP BY symbol, provider, interval_seconds, source_resolution_ms
            """,
            (DUKASCOPY_PROVIDER, HISTDATA_PROVIDER),
        ):
            symbol = row["symbol"]
            summary = coverage.setdefault(
                symbol,
                {
                    "first_timestamp_ms": row["first_timestamp_ms"],
                    "last_timestamp_ms": row["last_timestamp_ms"],
                    "candles_count": 0,
                },
            )
            summary["first_timestamp_ms"] = min(
                summary["first_timestamp_ms"], row["first_timestamp_ms"]
            )
            summary["last_timestamp_ms"] = max(
                summary["last_timestamp_ms"], row["last_timestamp_ms"]
            )
            summary["candles_count"] += row["candles_count"]
            interval_coverage.setdefault(symbol, set()).add(row["interval_seconds"])
            source_intervals.setdefault((symbol, row["provider"]), set()).add(
                row["interval_seconds"]
            )
            if row["source_resolution_ms"] is not None:
                source_resolutions.setdefault(symbol, set()).add(
                    row["source_resolution_ms"]
                )
        tick_coverage = {
            row["symbol"]: dict(row)
            for row in connection.execute(
                """
                SELECT symbol, MIN(timestamp_ms) AS first_tick_timestamp_ms,
                    MAX(timestamp_ms) AS last_tick_timestamp_ms,
                    COUNT(*) AS ticks_count
                FROM market_ticks
                WHERE provider = ?
                GROUP BY symbol
                """,
                (DUKASCOPY_PROVIDER,),
            ).fetchall()
        }
        return {
            "provider": f"{HISTDATA_PROVIDER} / {DUKASCOPY_PROVIDER}",
            "instruments": [
                {
                    "provider": (
                        DUKASCOPY_PROVIDER
                        if 1 in source_intervals.get((symbol, DUKASCOPY_PROVIDER), set())
                        else HISTDATA_PROVIDER
                        if source_intervals.get((symbol, HISTDATA_PROVIDER))
                        else DUKASCOPY_PROVIDER
                    ),
                    "market_type": "CLASSIC",
                    "symbol": symbol,
                    "display_name": symbol,
                    "history_available": symbol in coverage,
                    "first_timestamp_ms": coverage.get(symbol, {}).get(
                        "first_timestamp_ms"
                    ),
                    "last_timestamp_ms": coverage.get(symbol, {}).get(
                        "last_timestamp_ms"
                    ),
                    "candles_count": coverage.get(symbol, {}).get("candles_count", 0),
                    "available_sources": [
                        source
                        for source in (HISTDATA_PROVIDER, DUKASCOPY_PROVIDER)
                        if source_intervals.get((symbol, source))
                    ],
                    "available_intervals_seconds": sorted(
                        interval_coverage.get(symbol, set())
                    ),
                    "available_source_resolutions_ms": sorted(
                        source_resolutions.get(symbol, set())
                    ),
                    "available_expirations_seconds": sorted(
                        {
                            expiry
                            for interval in interval_coverage.get(symbol, set())
                            for expiry in (5, 10, 15, 20, 25, 30, 60, 120, 180, 240, 300, 600, 900)
                            if expiry >= interval and expiry % interval == 0
                        }
                    ),
                    "ticks_count": tick_coverage.get(symbol, {}).get("ticks_count", 0),
                    "tick_timestamp_resolution_ms": 1
                    if symbol in tick_coverage
                    else None,
                    "first_tick_timestamp_ms": tick_coverage.get(symbol, {}).get(
                        "first_tick_timestamp_ms"
                    ),
                    "last_tick_timestamp_ms": tick_coverage.get(symbol, {}).get(
                        "last_tick_timestamp_ms"
                    ),
                }
                for symbol in DUKASCOPY_INSTRUMENTS
            ],
        }

    @application.get("/api/v1/history/reference/instruments")
    def reference_instruments(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, object]:
        coverage = {
            f'{row["base_currency"]}/{row["quote_currency"]}': dict(row)
            for row in connection.execute(
                """
                SELECT base_currency, quote_currency,
                    MIN(rate_date) AS first_date,
                    MAX(rate_date) AS last_date,
                    COUNT(*) AS observations
                FROM reference_rates
                WHERE provider = ?
                GROUP BY base_currency, quote_currency
                """,
                (REFERENCE_PROVIDER,),
            ).fetchall()
        }
        return {
            "provider": REFERENCE_PROVIDER,
            "resolution_seconds": 86400,
            "data_kind": "DAILY_REFERENCE_RATE",
            "instruments": [
                {
                    "symbol": symbol,
                    "first_date": coverage.get(symbol, {}).get("first_date"),
                    "last_date": coverage.get(symbol, {}).get("last_date"),
                    "observations": coverage.get(symbol, {}).get("observations", 0),
                }
                for symbol in REFERENCE_PAIRS
            ],
        }

    @application.get("/api/v1/history/reference")
    def reference_history(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        symbol: str = Query(min_length=7, max_length=7),
        start_date: Date | None = None,
        end_date: Date | None = None,
        limit: int = Query(default=5000, ge=1, le=10000),
    ) -> dict[str, object]:
        normalized_symbol = symbol.upper()
        if normalized_symbol not in REFERENCE_PAIRS:
            raise HTTPException(status_code=422, detail="Unsupported reference-rate pair")
        query = """
            SELECT rate_date, rate
            FROM reference_rates
            WHERE provider = ? AND base_currency = ? AND quote_currency = ?
        """
        parameters: list[object] = [
            REFERENCE_PROVIDER,
            *normalized_symbol.split("/"),
        ]
        if start_date is not None:
            query += " AND rate_date >= ?"
            parameters.append(start_date.isoformat())
        if end_date is not None:
            query += " AND rate_date <= ?"
            parameters.append(end_date.isoformat())
        query += " ORDER BY rate_date DESC LIMIT ?"
        parameters.append(limit)
        rows = connection.execute(query, parameters).fetchall()
        return {
            "provider": REFERENCE_PROVIDER,
            "symbol": normalized_symbol,
            "resolution_seconds": 86400,
            "data_kind": "DAILY_REFERENCE_RATE",
            "rates": [
                {"date": row["rate_date"], "rate": row["rate"]}
                for row in reversed(rows)
            ],
        }

    @application.get("/api/v1/history/dukascopy/ticks")
    def dukascopy_ticks(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        symbol: str = Query(min_length=1, max_length=24),
        from_ms: int | None = Query(default=None, ge=0),
        to_ms: int | None = Query(default=None, ge=0),
        limit: int = Query(default=5000, ge=1, le=50000),
    ) -> dict[str, object]:
        normalized_symbol = symbol.upper()
        if normalized_symbol not in DUKASCOPY_INSTRUMENTS:
            raise HTTPException(status_code=422, detail="Unsupported Dukascopy instrument")
        if from_ms is not None and to_ms is not None and from_ms > to_ms:
            raise HTTPException(status_code=422, detail="from_ms must not exceed to_ms")
        query = """
            SELECT timestamp_ms, timestamp_resolution_ms, bid, ask
            FROM market_ticks
            WHERE provider = ? AND symbol = ?
        """
        parameters: list[object] = [DUKASCOPY_PROVIDER, normalized_symbol]
        if from_ms is not None:
            query += " AND timestamp_ms >= ?"
            parameters.append(from_ms)
        if to_ms is not None:
            query += " AND timestamp_ms <= ?"
            parameters.append(to_ms)
        query += " ORDER BY timestamp_ms ASC, tick_sequence ASC LIMIT ?"
        parameters.append(limit)
        rows = connection.execute(query, parameters).fetchall()
        return {
            "provider": DUKASCOPY_PROVIDER,
            "symbol": normalized_symbol,
            "data_kind": "HISTORICAL_TICKS",
            "timestamp_resolution_ms": 1,
            "ticks": [
                {
                    "timestamp_ms": row["timestamp_ms"],
                    "timestamp_resolution_ms": row["timestamp_resolution_ms"],
                    "bid": row["bid"],
                    "ask": row["ask"],
                }
                for row in rows
            ],
        }

    @application.get("/api/v1/sources/status")
    def source_status(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        symbol: str | None = Query(default=None, min_length=1, max_length=24),
    ) -> dict[str, object]:
        now_ms = int(time.time() * 1000)
        try:
            normalized_symbol = normalize_symbol(symbol) if symbol else None
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        online_parameters: list[object] = [PROVIDER_NAME]
        online_filter = ""
        if normalized_symbol is not None:
            online_filter = " AND symbol = ?"
            online_parameters.append(normalized_symbol)
        latest_tick = connection.execute(
            f"""
            SELECT symbol, timestamp_ms FROM ticks
            WHERE provider = ?{online_filter}
            ORDER BY timestamp_ms DESC LIMIT 1
            """,
            online_parameters,
        ).fetchone()
        key_configured = bool(os.getenv("TWELVE_DATA_API_KEY"))
        age_ms = (
            max(0, now_ms - latest_tick["timestamp_ms"])
            if latest_tick is not None
            else None
        )
        online_ready = bool(key_configured and age_ms is not None and age_ms <= 2000)
        reference_latest = connection.execute(
            """
            SELECT MAX(rate_date) AS last_date, MAX(retrieved_at_ms) AS retrieved_at_ms,
                COUNT(*) AS observations
            FROM reference_rates WHERE provider = ?
            """,
            (REFERENCE_PROVIDER,),
        ).fetchone()
        sync_status = [
            {
                "provider": row["provider"],
                "symbol": row["symbol"],
                "status": row["status"],
                "updated_at_ms": row["updated_at_ms"],
                "last_success_at_ms": row["last_success_at_ms"],
                "error_message": row["error_message"],
            }
            for row in connection.execute(
                """
                SELECT provider, symbol, status, updated_at_ms,
                    last_success_at_ms, error_message
                FROM public_history_sync
                ORDER BY provider, symbol
                """
            ).fetchall()
        ]
        dukascopy_sync = [
            item for item in sync_status if item["provider"] == DUKASCOPY_PROVIDER
        ]
        dukascopy_sync_ready = any(
            item["last_success_at_ms"] is not None for item in dukascopy_sync
        )
        dukascopy_sync_loading = any(
            item["status"] == "loading" for item in dukascopy_sync
        )
        dukascopy_sync_errors = [
            item["error_message"]
            for item in dukascopy_sync
            if item["error_message"] is not None
        ]
        histdata_sync = [
            item for item in sync_status if item["provider"] == HISTDATA_PROVIDER
        ]
        histdata_candles = connection.execute(
            """
            SELECT COUNT(*) AS candles_count, MIN(timestamp_ms) AS first_timestamp_ms,
                MAX(timestamp_ms) AS last_timestamp_ms
            FROM candles
            WHERE provider = ? AND data_kind = 'HISTORICAL'
                AND (? IS NULL OR symbol = ?)
            """,
            (HISTDATA_PROVIDER, normalized_symbol, normalized_symbol),
        ).fetchone()
        if histdata_candles["candles_count"]:
            histdata_status = "ready"
        elif any(item["status"] == "loading" for item in histdata_sync):
            histdata_status = "loading"
        elif any(item["error_message"] for item in histdata_sync):
            histdata_status = "unavailable"
        else:
            histdata_status = "not_yet_checked"
        if dukascopy_sync_ready:
            dukascopy_status = "ready"
        elif dukascopy_sync_loading:
            dukascopy_status = "loading"
        elif any(
            "429" in message or "rate limit" in message.lower()
            for message in dukascopy_sync_errors
        ):
            dukascopy_status = "rate_limited"
        elif dukascopy_sync_errors:
            dukascopy_status = "unavailable"
        else:
            dukascopy_status = "not_yet_checked"
        return {
            "server_time_ms": now_ms,
            "online": {
                "provider": PROVIDER_NAME,
                "status": "ready"
                if online_ready
                else "not_configured"
                if not key_configured
                else "stale_or_unavailable",
                "ready": online_ready,
                "reason": None
                if online_ready
                else "Twelve Data API key is not configured on the backend."
                if not key_configured
                else "No quote newer than two seconds is available from the live feed.",
                "last_quote_timestamp_ms": (
                    latest_tick["timestamp_ms"] if latest_tick is not None else None
                ),
                "quote_age_ms": age_ms,
                "update_frequency": "provider-driven WebSocket; no keyless live source verified",
            },
            "historical": {
                "histdata_provider": HISTDATA_PROVIDER,
                "histdata_status": histdata_status,
                "histdata_resolution_seconds": 60,
                "histdata_candles": histdata_candles["candles_count"],
                "histdata_first_timestamp_ms": histdata_candles["first_timestamp_ms"],
                "histdata_last_timestamp_ms": histdata_candles["last_timestamp_ms"],
                "dukascopy_provider": DUKASCOPY_PROVIDER,
                "dukascopy_status": dukascopy_status,
                "reference_provider": REFERENCE_PROVIDER,
                "reference_resolution_seconds": 86400,
                "reference_last_date": reference_latest["last_date"],
                "reference_retrieved_at_ms": reference_latest["retrieved_at_ms"],
                "reference_observations": reference_latest["observations"],
                "reference_status": "daily_reference_only_not_intraday",
                "sync_status": sync_status,
            },
        }

    @application.post("/api/v1/history/dukascopy/import")
    async def import_dukascopy_history(
        symbol: str = Query(min_length=1, max_length=24),
        trading_date: Date = Query(alias="date"),
        start_hour: int = Query(default=0, ge=0, le=23),
        end_hour: int = Query(default=24, ge=1, le=24),
        interval_seconds: int = Query(default=60, ge=1, le=3600),
    ) -> dict[str, int | str]:
        normalized_symbol = symbol.upper()
        if normalized_symbol not in DUKASCOPY_INSTRUMENTS:
            raise HTTPException(status_code=422, detail="Unsupported Dukascopy instrument")
        if start_hour >= end_hour:
            raise HTTPException(
                status_code=422, detail="start_hour must be less than end_hour"
            )
        if end_hour - start_hour > MAX_HOURS_PER_REQUEST:
            raise HTTPException(
                status_code=422,
                detail=f"Import at most {MAX_HOURS_PER_REQUEST} UTC hours per request",
            )
        now = datetime.now(timezone.utc)
        if trading_date > now.date() or (
            trading_date == now.date() and end_hour > now.hour
        ):
            raise HTTPException(
                status_code=422, detail="Historical import cannot include future hours"
            )

        try:
            async with DukascopyClient() as provider:
                batch = await provider.fetch_candles(
                    normalized_symbol,
                    trading_date,
                    start_hour=start_hour,
                    end_hour=end_hour,
                    interval_seconds=interval_seconds,
                )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except MarketDataError as error:
            raise HTTPException(
                status_code=502, detail=f"Dukascopy historical data error: {error}"
            ) from error

        imported, ticks_imported = _persist_dukascopy_batch(
            resolved_database_path,
            batch,
            interval_seconds=interval_seconds,
        )

        return {
            "provider": DUKASCOPY_PROVIDER,
            "symbol": normalized_symbol,
            "date": batch.date.isoformat(),
            "start_hour": batch.start_hour,
            "end_hour": batch.end_hour,
            "interval_seconds": interval_seconds,
            "source_resolution_ms": 1,
            "files_requested": batch.files_requested,
            "files_missing": batch.files_missing,
            "ticks_received": batch.ticks_received,
            "ticks_imported": ticks_imported,
            "candles_received": len(batch.candles),
            "candles_imported": imported,
            "missing_intervals": batch.missing_intervals,
        }

    @application.post("/api/v1/provider/twelvedata/catalog")
    async def refresh_twelvedata_catalog(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, int | str]:
        try:
            async with TwelveDataClient() as provider:
                instruments = await provider.forex_pairs()
        except MarketDataError as error:
            raise _provider_http_error(error) from error
        if not instruments:
            raise HTTPException(
                status_code=502,
                detail="Twelve Data returned an empty Forex catalog; existing catalog was preserved",
            )

        updated_at_ms = int(time.time() * 1000)
        with connection:
            connection.execute(
                "DELETE FROM market_instruments WHERE provider = ? AND market_type = 'CLASSIC'",
                (PROVIDER_NAME,),
            )
            cursor = connection.executemany(
                """
                INSERT INTO market_instruments (
                    provider, market_type, symbol, display_name, updated_at_ms
                ) VALUES (?, 'CLASSIC', ?, ?, ?)
                """,
                [
                    (
                        instrument["provider"],
                        instrument["symbol"],
                        instrument["display_name"],
                        updated_at_ms,
                    )
                    for instrument in instruments
                ],
            )
        return {"provider": PROVIDER_NAME, "instruments_updated": len(instruments)}

    @application.get("/api/v1/candles", response_model=CandleSeriesResponse)
    def candles(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        symbol: str = Query(min_length=1, max_length=24),
        interval_seconds: int = Query(gt=0, le=86400),
        data_kind: str = Query(pattern="^(HISTORICAL|LIVE)$"),
        provider: str = Query(min_length=1, max_length=80),
        limit: int = Query(default=500, ge=1, le=5000),
    ) -> CandleSeriesResponse:
        try:
            normalized_symbol = normalize_symbol(symbol)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        rows = connection.execute(
            """
            SELECT timestamp_ms, source_resolution_ms, open, high, low, close, volume
            FROM candles
            WHERE provider = ? AND symbol = ? AND interval_seconds = ?
                AND data_kind = ?
            ORDER BY timestamp_ms DESC
            LIMIT ?
            """,
            (provider, normalized_symbol, interval_seconds, data_kind, limit),
        ).fetchall()
        return CandleSeriesResponse(
            provider=provider,
            market_type="CLASSIC",
            symbol=normalized_symbol,
            interval_seconds=interval_seconds,
            data_kind=data_kind,
            candles=[CandleResponse.model_validate(dict(row)) for row in reversed(rows)],
        )

    @application.post("/api/v1/history/import")
    def import_history(
        request: HistoricalImportRequest,
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, int]:
        try:
            symbol = normalize_symbol(request.symbol)
            report = import_historical_csv(
                connection,
                request.csv,
                provider=request.provider,
                symbol=symbol,
                interval_seconds=request.interval_seconds,
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return {
            "rows_read": report.rows_read,
            "rows_imported": report.rows_imported,
            "invalid_rows": report.invalid_rows,
            "duplicate_rows": report.duplicate_rows,
            "missing_intervals": report.missing_intervals,
            "interval_seconds": report.interval_seconds,
        }

    @application.post("/api/v1/history/twelvedata")
    async def fetch_twelvedata_history(
        symbol: str = Query(min_length=1, max_length=24),
        interval_seconds: int = Query(ge=60, le=86400),
        outputsize: int = Query(default=500, ge=1, le=5000),
    ) -> dict[str, int | str]:
        try:
            normalized_symbol = normalize_symbol(symbol)
            if interval_seconds not in INTERVALS_BY_SECONDS:
                raise ValueError("Unsupported Twelve Data historical interval")
            async with TwelveDataClient() as provider:
                history = await provider.time_series(
                    normalized_symbol, interval_seconds, outputsize
                )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except MarketDataError as error:
            raise _provider_http_error(error) from error
        if not history:
            raise HTTPException(status_code=502, detail="Provider returned no historical candles")

        history_connection = connect(resolved_database_path)
        try:
            with history_connection:
                imported = save_historical_candles(
                    history_connection,
                    history,
                    provider=PROVIDER_NAME,
                    symbol=normalized_symbol,
                    interval_seconds=interval_seconds,
                    source_resolution_ms=interval_seconds * 1000,
                )
        finally:
            history_connection.close()
        return {
            "provider": PROVIDER_NAME,
            "symbol": normalized_symbol,
            "rows_received": len(history),
            "rows_imported": imported,
        }

    @application.post("/api/v1/sessions", status_code=201)
    def create_session(
        request: SessionRequest,
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, int | str]:
        started_at_ms = request.started_at_ms or int(time.time() * 1000)
        if started_at_ms > int(time.time() * 1000) + 60_000:
            raise HTTPException(status_code=422, detail="Session start cannot be in the future")
        active = connection.execute(
            "SELECT id FROM trading_sessions WHERE ended_at_ms IS NULL LIMIT 1"
        ).fetchone()
        if active is not None:
            raise HTTPException(
                status_code=409,
                detail={"message": "A trading session is already active", "id": active["id"]},
            )
        session_id = str(uuid.uuid4())
        with connection:
            connection.execute(
                "INSERT INTO trading_sessions (id, started_at_ms) VALUES (?, ?)",
                (session_id, started_at_ms),
            )
        return {"id": session_id, "started_at_ms": started_at_ms}

    @application.get("/api/v1/sessions")
    def list_sessions(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        limit: int = Query(default=50, ge=1, le=500),
    ) -> list[dict[str, int | str | None]]:
        rows = connection.execute(
            """
            SELECT s.id, s.started_at_ms, s.ended_at_ms, COUNT(g.id) AS signal_count
            FROM trading_sessions s
            LEFT JOIN signals g ON g.session_id = s.id
            GROUP BY s.id
            ORDER BY s.started_at_ms DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    @application.post("/api/v1/sessions/{session_id}/finish")
    def finish_session(
        session_id: str,
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, int | str]:
        now_ms = int(time.time() * 1000)
        with connection:
            cursor = connection.execute(
                """
                UPDATE trading_sessions SET ended_at_ms = ?
                WHERE id = ? AND ended_at_ms IS NULL
                """,
                (now_ms, session_id),
            )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Active session was not found")
        return {"id": session_id, "ended_at_ms": now_ms}

    @application.get("/api/v1/signals")
    def list_signals(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        session_id: str | None = None,
        outcome: str | None = Query(
            default=None, pattern="^(PENDING|WON|LOST|MISSED)$"
        ),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        parameters: list[object] = []
        if session_id is not None:
            clauses.append("session_id = ?")
            parameters.append(session_id)
        if outcome is not None:
            clauses.append("outcome = ?")
            parameters.append(outcome)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(limit)
        rows = connection.execute(
            f"""
            SELECT id, session_id, provider, market_type, symbol, direction,
                expiry_seconds, issued_at_ms, recommended_entry_at_ms, expires_at_ms,
                quote_timestamp_ms, quote_latency_ms, probability_up, probability_down,
                analytics_json, explanation, outcome, outcome_recorded_at_ms
            FROM signals {where}
            ORDER BY issued_at_ms DESC LIMIT ?
            """,
            parameters,
        ).fetchall()
        results: list[dict[str, object]] = []
        for row in rows:
            result = dict(row)
            result["analytics"] = json.loads(result.pop("analytics_json"))
            results.append(result)
        return results

    @application.patch("/api/v1/signals/{signal_id}/outcome")
    def record_signal_outcome(
        signal_id: str,
        request: OutcomeRequest,
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, str | int]:
        signal = connection.execute(
            "SELECT direction, outcome FROM signals WHERE id = ?", (signal_id,)
        ).fetchone()
        if signal is None:
            raise HTTPException(status_code=404, detail="Signal was not found")
        if signal["direction"] == "NO_SIGNAL":
            raise HTTPException(
                status_code=409, detail="A NO_SIGNAL analysis cannot be scored as a trade"
            )
        if signal["outcome"] != "PENDING":
            raise HTTPException(status_code=409, detail="Signal outcome is already recorded")
        recorded_at_ms = int(time.time() * 1000)
        with connection:
            connection.execute(
                """
                UPDATE signals SET outcome = ?, outcome_recorded_at_ms = ?
                WHERE id = ? AND outcome = 'PENDING'
                """,
                (request.outcome, recorded_at_ms, signal_id),
            )
        return {
            "id": signal_id,
            "outcome": request.outcome,
            "outcome_recorded_at_ms": recorded_at_ms,
        }

    @application.get("/api/v1/statistics")
    def statistics_summary(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        period: str = Query(default="all", pattern="^(hour|day|week|all)$"),
        session_id: str | None = None,
    ) -> dict[str, object]:
        period_ms = {"hour": 3_600_000, "day": 86_400_000, "week": 604_800_000}
        now_ms = int(time.time() * 1000)
        clauses: list[str] = []
        parameters: list[object] = []
        if period != "all":
            clauses.append("issued_at_ms >= ?")
            parameters.append(now_ms - period_ms[period])
        if session_id is not None:
            clauses.append("session_id = ?")
            parameters.append(session_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = connection.execute(
            f"""
            SELECT market_type, symbol, expiry_seconds, outcome, COUNT(*) AS count
            FROM signals {where}
            GROUP BY market_type, symbol, expiry_seconds, outcome
            """,
            parameters,
        ).fetchall()
        total = won = lost = missed = 0
        by_pair: dict[str, dict[str, int]] = {}
        by_expiry: dict[str, dict[str, int]] = {}
        by_market: dict[str, dict[str, int]] = {}
        for row in rows:
            count = row["count"]
            outcome = row["outcome"]
            total += count
            won += count if outcome == "WON" else 0
            lost += count if outcome == "LOST" else 0
            missed += count if outcome == "MISSED" else 0
            for bucket, key in (
                (by_pair, row["symbol"]),
                (by_expiry, str(row["expiry_seconds"])),
                (by_market, row["market_type"]),
            ):
                group = bucket.setdefault(key, {"total": 0, "won": 0, "lost": 0, "missed": 0})
                group["total"] += count
                if outcome in ("WON", "LOST", "MISSED"):
                    group[outcome.lower()] += count
        decided = won + lost
        session_duration_ms: int | None = None
        if session_id is not None:
            session = connection.execute(
                """
                SELECT started_at_ms, COALESCE(ended_at_ms, ?) AS ended_at_ms
                FROM trading_sessions WHERE id = ?
                """,
                (now_ms, session_id),
            ).fetchone()
            if session is not None:
                session_duration_ms = max(
                    0, session["ended_at_ms"] - session["started_at_ms"]
                )
        return {
            "period": period,
            "session_id": session_id,
            "session_duration_ms": session_duration_ms,
            "total_signals": total,
            "wins": won,
            "losses": lost,
            "missed": missed,
            "pending": total - won - lost - missed,
            "win_rate_percent": round(won * 100 / decided, 2) if decided else None,
            "by_pair": by_pair,
            "by_expiry_seconds": by_expiry,
            "by_market": by_market,
        }

    @application.post("/api/v1/models/train")
    def train_signal_model(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> dict[str, object]:
        latest_version = connection.execute(
            """
            SELECT id, created_at_ms
            FROM model_versions ORDER BY created_at_ms DESC LIMIT 1
            """
        ).fetchone()
        active = connection.execute(
            """
            SELECT id, created_at_ms, metrics_json
            FROM model_versions WHERE active = 1
            ORDER BY created_at_ms DESC LIMIT 1
            """
        ).fetchone()
        records = connection.execute(
            """
            SELECT issued_at_ms, direction, expiry_seconds, outcome,
                analytics_json, market_snapshot_json
            FROM signals
            WHERE outcome IN ('WON', 'LOST')
            ORDER BY issued_at_ms ASC
            """
        ).fetchall()
        if latest_version is not None:
            new_labels = sum(
                record["issued_at_ms"] > latest_version["created_at_ms"]
                for record in records
            )
            if new_labels < 25:
                return {
                    "eligible": False,
                    "active": False,
                    "reason": "At least 25 new labeled outcomes are required before retraining.",
                    "new_labeled_outcomes": new_labels,
                    "latest_model_version_id": latest_version["id"],
                }

        result = train_model(records)
        if result.artifact is None:
            return {
                "eligible": False,
                "active": False,
                "reason": result.reason,
                "training_samples": result.training_samples,
                "validation_samples": result.validation_samples,
            }

        comparison: dict[str, object] | None = None
        activate = True
        if active is not None:
            active_payload = json.loads(active["metrics_json"])
            active_artifact = active_payload.get("artifact")
            if isinstance(active_artifact, dict):
                old_metrics = evaluate_artifact(
                    active_artifact, result.validation_examples
                )
                comparison = {
                    "previous_model_id": active["id"],
                    "previous_model_metrics_on_current_holdout": old_metrics,
                    "candidate_model_metrics_on_current_holdout": {
                        "accuracy": result.metrics["accuracy"],
                        "brier_score": result.metrics["brier_score"],
                    },
                }
                activate = (
                    result.metrics["accuracy"] >= old_metrics["accuracy"]
                    and result.metrics["brier_score"] <= old_metrics["brier_score"]
                    and (
                        result.metrics["accuracy"] > old_metrics["accuracy"]
                        or result.metrics["brier_score"] < old_metrics["brier_score"]
                    )
                )

        model_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        metrics_payload = {
            "validated": bool(result.eligible and activate),
            "metrics": result.metrics,
            "artifact": result.artifact,
            "comparison": comparison,
            "reason": result.reason,
        }
        with connection:
            if result.eligible and activate:
                connection.execute("UPDATE model_versions SET active = 0")
            connection.execute(
                """
                INSERT INTO model_versions (
                    id, created_at_ms, training_samples, validation_samples,
                    metrics_json, active
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    model_id,
                    now_ms,
                    result.training_samples,
                    result.validation_samples,
                    json.dumps(metrics_payload),
                    int(result.eligible and activate),
                ),
            )
        return {
            "model_id": model_id,
            "eligible": result.eligible,
            "active": bool(result.eligible and activate),
            "reason": (
                "Model passed the chronological holdout and version comparison."
                if result.eligible and activate
                else result.reason
                if not result.eligible
                else "Candidate did not improve the active model on the same holdout."
            ),
            "training_samples": result.training_samples,
            "validation_samples": result.validation_samples,
            "metrics": result.metrics,
            "comparison": comparison,
        }

    @application.get("/api/v1/models")
    def list_models(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        limit: int = Query(default=20, ge=1, le=100),
    ) -> list[dict[str, object]]:
        rows = connection.execute(
            """
            SELECT id, created_at_ms, training_samples, validation_samples,
                metrics_json, active
            FROM model_versions
            ORDER BY created_at_ms DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        versions: list[dict[str, object]] = []
        for row in rows:
            metrics = json.loads(row["metrics_json"])
            versions.append(
                {
                    "id": row["id"],
                    "created_at_ms": row["created_at_ms"],
                    "training_samples": row["training_samples"],
                    "validation_samples": row["validation_samples"],
                    "active": bool(row["active"]),
                    "metrics": metrics.get("metrics", {}),
                    "comparison": metrics.get("comparison"),
                }
            )
        return versions

    @application.post("/api/v1/analyze", response_model=AnalysisResponse)
    def analyze_market(
        request: AnalysisRequest,
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
    ) -> AnalysisResponse:
        analysis_started = time.perf_counter()
        if request.expiry_seconds not in (
            3,
            5,
            10,
            15,
            20,
            25,
            30,
            60,
            120,
            180,
            240,
            300,
            600,
            900,
        ):
            raise HTTPException(status_code=422, detail="Unsupported expiration")
        if request.market_type == "OTC":
            raise HTTPException(
                status_code=409,
                detail="Pocket Option OTC data is unavailable; third-party Forex quotes are not OTC",
            )
        try:
            symbol = normalize_symbol(request.symbol)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

        if request.session_id is not None:
            session = connection.execute(
                "SELECT id FROM trading_sessions WHERE id = ? AND ended_at_ms IS NULL",
                (request.session_id,),
            ).fetchone()
            if session is None:
                raise HTTPException(status_code=404, detail="Active session was not found")

        now_ms = int(time.time() * 1000)
        instrument = connection.execute(
            """
            SELECT 1 FROM market_instruments
            WHERE provider = ? AND market_type = 'CLASSIC'
                AND symbol = ? AND enabled = 1
            """,
            (PROVIDER_NAME, symbol),
        ).fetchone()
        if instrument is None:
            raise HTTPException(
                status_code=404,
                detail="Instrument is not in the refreshed CLASSIC provider catalog",
            )
        latest_tick = connection.execute(
            """
            SELECT timestamp_ms FROM ticks
            WHERE provider = ? AND symbol = ?
            ORDER BY timestamp_ms DESC LIMIT 1
            """,
            (PROVIDER_NAME, symbol),
        ).fetchone()
        quote_timestamp = latest_tick["timestamp_ms"] if latest_tick else None
        quote_latency = max(0, now_ms - quote_timestamp) if quote_timestamp else None
        quote_is_future = quote_timestamp is not None and quote_timestamp > now_ms + 2000
        tick_window_ms = max(5000, request.expiry_seconds * 1000)
        recent_tick_count = connection.execute(
            """
            SELECT COUNT(*) FROM ticks
            WHERE provider = ? AND symbol = ? AND timestamp_ms >= ?
            """,
            (PROVIDER_NAME, symbol, now_ms - tick_window_ms),
        ).fetchone()[0]
        candles_rows = connection.execute(
            """
            SELECT timestamp_ms, open, high, low, close, volume
            FROM candles
            WHERE provider = ? AND symbol = ? AND interval_seconds = 1
                AND data_kind = 'LIVE'
            ORDER BY timestamp_ms DESC LIMIT 500
            """,
            (PROVIDER_NAME, symbol),
        ).fetchall()
        candles = [
            Candle(
                timestamp_ms=row["timestamp_ms"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
            )
            for row in reversed(candles_rows)
        ]
        technical = analyze_candles(candles)
        direction = "NO_SIGNAL"
        explanation = technical.explanation
        probability_up = probability_down = None
        model_version_id: str | None = None
        if quote_latency is None or quote_latency > 2000 or quote_is_future:
            explanation = "Quote is unavailable or stale; no signal was issued."
        elif recent_tick_count < max(3, math.ceil(request.expiry_seconds / 2)):
            explanation = (
                "The provider has not delivered enough recent ticks for this "
                "expiration; no signal was issued."
            )
        elif technical.candidate_direction != "NO_SIGNAL":
            model = connection.execute(
                """
                SELECT id, metrics_json
                FROM model_versions WHERE active = 1
                ORDER BY created_at_ms DESC LIMIT 1
                """
            ).fetchone()
            if model is None:
                explanation = (
                    f"Technical candidate {technical.candidate_direction}, but no validated "
                    "out-of-sample probability model is active; no trade signal was issued."
                )
            else:
                model_payload = json.loads(model["metrics_json"])
                if not model_payload.get("validated"):
                    explanation = (
                        "The active model has no valid holdout; no trade signal was issued."
                    )
                else:
                    probability_win = predict_win_probability(
                        model_payload["artifact"],
                        technical.features,
                        {"latest_price": candles[-1].close if candles else None},
                        technical.candidate_direction,
                        request.expiry_seconds,
                    )
                    model_version_id = model["id"]
                    if technical.candidate_direction == "UP":
                        probability_up = round(probability_win * 100, 2)
                        probability_down = round((1 - probability_win) * 100, 2)
                    else:
                        probability_down = round(probability_win * 100, 2)
                        probability_up = round((1 - probability_win) * 100, 2)
                    if probability_win >= 0.5:
                        direction = technical.candidate_direction
                        explanation = (
                            f"Validated model supports {direction}; {technical.explanation}"
                        )
                    else:
                        explanation = (
                            "The validated model found no positive edge; "
                            f"{technical.explanation}"
                        )

        signal_id = str(uuid.uuid4())
        expires_at_ms = now_ms + request.expiry_seconds * 1000
        recommended_entry_at_ms = now_ms if direction in ("UP", "DOWN") else None
        with connection:
            connection.execute(
                """
                INSERT INTO signals (
                    id, session_id, provider, market_type, symbol, direction,
                    expiry_seconds, issued_at_ms, recommended_entry_at_ms,
                    expires_at_ms, quote_timestamp_ms,
                    quote_latency_ms, probability_up, probability_down,
                    market_snapshot_json, analytics_json, explanation, model_version_id
                ) VALUES (?, ?, ?, 'CLASSIC', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    request.session_id,
                    PROVIDER_NAME,
                    symbol,
                    direction,
                    request.expiry_seconds,
                    now_ms,
                    recommended_entry_at_ms,
                    expires_at_ms,
                    quote_timestamp,
                    quote_latency,
                    probability_up,
                    probability_down,
                    json.dumps(
                        {
                            "quote_timestamp_ms": quote_timestamp,
                            "quote_latency_ms": quote_latency,
                            "latest_price": candles[-1].close if candles else None,
                        }
                    ),
                    json.dumps(
                        {
                            **technical.features,
                            "candidate_direction": technical.candidate_direction,
                            "score": technical.score,
                        }
                    ),
                    explanation,
                    model_version_id,
                ),
            )
        return AnalysisResponse(
            id=signal_id,
            symbol=symbol,
            provider=PROVIDER_NAME,
            market_type="CLASSIC",
            direction=direction,
            candidate_direction=technical.candidate_direction,
            expiry_seconds=request.expiry_seconds,
            issued_at_ms=now_ms,
            recommended_entry_at_ms=recommended_entry_at_ms,
            expires_at_ms=expires_at_ms,
            quote_timestamp_ms=quote_timestamp,
            quote_latency_ms=quote_latency,
            probability_up=probability_up,
            probability_down=probability_down,
            analytics=technical.features,
            explanation=explanation,
            analysis_latency_ms=int((time.perf_counter() - analysis_started) * 1000),
        )

    @application.get("/api/v1/backtest")
    def backtest(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        symbol: str = Query(min_length=1, max_length=24),
        provider: str = Query(min_length=1, max_length=80),
        expiry_seconds: int = Query(ge=1, le=900),
    ) -> dict[str, int | float | str | None]:
        try:
            normalized_symbol = normalize_symbol(symbol)
            rows = connection.execute(
                """
                SELECT timestamp_ms, open, high, low, close, volume
                FROM candles
                WHERE provider = ? AND symbol = ? AND interval_seconds = 60
                    AND data_kind = 'HISTORICAL'
                ORDER BY timestamp_ms ASC
                """,
                (provider, normalized_symbol),
            ).fetchall()
            result = run_backtest(
                [
                    Candle(
                        timestamp_ms=row["timestamp_ms"],
                        open=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                    )
                    for row in rows
                ],
                provider=provider,
                symbol=normalized_symbol,
                candle_interval_seconds=60,
                expiry_seconds=expiry_seconds,
            )
        except BacktestError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return {
            "provider": result.provider,
            "symbol": result.symbol,
            "candle_interval_seconds": result.candle_interval_seconds,
            "expiry_seconds": result.expiry_seconds,
            "sample_count": result.sample_count,
            "trade_count": result.trade_count,
            "wins": result.wins,
            "losses": result.losses,
            "coverage_percent": result.coverage_percent,
            "accuracy_percent": result.accuracy_percent,
            "out_of_sample_start_ms": result.out_of_sample_start_ms,
            "evaluation": "out-of-sample; minute candles; not seconds validation",
        }

    @application.get("/api/v1/backtest/history")
    @application.get("/api/v1/backtest/dukascopy")
    def backtest_dukascopy(
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
        symbol: str = Query(min_length=1, max_length=24),
        period_days: int = Query(default=1, ge=1, le=3650),
        expiry_seconds: list[int] = Query(default=[60, 300, 900]),
        availability_only: bool = Query(default=False),
    ) -> dict[str, object]:
        normalized_symbol = symbol.upper()
        if normalized_symbol not in DUKASCOPY_INSTRUMENTS:
            raise HTTPException(status_code=422, detail="Unsupported Dukascopy symbol")
        supported_expiries = {5, 10, 15, 20, 25, 30, 60, 120, 180, 240, 300, 600, 900}
        if (
            not expiry_seconds
            or len(expiry_seconds) > 3
            or any(expiry not in supported_expiries for expiry in expiry_seconds)
        ):
            raise HTTPException(
                status_code=422,
                detail="Select one to three supported historical expirations",
            )

        now = datetime.now(timezone.utc)
        latest_history = connection.execute(
            """
            SELECT MAX(timestamp_ms) AS latest_timestamp_ms
            FROM candles
            WHERE provider IN (?, ?) AND symbol = ? AND data_kind = 'HISTORICAL'
            """,
            (DUKASCOPY_PROVIDER, HISTDATA_PROVIDER, normalized_symbol),
        ).fetchone()["latest_timestamp_ms"]
        period_end_date = (
            datetime.fromtimestamp(latest_history / 1000, tz=timezone.utc).date()
            if latest_history is not None
            else now.date()
        )
        period_start = datetime.combine(
            period_end_date - timedelta(days=period_days - 1),
            DateTime.min,
            tzinfo=timezone.utc,
        )
        start_ms = int(period_start.timestamp() * 1000)
        end_ms = min(
            int(now.timestamp() * 1000),
            int(latest_history + 60_000)
            if latest_history is not None
            else int(now.timestamp() * 1000),
        )
        source_intervals = {
            (row["provider"], row["interval_seconds"])
            for row in connection.execute(
                """
                SELECT DISTINCT provider, interval_seconds
                FROM candles
                WHERE provider IN (?, ?) AND symbol = ? AND data_kind = 'HISTORICAL'
                    AND timestamp_ms >= ? AND timestamp_ms <= ?
                """,
                (
                    DUKASCOPY_PROVIDER,
                    HISTDATA_PROVIDER,
                    normalized_symbol,
                    start_ms,
                    end_ms,
                ),
            )
        }
        if (DUKASCOPY_PROVIDER, 1) in source_intervals:
            provider = DUKASCOPY_PROVIDER
        elif (HISTDATA_PROVIDER, 60) in source_intervals:
            provider = HISTDATA_PROVIDER
        else:
            provider = DUKASCOPY_PROVIDER
        intervals = {
            interval
            for source, interval in source_intervals
            if source == provider
        }
        if availability_only:
            counts_by_interval: dict[str, int] = {}
            gaps_by_interval: dict[str, int] = {}
            first_timestamps: list[int] = []
            last_timestamps: list[int] = []
            for interval in intervals:
                interval_ms = interval * 1000
                row = connection.execute(
                    """
                    SELECT COUNT(*) AS candle_count,
                        MIN(timestamp_ms) AS first_timestamp_ms,
                        MAX(timestamp_ms) AS last_timestamp_ms,
                        COALESCE(SUM(
                            CASE
                                WHEN previous_timestamp_ms IS NOT NULL
                                    AND timestamp_ms - previous_timestamp_ms > ?
                                THEN (timestamp_ms - previous_timestamp_ms) / ? - 1
                                ELSE 0
                            END
                        ), 0) AS missing_intervals
                    FROM (
                        SELECT timestamp_ms,
                            LAG(timestamp_ms) OVER (ORDER BY timestamp_ms)
                                AS previous_timestamp_ms
                        FROM candles
                        WHERE provider = ? AND symbol = ? AND data_kind = 'HISTORICAL'
                            AND interval_seconds = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                    )
                    """,
                    (
                        interval_ms,
                        interval_ms,
                        provider,
                        normalized_symbol,
                        interval,
                        start_ms,
                        end_ms,
                    ),
                ).fetchone()
                counts_by_interval[str(interval)] = row["candle_count"]
                gaps_by_interval[str(interval)] = row["missing_intervals"]
                if row["first_timestamp_ms"] is not None:
                    first_timestamps.append(row["first_timestamp_ms"])
                    last_timestamps.append(row["last_timestamp_ms"])
            return {
                "provider": provider,
                "symbol": normalized_symbol,
                "period_days": period_days,
                "period_start_ms": start_ms,
                "period_end_ms": end_ms,
                "candle_count": counts_by_interval.get(
                    "1" if 1 in intervals else "60", 0
                ),
                "candle_counts_by_interval": counts_by_interval,
                "missing_intervals": gaps_by_interval.get(
                    "1" if 1 in intervals else "60", 0
                ),
                "missing_intervals_by_interval": gaps_by_interval,
                "first_timestamp_ms": min(first_timestamps, default=None),
                "last_timestamp_ms": max(last_timestamps, default=None),
                "available_intervals_seconds": sorted(intervals),
                "available_expirations_seconds": sorted(
                    expiry
                    for expiry in supported_expiries
                    if (1 in intervals and expiry >= 5)
                    or (60 in intervals and expiry >= 60)
                ),
                "cost_adjusted": False,
                "cost_note": (
                    "Bid-only simulation; spread, commissions, and slippage are not included."
                ),
                "results": [],
            }
        counts_by_interval: dict[int, int] = {}
        candles_by_interval: dict[int, list[Candle]] = {}
        gaps_by_interval: dict[int, int] = {}
        for interval in intervals:
            rows = connection.execute(
                """
                SELECT timestamp_ms, open, high, low, close, volume
                FROM candles
                WHERE provider = ? AND symbol = ? AND data_kind = 'HISTORICAL'
                    AND interval_seconds = ? AND timestamp_ms >= ? AND timestamp_ms <= ?
                ORDER BY timestamp_ms ASC
                """,
                (provider, normalized_symbol, interval, start_ms, end_ms),
            ).fetchall()
            candles = [
                Candle(
                    timestamp_ms=row["timestamp_ms"],
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                )
                for row in rows
            ]
            interval_ms = interval * 1000
            counts_by_interval[interval] = len(candles)
            candles_by_interval[interval] = candles
            gaps_by_interval[interval] = sum(
                max(0, (current.timestamp_ms - previous.timestamp_ms) // interval_ms - 1)
                for previous, current in zip(candles, candles[1:])
            )

        results: list[dict[str, object]] = []
        for expiry in sorted(set(expiry_seconds)):
            interval = 1 if 1 in intervals else 60
            if interval == 60 and expiry < 60:
                results.append(
                    {
                        "expiry_seconds": expiry,
                        "resolution_seconds": None,
                        "status": "unsupported_resolution",
                        "sample_count": 0,
                        "trade_count": 0,
                        "wins": 0,
                        "losses": 0,
                        "win_rate_percent": None,
                    }
                )
                continue
            candles = candles_by_interval.get(interval, [])
            try:
                backtest_result = run_backtest(
                    candles,
                    provider=provider,
                    symbol=normalized_symbol,
                    candle_interval_seconds=interval,
                    expiry_seconds=expiry,
                )
            except BacktestError:
                results.append(
                    {
                        "expiry_seconds": expiry,
                        "resolution_seconds": interval,
                        "status": "insufficient_data",
                        "sample_count": 0,
                        "trade_count": 0,
                        "wins": 0,
                        "losses": 0,
                        "win_rate_percent": None,
                    }
                )
                continue
            status = (
                "ok"
                if backtest_result.trade_count
                else "insufficient_signals"
                if backtest_result.sample_count
                else "insufficient_data"
            )
            results.append(
                {
                    "expiry_seconds": expiry,
                    "resolution_seconds": interval,
                    "status": status,
                    "sample_count": backtest_result.sample_count,
                    "trade_count": backtest_result.trade_count,
                    "wins": backtest_result.wins,
                    "losses": backtest_result.losses,
                    "win_rate_percent": (
                        backtest_result.accuracy_percent if status == "ok" else None
                    ),
                }
            )
        return {
            "provider": provider,
            "symbol": normalized_symbol,
            "period_days": period_days,
            "period_start_ms": start_ms,
            "period_end_ms": end_ms,
            "candle_count": counts_by_interval.get(
                1 if 1 in intervals else 60, 0
            ),
            "candle_counts_by_interval": {
                str(interval): count for interval, count in counts_by_interval.items()
            },
            "missing_intervals": gaps_by_interval.get(1 if 1 in intervals else 60, 0),
            "missing_intervals_by_interval": {
                str(interval): count for interval, count in gaps_by_interval.items()
            },
            "first_timestamp_ms": min(
                (candle.timestamp_ms for values in candles_by_interval.values() for candle in values),
                default=None,
            ),
            "last_timestamp_ms": max(
                (candle.timestamp_ms for values in candles_by_interval.values() for candle in values),
                default=None,
            ),
            "available_intervals_seconds": sorted(intervals),
            "available_expirations_seconds": sorted(
                expiry
                for expiry in supported_expiries
                if (1 in intervals and expiry >= 5)
                or (60 in intervals and expiry >= 60)
            ),
            "cost_adjusted": False,
            "cost_note": (
                "Bid-only simulation; spread, commissions, and slippage are not included."
            ),
            "results": results,
        }

    @application.websocket("/api/v1/stream")
    async def stream_twelvedata(websocket: WebSocket, symbol: str) -> None:
        await websocket.accept()
        try:
            normalized_symbol = normalize_symbol(symbol)
        except ValueError:
            await websocket.send_json({"error": "Invalid instrument symbol"})
            await websocket.close(code=1008)
            return

        api_key = os.getenv("TWELVE_DATA_API_KEY")
        if not api_key:
            await websocket.send_json(
                {"error": "Live stream is unavailable: TWELVE_DATA_API_KEY is not configured"}
            )
            await websocket.close(code=1013)
            return

        connection = connect(resolved_database_path)
        try:
            instrument = connection.execute(
                """
                SELECT 1 FROM market_instruments
                WHERE provider = ? AND market_type = 'CLASSIC'
                    AND symbol = ? AND enabled = 1
                """,
                (PROVIDER_NAME, normalized_symbol),
            ).fetchone()
            if instrument is None:
                await websocket.send_json(
                    {"error": "Instrument is not in the refreshed CLASSIC provider catalog"}
                )
                await websocket.close(code=1008)
                return

            async with websockets.connect(
                "wss://ws.twelvedata.com/v1/quotes/price",
                additional_headers={"X-TD-APIKEY": api_key},
                open_timeout=10,
                close_timeout=3,
                ping_interval=20,
            ) as upstream:
                await upstream.send(
                    json.dumps(
                        {
                            "action": "subscribe",
                            "params": {"symbols": normalized_symbol},
                        }
                    )
                )
                while True:
                    message = await upstream.recv()
                    if isinstance(message, str):
                        try:
                            event = json.loads(message)
                        except json.JSONDecodeError:
                            await websocket.send_text(message)
                            continue
                        if isinstance(event, dict) and event.get("event") == "price":
                            _store_live_tick(connection, normalized_symbol, event)
                        await websocket.send_json(event)
                    else:
                        await websocket.send_bytes(message)
        except WebSocketDisconnect:
            pass
        except (
            websockets.WebSocketException,
            OSError,
            sqlite3.Error,
            ValueError,
        ) as error:
            try:
                await websocket.send_json({"error": f"Live market stream failed: {error}"})
                await websocket.close(code=1011)
            except (WebSocketDisconnect, RuntimeError):
                pass
        finally:
            connection.close()

    return application


def _store_live_tick(
    connection: sqlite3.Connection, symbol: str, event: dict[str, object]
) -> None:
    try:
        price = float(event["price"])
        timestamp = float(event["timestamp"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Provider sent a malformed price tick") from error
    if not (price > 0 and timestamp > 0):
        raise ValueError("Provider sent a non-positive price or timestamp")
    timestamp_ms = int(timestamp * 1000) if timestamp < 10_000_000_000 else int(timestamp)
    volume_value = event.get("day_volume")
    volume = float(volume_value) if volume_value is not None else None
    with connection:
        connection.execute(
            """
            INSERT INTO ticks (provider, symbol, timestamp_ms, price, volume)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (provider, symbol, timestamp_ms, price) DO NOTHING
            """,
            (PROVIDER_NAME, symbol, timestamp_ms, price, volume),
        )
        for interval_seconds in (1, 60):
            bucket_ms = timestamp_ms // (interval_seconds * 1000) * (
                interval_seconds * 1000
            )
            connection.execute(
                """
                INSERT INTO candles (
                    provider, market_type, symbol, interval_seconds, timestamp_ms,
                    open, high, low, close, volume, data_kind
                ) VALUES (?, 'CLASSIC', ?, ?, ?, ?, ?, ?, ?, ?, 'LIVE')
                ON CONFLICT (provider, symbol, interval_seconds, timestamp_ms, data_kind)
                DO UPDATE SET
                    high = MAX(candles.high, excluded.close),
                    low = MIN(candles.low, excluded.close),
                    close = excluded.close,
                    volume = excluded.volume
                """,
                (
                    PROVIDER_NAME,
                    symbol,
                    interval_seconds,
                    bucket_ms,
                    price,
                    price,
                    price,
                    price,
                    None,
                ),
            )


app = create_app()
