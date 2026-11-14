import os
import sqlite3
import json
import math
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Iterator

import websockets
from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from app.analytics import analyze_candles
from app.backtesting import BacktestError, run_backtest
from app.database import connect, initialize_database
from app.historical_import import MAX_CSV_BYTES, import_historical_csv
from app.learning import evaluate_artifact, predict_win_probability, train_model
from app.market_data import (
    Candle,
    INTERVALS_BY_SECONDS,
    PROVIDER_NAME,
    MarketDataError,
    TwelveDataClient,
    normalize_symbol,
)

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
    resolved_database_path = database_path or Path(
        os.environ.get("DATABASE_PATH", "data/signals.db")
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        initialize_database(resolved_database_path)
        yield

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
            SELECT timestamp_ms, open, high, low, close, volume
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
        connection: Annotated[sqlite3.Connection, Depends(get_connection)],
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

        with connection:
            connection.executemany(
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
                        PROVIDER_NAME,
                        normalized_symbol,
                        interval_seconds,
                        candle.timestamp_ms,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                    )
                    for candle in history
                ],
            )
            imported = cursor.rowcount
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
        if request.expiry_seconds not in (3, 5, 10, 15, 30, 60, 120, 180, 240, 300, 600, 900):
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
