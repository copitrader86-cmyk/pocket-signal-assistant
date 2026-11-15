import sqlite3
from pathlib import Path


SCHEMA_VERSION = 11

_SCHEMA = (
    """
    CREATE TABLE market_instruments (
        provider TEXT NOT NULL,
        market_type TEXT NOT NULL CHECK (market_type IN ('CLASSIC', 'OTC')),
        symbol TEXT NOT NULL,
        display_name TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (provider, market_type, symbol)
    )
    """,
    """
    CREATE TABLE candles (
        id INTEGER PRIMARY KEY,
        provider TEXT NOT NULL,
        market_type TEXT NOT NULL CHECK (market_type IN ('CLASSIC', 'OTC')),
        symbol TEXT NOT NULL,
        interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
        timestamp_ms INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL,
        data_kind TEXT NOT NULL CHECK (data_kind IN ('HISTORICAL', 'LIVE')),
        CHECK (high >= open AND high >= close),
        CHECK (low <= open AND low <= close),
        CHECK (high >= low),
        CHECK (volume IS NULL OR volume >= 0),
        UNIQUE (provider, symbol, interval_seconds, timestamp_ms, data_kind)
    )
    """,
    """
    CREATE INDEX candles_lookup
    ON candles (provider, symbol, interval_seconds, timestamp_ms)
    """,
    """
    CREATE INDEX candles_historical_lookup
    ON candles (provider, market_type, symbol, timestamp_ms)
    WHERE data_kind = 'HISTORICAL'
    """,
    """
    CREATE TABLE trading_sessions (
        id TEXT PRIMARY KEY,
        started_at_ms INTEGER NOT NULL,
        ended_at_ms INTEGER,
        CHECK (ended_at_ms IS NULL OR ended_at_ms >= started_at_ms)
    )
    """,
    """
    CREATE TABLE model_versions (
        id TEXT PRIMARY KEY,
        created_at_ms INTEGER NOT NULL,
        training_samples INTEGER NOT NULL CHECK (training_samples >= 0),
        validation_samples INTEGER NOT NULL CHECK (validation_samples >= 0),
        metrics_json TEXT NOT NULL CHECK (json_valid(metrics_json)),
        active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1))
    )
    """,
    """
    CREATE TABLE signals (
        id TEXT PRIMARY KEY,
        session_id TEXT REFERENCES trading_sessions (id),
        provider TEXT NOT NULL,
        market_type TEXT NOT NULL CHECK (market_type IN ('CLASSIC', 'OTC')),
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL CHECK (direction IN ('UP', 'DOWN', 'NO_SIGNAL')),
        expiry_seconds INTEGER NOT NULL
            CHECK (expiry_seconds IN (3, 5, 10, 15, 20, 25, 30, 60, 120, 180, 240, 300, 600, 900)),
        issued_at_ms INTEGER NOT NULL,
        recommended_entry_at_ms INTEGER,
        expires_at_ms INTEGER NOT NULL,
        quote_timestamp_ms INTEGER,
        quote_latency_ms INTEGER CHECK (quote_latency_ms IS NULL OR quote_latency_ms >= 0),
        probability_up REAL CHECK (probability_up IS NULL OR probability_up BETWEEN 0 AND 100),
        probability_down REAL CHECK (probability_down IS NULL OR probability_down BETWEEN 0 AND 100),
        market_snapshot_json TEXT NOT NULL CHECK (json_valid(market_snapshot_json)),
        analytics_json TEXT NOT NULL CHECK (json_valid(analytics_json)),
        explanation TEXT NOT NULL,
        model_version_id TEXT REFERENCES model_versions (id),
        outcome TEXT NOT NULL DEFAULT 'PENDING'
            CHECK (outcome IN ('PENDING', 'WON', 'LOST', 'MISSED', 'DRAW')),
        outcome_recorded_at_ms INTEGER,
        CHECK (
            (probability_up IS NULL AND probability_down IS NULL)
            OR (probability_up IS NOT NULL AND probability_down IS NOT NULL)
        )
    )
    """,
    """
    CREATE INDEX signals_history
    ON signals (issued_at_ms DESC)
    """,
    """
    CREATE INDEX signals_session_history
    ON signals (session_id, issued_at_ms DESC)
    """,
)


def connect(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


def initialize_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(database_path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        current_version = connection.execute("PRAGMA user_version").fetchone()[0]
        if current_version > SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {current_version} is newer than "
                f"supported version {SCHEMA_VERSION}"
            )
        if current_version == SCHEMA_VERSION:
            return

        connection.execute("BEGIN IMMEDIATE")
        try:
            if current_version == 0:
                for statement in _SCHEMA:
                    connection.execute(statement)
                current_version = 1
                connection.execute("PRAGMA user_version = 1")
            if current_version == 1:
                connection.execute(
                    """
                    CREATE TABLE ticks (
                        id INTEGER PRIMARY KEY,
                        provider TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        timestamp_ms INTEGER NOT NULL,
                        price REAL NOT NULL CHECK (price > 0),
                        volume REAL CHECK (volume IS NULL OR volume >= 0),
                        UNIQUE (provider, symbol, timestamp_ms, price)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX ticks_lookup
                    ON ticks (provider, symbol, timestamp_ms)
                    """
                )
                current_version = 2
                connection.execute("PRAGMA user_version = 2")
            if current_version == 2:
                connection.execute(
                    """
                    CREATE TABLE signals_new (
                        id TEXT PRIMARY KEY,
                        session_id TEXT REFERENCES trading_sessions (id),
                        provider TEXT NOT NULL,
                        market_type TEXT NOT NULL CHECK (market_type IN ('CLASSIC', 'OTC')),
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL CHECK (direction IN ('UP', 'DOWN', 'NO_SIGNAL')),
                        expiry_seconds INTEGER NOT NULL
                            CHECK (expiry_seconds IN (3, 5, 10, 15, 20, 25, 30, 60, 120, 180, 240, 300, 600, 900)),
                        issued_at_ms INTEGER NOT NULL,
                        recommended_entry_at_ms INTEGER,
                        expires_at_ms INTEGER NOT NULL,
                        quote_timestamp_ms INTEGER,
                        quote_latency_ms INTEGER
                            CHECK (quote_latency_ms IS NULL OR quote_latency_ms >= 0),
                        probability_up REAL
                            CHECK (probability_up IS NULL OR probability_up BETWEEN 0 AND 100),
                        probability_down REAL
                            CHECK (probability_down IS NULL OR probability_down BETWEEN 0 AND 100),
                        market_snapshot_json TEXT NOT NULL CHECK (json_valid(market_snapshot_json)),
                        analytics_json TEXT NOT NULL CHECK (json_valid(analytics_json)),
                        explanation TEXT NOT NULL,
                        model_version_id TEXT REFERENCES model_versions (id),
                        outcome TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK (outcome IN ('PENDING', 'WON', 'LOST', 'MISSED', 'DRAW')),
                        outcome_recorded_at_ms INTEGER,
                        CHECK (
                            (probability_up IS NULL AND probability_down IS NULL)
                            OR (probability_up IS NOT NULL AND probability_down IS NOT NULL)
                        )
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO signals_new
                    SELECT id, session_id, provider, market_type, symbol, direction,
                        expiry_seconds, issued_at_ms, recommended_entry_at_ms,
                        expires_at_ms, quote_timestamp_ms, quote_latency_ms,
                        probability_up, probability_down, market_snapshot_json,
                        analytics_json, explanation, model_version_id, outcome,
                        outcome_recorded_at_ms
                    FROM signals
                    """
                )
                connection.execute("DROP TABLE signals")
                connection.execute("ALTER TABLE signals_new RENAME TO signals")
                connection.execute(
                    "CREATE INDEX signals_history ON signals (issued_at_ms DESC)"
                )
                connection.execute(
                    "CREATE INDEX signals_session_history "
                    "ON signals (session_id, issued_at_ms DESC)"
                )
                current_version = 3
                connection.execute("PRAGMA user_version = 3")
            if current_version == 3:
                connection.execute(
                    """
                    CREATE TABLE reference_rates (
                        provider TEXT NOT NULL,
                        base_currency TEXT NOT NULL,
                        quote_currency TEXT NOT NULL,
                        rate_date TEXT NOT NULL,
                        rate REAL NOT NULL CHECK (rate > 0),
                        retrieved_at_ms INTEGER NOT NULL,
                        PRIMARY KEY (provider, base_currency, quote_currency, rate_date)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX reference_rates_lookup
                    ON reference_rates (base_currency, quote_currency, rate_date)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE market_ticks (
                        provider TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        timestamp_ms INTEGER NOT NULL,
                        timestamp_resolution_ms INTEGER NOT NULL
                            CHECK (timestamp_resolution_ms > 0),
                        bid REAL NOT NULL CHECK (bid > 0),
                        ask REAL NOT NULL CHECK (ask >= bid),
                        PRIMARY KEY (provider, symbol, timestamp_ms)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX market_ticks_lookup
                    ON market_ticks (provider, symbol, timestamp_ms)
                    """
                )
                current_version = 4
                connection.execute("PRAGMA user_version = 4")
            if current_version == 4:
                connection.execute(
                """
                CREATE TABLE signals_new (
                    id TEXT PRIMARY KEY,
                    session_id TEXT REFERENCES trading_sessions (id),
                    provider TEXT NOT NULL,
                    market_type TEXT NOT NULL CHECK (market_type IN ('CLASSIC', 'OTC')),
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK (direction IN ('UP', 'DOWN', 'NO_SIGNAL')),
                    expiry_seconds INTEGER NOT NULL
                        CHECK (expiry_seconds IN (3, 5, 10, 15, 20, 25, 30, 60, 120, 180, 240, 300, 600, 900)),
                    issued_at_ms INTEGER NOT NULL,
                    recommended_entry_at_ms INTEGER,
                    expires_at_ms INTEGER NOT NULL,
                    quote_timestamp_ms INTEGER,
                    quote_latency_ms INTEGER
                        CHECK (quote_latency_ms IS NULL OR quote_latency_ms >= 0),
                    probability_up REAL
                        CHECK (probability_up IS NULL OR probability_up BETWEEN 0 AND 100),
                    probability_down REAL
                        CHECK (probability_down IS NULL OR probability_down BETWEEN 0 AND 100),
                    market_snapshot_json TEXT NOT NULL CHECK (json_valid(market_snapshot_json)),
                    analytics_json TEXT NOT NULL CHECK (json_valid(analytics_json)),
                    explanation TEXT NOT NULL,
                    model_version_id TEXT REFERENCES model_versions (id),
                    outcome TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (outcome IN ('PENDING', 'WON', 'LOST', 'MISSED', 'DRAW')),
                    outcome_recorded_at_ms INTEGER,
                    CHECK (
                        (probability_up IS NULL AND probability_down IS NULL)
                        OR (probability_up IS NOT NULL AND probability_down IS NOT NULL)
                    )
                )
                """
                )
                connection.execute(
                """
                INSERT INTO signals_new
                SELECT id, session_id, provider, market_type, symbol, direction,
                    expiry_seconds, issued_at_ms, recommended_entry_at_ms,
                    expires_at_ms, quote_timestamp_ms, quote_latency_ms,
                    probability_up, probability_down, market_snapshot_json,
                    analytics_json, explanation, model_version_id, outcome,
                    outcome_recorded_at_ms
                FROM signals
                """
                )
                connection.execute("DROP TABLE signals")
                connection.execute("ALTER TABLE signals_new RENAME TO signals")
                connection.execute(
                "CREATE INDEX signals_history ON signals (issued_at_ms DESC)"
                )
                connection.execute(
                "CREATE INDEX signals_session_history "
                "ON signals (session_id, issued_at_ms DESC)"
                )
                current_version = 5
                connection.execute("PRAGMA user_version = 5")
            if current_version == 5:
                connection.execute("ALTER TABLE market_ticks RENAME TO market_ticks_old")
                connection.execute(
                    """
                    CREATE TABLE market_ticks (
                        provider TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        timestamp_ms INTEGER NOT NULL,
                        tick_sequence INTEGER NOT NULL CHECK (tick_sequence >= 0),
                        timestamp_resolution_ms INTEGER NOT NULL
                            CHECK (timestamp_resolution_ms > 0),
                        bid REAL NOT NULL CHECK (bid > 0),
                        ask REAL NOT NULL CHECK (ask >= bid),
                        PRIMARY KEY (provider, symbol, timestamp_ms, tick_sequence)
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO market_ticks (
                        provider, symbol, timestamp_ms, tick_sequence,
                        timestamp_resolution_ms, bid, ask
                    )
                    SELECT provider, symbol, timestamp_ms, 0,
                        timestamp_resolution_ms, bid, ask
                    FROM market_ticks_old
                    """
                )
                connection.execute("DROP TABLE market_ticks_old")
                connection.execute(
                    """
                    CREATE INDEX market_ticks_lookup
                    ON market_ticks (provider, symbol, timestamp_ms)
                    """
                )
                current_version = 6
                connection.execute("PRAGMA user_version = 6")
            if current_version == 6:
                connection.execute(
                    """
                    CREATE TABLE public_history_sync (
                        provider TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        status TEXT NOT NULL
                            CHECK (status IN ('pending', 'loading', 'ready', 'error')),
                        started_at_ms INTEGER,
                        updated_at_ms INTEGER NOT NULL,
                        last_success_at_ms INTEGER,
                        error_message TEXT,
                        PRIMARY KEY (provider, symbol)
                    )
                    """
                )
                current_version = 7
                connection.execute("PRAGMA user_version = 7")
            if current_version == 7:
                connection.execute(
                    """
                    ALTER TABLE candles ADD COLUMN source_resolution_ms INTEGER
                    CHECK (source_resolution_ms IS NULL OR source_resolution_ms > 0)
                    """
                )
                connection.execute(
                    """
                    UPDATE candles
                    SET source_resolution_ms = CASE
                        WHEN provider = 'Dukascopy' THEN 1
                        WHEN provider = 'TwelveData' THEN interval_seconds * 1000
                        ELSE NULL
                    END
                    WHERE data_kind = 'HISTORICAL'
                    """
                )
                current_version = 8
                connection.execute("PRAGMA user_version = 8")
            if current_version == 8:
                connection.execute(
                    """
                    ALTER TABLE public_history_sync
                    ADD COLUMN last_scanned_timestamp_ms INTEGER
                    CHECK (
                        last_scanned_timestamp_ms IS NULL
                        OR last_scanned_timestamp_ms >= 0
                    )
                    """
                )
                current_version = 9
                connection.execute("PRAGMA user_version = 9")
            if current_version == 9:
                connection.execute("ALTER TABLE signals RENAME TO signals_old")
                connection.execute(
                    """
                    CREATE TABLE signals_new (
                        id TEXT PRIMARY KEY,
                        session_id TEXT REFERENCES trading_sessions (id),
                        provider TEXT NOT NULL,
                        market_type TEXT NOT NULL CHECK (market_type IN ('CLASSIC', 'OTC')),
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL CHECK (
                            direction IN ('UP', 'DOWN', 'NO_SIGNAL')
                        ),
                        expiry_seconds INTEGER NOT NULL CHECK (
                            expiry_seconds IN (
                                3, 5, 10, 15, 20, 25, 30, 60, 120, 180,
                                240, 300, 600, 900
                            )
                        ),
                        issued_at_ms INTEGER NOT NULL,
                        recommended_entry_at_ms INTEGER,
                        expires_at_ms INTEGER NOT NULL,
                        quote_timestamp_ms INTEGER,
                        quote_latency_ms INTEGER
                            CHECK (quote_latency_ms IS NULL OR quote_latency_ms >= 0),
                        probability_up REAL
                            CHECK (probability_up IS NULL OR probability_up BETWEEN 0 AND 100),
                        probability_down REAL
                            CHECK (probability_down IS NULL OR probability_down BETWEEN 0 AND 100),
                        market_snapshot_json TEXT NOT NULL
                            CHECK (json_valid(market_snapshot_json)),
                        analytics_json TEXT NOT NULL CHECK (json_valid(analytics_json)),
                        explanation TEXT NOT NULL,
                        model_version_id TEXT REFERENCES model_versions (id),
                        outcome TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK (outcome IN ('PENDING', 'WON', 'LOST', 'MISSED', 'DRAW')),
                        outcome_recorded_at_ms INTEGER,
                        CHECK (
                            (probability_up IS NULL AND probability_down IS NULL)
                            OR (probability_up IS NOT NULL AND probability_down IS NOT NULL)
                        )
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO signals_new
                    SELECT id, session_id, provider, market_type, symbol, direction,
                        expiry_seconds, issued_at_ms, recommended_entry_at_ms,
                        expires_at_ms, quote_timestamp_ms, quote_latency_ms,
                        probability_up, probability_down, market_snapshot_json,
                        analytics_json, explanation, model_version_id, outcome,
                        outcome_recorded_at_ms
                    FROM signals_old
                    """
                )
                connection.execute("DROP TABLE signals_old")
                connection.execute("ALTER TABLE signals_new RENAME TO signals")
                connection.execute(
                    "CREATE INDEX signals_history ON signals (issued_at_ms DESC)"
                )
                connection.execute(
                    "CREATE INDEX signals_session_history "
                    "ON signals (session_id, issued_at_ms DESC)"
                )
                current_version = 10
                connection.execute("PRAGMA user_version = 10")
            if current_version == 10:
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS candles_historical_lookup
                    ON candles (provider, market_type, symbol, timestamp_ms)
                    WHERE data_kind = 'HISTORICAL'
                    """
                )
                current_version = 11
                connection.execute("PRAGMA user_version = 11")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()
