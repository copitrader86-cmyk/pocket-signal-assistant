from pathlib import Path

from app.database import connect, initialize_database


def test_schema_upgrade_adds_covering_historical_candle_index(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "signals.db"
    initialize_database(database_path)
    with connect(database_path) as connection:
        connection.execute("DROP INDEX candles_historical_lookup")
        connection.execute("PRAGMA user_version = 10")

    initialize_database(database_path)

    with connect(database_path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        catalog_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
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
            """
        ).fetchall()
        status_plan = connection.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT COUNT(*), MIN(timestamp_ms), MAX(timestamp_ms)
            FROM candles
            WHERE provider = ? AND data_kind = 'HISTORICAL'
                AND (? IS NULL OR symbol = ?)
            """,
            ("HistData", None, None),
        ).fetchall()

    assert version == 11
    assert any(
        "candles_historical_lookup" in row["detail"] for row in catalog_plan
    )
    assert any("candles_historical_lookup" in row["detail"] for row in status_plan)
