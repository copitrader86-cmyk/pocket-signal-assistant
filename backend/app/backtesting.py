from dataclasses import dataclass
from typing import Sequence

from app.analytics import MINIMUM_ANALYSIS_CANDLES, analyze_candles
from app.market_data import Candle


@dataclass(frozen=True)
class BacktestResult:
    provider: str
    symbol: str
    candle_interval_seconds: int
    expiry_seconds: int
    sample_count: int
    trade_count: int
    wins: int
    losses: int
    coverage_percent: float
    accuracy_percent: float | None
    out_of_sample_start_ms: int | None


class BacktestError(ValueError):
    pass


def run_backtest(
    candles: Sequence[Candle],
    *,
    provider: str,
    symbol: str,
    candle_interval_seconds: int,
    expiry_seconds: int,
) -> BacktestResult:
    if candle_interval_seconds != 60:
        raise BacktestError(
            "Current technical backtest requires 1-minute historical candles; "
            "minute bars cannot validate second-based expirations."
        )
    if expiry_seconds < 60 or expiry_seconds % candle_interval_seconds:
        raise BacktestError(
            "Historical minute candles cannot validate second-based expirations "
            "or expirations not aligned to the candle interval."
        )
    ordered = sorted(candles, key=lambda candle: candle.timestamp_ms)
    if len(ordered) < MINIMUM_ANALYSIS_CANDLES + 2:
        raise BacktestError("Not enough historical candles for a holdout backtest")

    expiry_steps = expiry_seconds // candle_interval_seconds
    split_index = int(len(ordered) * 0.8)
    sample_count = 0
    trade_count = 0
    wins = 0
    losses = 0
    test_start_ms: int | None = None
    interval_ms = candle_interval_seconds * 1000
    segment_start = 0
    segments: list[tuple[int, int]] = []
    for index in range(1, len(ordered)):
        if ordered[index].timestamp_ms - ordered[index - 1].timestamp_ms != interval_ms:
            segments.append((segment_start, index))
            segment_start = index
    segments.append((segment_start, len(ordered)))

    for start, end in segments:
        first_test_index = max(start + MINIMUM_ANALYSIS_CANDLES, split_index)
        last_test_index = end - expiry_steps
        for index in range(first_test_index, last_test_index):
            sample_count += 1
            test_start_ms = test_start_ms or ordered[index].timestamp_ms
            analysis = analyze_candles(ordered[start : index + 1])
            direction = analysis.candidate_direction
            if direction == "NO_SIGNAL":
                continue
            trade_count += 1
            entry = ordered[index].close
            exit_price = ordered[index + expiry_steps].close
            if (direction == "UP" and exit_price > entry) or (
                direction == "DOWN" and exit_price < entry
            ):
                wins += 1
            else:
                losses += 1

    return BacktestResult(
        provider=provider,
        symbol=symbol,
        candle_interval_seconds=candle_interval_seconds,
        expiry_seconds=expiry_seconds,
        sample_count=sample_count,
        trade_count=trade_count,
        wins=wins,
        losses=losses,
        coverage_percent=round(trade_count * 100 / sample_count, 2)
        if sample_count
        else 0.0,
        accuracy_percent=round(wins * 100 / trade_count, 2) if trade_count else None,
        out_of_sample_start_ms=test_start_ms,
    )
