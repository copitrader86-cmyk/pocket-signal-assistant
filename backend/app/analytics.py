import math
import statistics
from dataclasses import dataclass
from typing import Sequence

from app.market_data import Candle


MINIMUM_ANALYSIS_CANDLES = 50


@dataclass(frozen=True)
class Analysis:
    candidate_direction: str
    score: float
    features: dict[str, float | str | None]
    explanation: str


def _ema(values: Sequence[float], period: int) -> float:
    if len(values) < period:
        raise ValueError(f"At least {period} values are required for EMA")
    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return result


def _rsi(values: Sequence[float], period: int = 14) -> float:
    if len(values) <= period:
        raise ValueError(f"At least {period + 1} values are required for RSI")
    changes = [right - left for left, right in zip(values, values[1:])]
    gains = [max(change, 0.0) for change in changes[:period]]
    losses = [max(-change, 0.0) for change in changes[:period]]
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period
    for change in changes[period:]:
        average_gain = (average_gain * (period - 1) + max(change, 0.0)) / period
        average_loss = (average_loss * (period - 1) + max(-change, 0.0)) / period
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    return 100 - 100 / (1 + average_gain / average_loss)


def analyze_candles(candles: Sequence[Candle]) -> Analysis:
    if len(candles) < MINIMUM_ANALYSIS_CANDLES:
        return Analysis(
            candidate_direction="NO_SIGNAL",
            score=0.0,
            features={},
            explanation=f"Need at least {MINIMUM_ANALYSIS_CANDLES} candles; received {len(candles)}.",
        )
    ordered = sorted(candles, key=lambda candle: candle.timestamp_ms)
    closes = [candle.close for candle in ordered]
    latest, previous = ordered[-1], ordered[-2]
    ema9 = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    ema50 = _ema(closes, 50)
    rsi = _rsi(closes)
    macd = _ema(closes, 12) - _ema(closes, 26)
    macd_signal = _ema(
        [
            _ema(closes[:end], 12) - _ema(closes[:end], 26)
            for end in range(26, len(closes) + 1)
        ],
        9,
    )
    returns = [
        (right / left) - 1
        for left, right in zip(closes[-21:-1], closes[-20:])
    ]
    volatility = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    momentum = (closes[-1] / closes[-6]) - 1
    recent = ordered[-20:]
    support = min(candle.low for candle in recent)
    resistance = max(candle.high for candle in recent)
    total_body = sum(abs(candle.close - candle.open) for candle in recent)
    signed_body = sum(candle.close - candle.open for candle in recent)
    pressure = signed_body / total_body if total_body else 0.0
    bullish_engulfing = (
        previous.close < previous.open
        and latest.close > latest.open
        and latest.open <= previous.close
        and latest.close >= previous.open
    )
    bearish_engulfing = (
        previous.close > previous.open
        and latest.close < latest.open
        and latest.open >= previous.close
        and latest.close <= previous.open
    )

    score = 0.0
    score += 1.0 if ema9 > ema21 else -1.0
    score += 1.0 if ema21 > ema50 else -1.0
    score += 0.5 if macd > macd_signal else -0.5
    score += 0.5 if momentum > 0 else -0.5
    score += 0.5 if pressure > 0.1 else -0.5 if pressure < -0.1 else 0.0
    score += 0.5 if bullish_engulfing else -0.5 if bearish_engulfing else 0.0
    if rsi >= 75:
        score -= 0.5
    elif rsi <= 25:
        score += 0.5

    direction = "UP" if score >= 2 else "DOWN" if score <= -2 else "NO_SIGNAL"
    details = [
        f"EMA 9/21/50: {ema9:.6g}/{ema21:.6g}/{ema50:.6g}",
        f"RSI(14): {rsi:.1f}",
        f"MACD histogram: {macd - macd_signal:.6g}",
        f"5-candle momentum: {momentum * 100:.4f}%",
        f"20-candle volatility: {volatility * 100:.4f}%",
    ]
    if direction == "NO_SIGNAL":
        details.append("Indicator votes do not show a strong directional consensus.")

    return Analysis(
        candidate_direction=direction,
        score=score,
        features={
            "ema_9": ema9,
            "ema_21": ema21,
            "ema_50": ema50,
            "rsi_14": rsi,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_histogram": macd - macd_signal,
            "momentum_5": momentum,
            "volatility_20": volatility,
            "support_20": support,
            "resistance_20": resistance,
            "buyer_seller_pressure_20": pressure,
            "bullish_engulfing": "true" if bullish_engulfing else "false",
            "bearish_engulfing": "true" if bearish_engulfing else "false",
            "score": score,
        },
        explanation="; ".join(details),
    )


def calibrated_probabilities(
    direction: str,
    *,
    model_win_probability: float | None,
    model_validated: bool,
) -> tuple[float | None, float | None]:
    if (
        direction not in ("UP", "DOWN")
        or not model_validated
        or model_win_probability is None
        or not math.isfinite(model_win_probability)
        or not 0 <= model_win_probability <= 1
    ):
        return None, None
    win_percent = round(model_win_probability * 100, 2)
    loss_percent = round((1 - model_win_probability) * 100, 2)
    return (
        (win_percent, loss_percent)
        if direction == "UP"
        else (loss_percent, win_percent)
    )
