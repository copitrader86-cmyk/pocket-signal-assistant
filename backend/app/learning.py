import json
import math
from dataclasses import dataclass
from typing import Any, Sequence


MINIMUM_LABELED_SIGNALS = 150
FEATURE_NAMES = (
    "score",
    "rsi",
    "macd_normalized",
    "momentum_percent",
    "volatility_percent",
    "pressure",
    "direction",
    "expiry_normalized",
)


@dataclass(frozen=True)
class TrainingResult:
    eligible: bool
    active: bool
    reason: str
    training_samples: int
    validation_samples: int
    metrics: dict[str, Any]
    artifact: dict[str, Any] | None
    validation_examples: list[tuple[list[float], int]]


def _sigmoid(value: float) -> float:
    if value >= 0:
        return 1 / (1 + math.exp(-min(value, 700)))
    exp_value = math.exp(max(value, -700))
    return exp_value / (1 + exp_value)


def _vector(
    analytics_json: str,
    market_snapshot_json: str,
    direction: str,
    expiry_seconds: int,
) -> list[float]:
    analytics = json.loads(analytics_json)
    market = json.loads(market_snapshot_json)
    latest_price = float(market.get("latest_price") or 0)
    macd_histogram = float(analytics.get("macd_histogram") or 0)
    return [
        max(-4.0, min(4.0, float(analytics.get("score") or 0) / 3.5)),
        float(analytics.get("rsi_14") or 50) / 100,
        macd_histogram / latest_price if latest_price else 0.0,
        float(analytics.get("momentum_5") or 0) * 100,
        float(analytics.get("volatility_20") or 0) * 100,
        float(analytics.get("buyer_seller_pressure_20") or 0),
        1.0 if direction == "UP" else -1.0,
        expiry_seconds / 900,
    ]


def _features(
    analytics_json: str,
    market_snapshot_json: str,
    direction: str,
    expiry_seconds: int,
) -> list[float]:
    try:
        vector = _vector(
            analytics_json, market_snapshot_json, direction, expiry_seconds
        )
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("A labeled signal has invalid feature data") from error
    if not all(math.isfinite(value) for value in vector):
        raise ValueError("A labeled signal contains non-finite features")
    return vector


def _raw_probability(artifact: dict[str, Any], vector: Sequence[float]) -> float:
    means = artifact["means"]
    scales = artifact["scales"]
    coefficients = artifact["coefficients"]
    standardized = [
        (value - means[index]) / scales[index]
        for index, value in enumerate(vector)
    ]
    logit = artifact["intercept"] + sum(
        weight * value for weight, value in zip(coefficients, standardized)
    )
    return _sigmoid(logit)


def _calibrated_probability(artifact: dict[str, Any], vector: Sequence[float]) -> float:
    raw = _raw_probability(artifact, vector)
    raw_logit = math.log(max(raw, 1e-12) / max(1 - raw, 1e-12))
    calibration = artifact.get("calibration", {"slope": 1.0, "intercept": 0.0})
    return _sigmoid(
        float(calibration["slope"]) * raw_logit
        + float(calibration["intercept"])
    )


def predict_win_probability(
    artifact: dict[str, Any],
    analytics: dict[str, Any],
    market_snapshot: dict[str, Any],
    direction: str,
    expiry_seconds: int,
) -> float:
    vector = _features(
        json.dumps(analytics),
        json.dumps(market_snapshot),
        direction,
        expiry_seconds,
    )
    return _calibrated_probability(artifact, vector)


def train_model(records: Sequence[Any]) -> TrainingResult:
    samples: list[tuple[int, list[float], int]] = []
    for record in records:
        if record["direction"] not in ("UP", "DOWN") or record["outcome"] not in (
            "WON",
            "LOST",
        ):
            continue
        samples.append(
            (
                int(record["issued_at_ms"]),
                _features(
                    record["analytics_json"],
                    record["market_snapshot_json"],
                    record["direction"],
                    int(record["expiry_seconds"]),
                ),
                1 if record["outcome"] == "WON" else 0,
            )
        )
    samples.sort(key=lambda item: item[0])

    sample_count = len(samples)
    training_end = int(sample_count * 0.6)
    calibration_end = int(sample_count * 0.8)
    training = samples[:training_end]
    calibration = samples[training_end:calibration_end]
    validation = samples[calibration_end:]
    if sample_count < MINIMUM_LABELED_SIGNALS or len(validation) < 20:
        return TrainingResult(
            eligible=False,
            active=False,
            reason=f"Need at least {MINIMUM_LABELED_SIGNALS} labeled trade outcomes.",
            training_samples=len(training),
            validation_samples=len(validation),
            metrics={},
            artifact=None,
            validation_examples=[],
        )

    train_labels = [sample[2] for sample in training]
    validation_labels = [sample[2] for sample in validation]
    if len(set(train_labels)) < 2 or len(set(validation_labels)) < 2:
        return TrainingResult(
            eligible=False,
            active=False,
            reason=(
                "Training, calibration, and out-of-sample partitions must each "
                "contain wins and losses."
            ),
            training_samples=len(training),
            validation_samples=len(validation),
            metrics={},
            artifact=None,
            validation_examples=[],
        )
    if len(set(sample[2] for sample in calibration)) < 2:
        return TrainingResult(
            eligible=False,
            active=False,
            reason="The calibration partition must contain wins and losses.",
            training_samples=len(training),
            validation_samples=len(validation),
            metrics={},
            artifact=None,
            validation_examples=[],
        )

    vectors = [sample[1] for sample in training]
    means = [
        sum(vector[index] for vector in vectors) / len(vectors)
        for index in range(len(FEATURE_NAMES))
    ]
    scales = [
        math.sqrt(
            sum((vector[index] - means[index]) ** 2 for vector in vectors)
            / len(vectors)
        )
        or 1.0
        for index in range(len(FEATURE_NAMES))
    ]
    standardized = [
        [(value - means[index]) / scales[index] for index, value in enumerate(vector)]
        for vector in vectors
    ]
    weights = [0.0] * len(FEATURE_NAMES)
    intercept = 0.0
    learning_rate = 0.08
    regularization = 0.02

    for _ in range(600):
        weight_gradients = [0.0] * len(weights)
        intercept_gradient = 0.0
        for vector, label in zip(standardized, train_labels):
            error = _sigmoid(
                intercept + sum(weight * value for weight, value in zip(weights, vector))
            ) - label
            intercept_gradient += error
            for index, value in enumerate(vector):
                weight_gradients[index] += error * value
        scale = learning_rate / len(training)
        intercept -= scale * intercept_gradient
        for index in range(len(weights)):
            weights[index] -= scale * (
                weight_gradients[index] + regularization * weights[index]
            )

    artifact: dict[str, Any] = {
        "feature_names": list(FEATURE_NAMES),
        "means": means,
        "scales": scales,
        "coefficients": weights,
        "intercept": intercept,
    }

    calibration_logits = [
        math.log(
            max(_raw_probability(artifact, vector), 1e-12)
            / max(1 - _raw_probability(artifact, vector), 1e-12)
        )
        for _, vector, _ in calibration
    ]
    calibration_labels = [label for _, _, label in calibration]
    calibration_slope = 1.0
    calibration_intercept = 0.0
    for _ in range(500):
        slope_gradient = 0.0
        intercept_gradient = 0.0
        for logit, label in zip(calibration_logits, calibration_labels):
            error = _sigmoid(calibration_slope * logit + calibration_intercept) - label
            slope_gradient += error * logit
            intercept_gradient += error
        learning_rate = 0.05 / len(calibration)
        calibration_slope -= learning_rate * slope_gradient
        calibration_intercept -= learning_rate * intercept_gradient
    artifact["calibration"] = {
        "slope": calibration_slope,
        "intercept": calibration_intercept,
    }

    validation_probabilities = [
        _calibrated_probability(artifact, vector)
        for _, vector, _ in validation
    ]
    predicted_labels = [int(probability >= 0.5) for probability in validation_probabilities]
    accuracy = sum(
        predicted == actual
        for predicted, actual in zip(predicted_labels, validation_labels)
    ) / len(validation_labels)
    brier_score = sum(
        (probability - actual) ** 2
        for probability, actual in zip(validation_probabilities, validation_labels)
    ) / len(validation_labels)
    base_accuracy = max(
        sum(validation_labels) / len(validation_labels),
        1 - sum(validation_labels) / len(validation_labels),
    )
    base_brier = sum(
        (sum(train_labels) / len(train_labels) - actual) ** 2
        for actual in validation_labels
    ) / len(validation_labels)
    eligible = accuracy > base_accuracy and brier_score < base_brier
    metrics = {
        "accuracy": accuracy,
        "brier_score": brier_score,
        "baseline_accuracy": base_accuracy,
        "baseline_brier_score": base_brier,
        "validation_samples": len(validation),
        "training_samples": len(training),
        "validated": eligible,
    }
    return TrainingResult(
        eligible=eligible,
        active=eligible,
        reason=(
            "Model passed the chronological holdout comparison."
            if eligible
            else "Model did not outperform the holdout baseline; it was not activated."
        ),
        training_samples=len(training),
        validation_samples=len(validation),
        metrics=metrics,
        artifact=artifact,
        validation_examples=[
            (vector, label) for (_, vector, label) in validation
        ],
    )


def evaluate_artifact(
    artifact: dict[str, Any],
    examples: Sequence[tuple[list[float], int]],
) -> dict[str, float]:
    probabilities = [
        _calibrated_probability(artifact, vector) for vector, _ in examples
    ]
    labels = [label for _, label in examples]
    return {
        "accuracy": sum(
            int(probability >= 0.5) == label
            for probability, label in zip(probabilities, labels)
        )
        / len(labels),
        "brier_score": sum(
            (probability - label) ** 2
            for probability, label in zip(probabilities, labels)
        )
        / len(labels),
    }
