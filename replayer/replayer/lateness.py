"""Decides when each event is emitted relative to its event time.

Everything here is in the event-time domain (milliseconds) and is a pure
function of the input and the seed, so a replay is reproducible: the same
config always produces the same disorder. Pacing against the wall clock is a
separate concern handled in replay.py.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class LatenessConfig:
    # Every event is delayed by uniform(0, jitter_s): mild, constant disorder.
    jitter_s: float = 5.0
    # This fraction of events is additionally delayed by an exponential draw.
    late_fraction: float = 0.02
    late_mean_s: float = 120.0
    # Hard cap on the extra delay, so the tail is bounded and testable.
    late_max_s: float = 900.0
    seed: int = 7

    def __post_init__(self) -> None:
        if not 0.0 <= self.late_fraction <= 1.0:
            raise ValueError("late_fraction must be within [0, 1]")
        if min(self.jitter_s, self.late_mean_s, self.late_max_s) < 0:
            raise ValueError("delays must be non-negative")


@dataclass(frozen=True)
class Schedule:
    # Indices into the input, in emission order.
    order: np.ndarray
    # Emission time of each input row (event-time domain, ms), input order.
    emit_ts_ms: np.ndarray
    # Extra delay injected into "late" rows (0 for the rest), input order.
    injected_delay_ms: np.ndarray


def schedule(event_ts_ms: np.ndarray, cfg: LatenessConfig) -> Schedule:
    """Assigns an emission time to every event and returns the emission order."""
    event_ts_ms = np.asarray(event_ts_ms, dtype=np.int64)
    n = event_ts_ms.shape[0]
    rng = np.random.default_rng(cfg.seed)

    jitter_ms = (rng.random(n) * cfg.jitter_s * 1000.0).astype(np.int64)

    is_late = rng.random(n) < cfg.late_fraction
    late_ms = np.minimum(
        rng.exponential(cfg.late_mean_s * 1000.0, size=n),
        cfg.late_max_s * 1000.0,
    ).astype(np.int64)
    injected_delay_ms = np.where(is_late, late_ms, 0).astype(np.int64)

    emit_ts_ms = event_ts_ms + jitter_ms + injected_delay_ms
    order = np.argsort(emit_ts_ms, kind="stable")
    return Schedule(order=order, emit_ts_ms=emit_ts_ms, injected_delay_ms=injected_delay_ms)
