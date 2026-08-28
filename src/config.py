"""Configuration loaded from .env. Fails loudly at import if misconfigured."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

load_dotenv(PROJECT_ROOT / ".env")


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or malformed."""


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(
            f"{name} is not set. Add it to {PROJECT_ROOT / '.env'} "
            f"(see .env.example for the expected shape)."
        )
    return value


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


# --- Database ---------------------------------------------------------

DB_URL_RW = _required("SUPABASE_DB_URL")
DB_URL_RO = _required("SUPABASE_DB_URL_RO")

# --- HTTP layer (populated at step 14; not required yet) --------------

APP_API_KEY = _optional("APP_API_KEY")
SESSION_SECRET = _optional("SESSION_SECRET")
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in _optional("ALLOWED_ORIGINS", "http://127.0.0.1:8000").split(",")
    if origin.strip()
]

# --- LLM (populated at step 20; not required yet) ---------------------

ANTHROPIC_API_KEY = _optional("ANTHROPIC_API_KEY")


@dataclass(frozen=True)
class RunConfig:
    """Replay pathology and watermark settings for one run.

    TWO CLOCKS, TWO UNITS. Mixing them is the classic bug:

    Feed pathology is in ARRIVAL seconds — wall-clock delay between an
    event happening and the consumer seeing it. Sub-second to a few
    seconds.

    Watermark settings are in EVENT seconds — session time. A window
    keyed (driver, lap) spans the whole lap in event time (~66s from
    sector 1 completing to sector 3 completing), and concurrent drivers
    are spread across another ~35s. allowed_lateness must exceed that
    combined span, or windows close before their own sectors arrive.
    """

    # --- Feed pathology: ARRIVAL seconds ---
    base_delay_s: float = 0.05
    tail_delay_s: float = 2.0
    p_tail: float = 0.05
    p_duplicate: float = 0.02
    p_drop: float = 0.0
    reorder_jitter_s: float = 0.5

    # --- Watermark: EVENT seconds (session time) ---
    # 150s covers intra-lap span (~66s) plus inter-driver skew (~35s)
    # with margin. Below ~110s, windows start closing early.
    allowed_lateness_s: float = 150.0
    max_lateness_s: float = 600.0

    # Replay speed multiplier: 20.0 means 20x faster than real time
    speed: float = 20.0

    # Checkpoint every N processed events
    checkpoint_every: int = 200

    def __post_init__(self) -> None:
        def check(name: str, lo: float, hi: float) -> None:
            value = getattr(self, name)
            if not lo <= value <= hi:
                raise ConfigError(
                    f"RunConfig.{name}={value} is outside the allowed "
                    f"range [{lo}, {hi}]."
                )

        check("base_delay_s", 0.0, 10.0)
        check("tail_delay_s", 0.0, 120.0)
        check("p_tail", 0.0, 1.0)
        check("p_duplicate", 0.0, 0.5)
        check("p_drop", 0.0, 0.5)
        check("reorder_jitter_s", 0.0, 60.0)
        check("allowed_lateness_s", 0.0, 3600.0)
        check("max_lateness_s", 0.0, 7200.0)
        check("speed", 0.1, 1000.0)
        check("checkpoint_every", 1, 100_000)

        if self.max_lateness_s < self.allowed_lateness_s:
            raise ConfigError(
                f"max_lateness_s ({self.max_lateness_s}) must be >= "
                f"allowed_lateness_s ({self.allowed_lateness_s})."
            )

    def to_json(self) -> dict:
        return {
            "base_delay_s": self.base_delay_s,
            "tail_delay_s": self.tail_delay_s,
            "p_tail": self.p_tail,
            "p_duplicate": self.p_duplicate,
            "p_drop": self.p_drop,
            "reorder_jitter_s": self.reorder_jitter_s,
            "allowed_lateness_s": self.allowed_lateness_s,
            "max_lateness_s": self.max_lateness_s,
            "speed": self.speed,
            "checkpoint_every": self.checkpoint_every,
        }