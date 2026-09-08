"""Request and response models.

extra="forbid" on every input: an unexpected field is a 422, not a
silently ignored one. Bounds are enforced here so a bad value never
reaches the processor.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuthRequest(StrictModel):
    api_key: str = Field(min_length=1, max_length=256)


class RunRequest(StrictModel):
    dataset_id: str = Field(min_length=36, max_length=36)
    seed: int = Field(default=7, ge=0, le=2**31 - 1)

    base_delay_s: float = Field(default=0.05, ge=0.0, le=10.0)
    tail_delay_s: float = Field(default=2.0, ge=0.0, le=120.0)
    p_tail: float = Field(default=0.05, ge=0.0, le=1.0)
    p_duplicate: float = Field(default=0.02, ge=0.0, le=0.5)
    p_drop: float = Field(default=0.0, ge=0.0, le=0.5)
    reorder_jitter_s: float = Field(default=0.5, ge=0.0, le=60.0)

    allowed_lateness_s: float = Field(default=150.0, ge=0.0, le=3600.0)
    max_lateness_s: float = Field(default=600.0, ge=0.0, le=7200.0)
    speed: float = Field(default=20.0, ge=0.1, le=1000.0)


class HealthResponse(BaseModel):
    status: str
    database: str
    authenticated: bool


class MessageResponse(BaseModel):
    message: str