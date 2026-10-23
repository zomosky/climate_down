"""CAMS keeps one fixed, tested scientific contract and configurable operations."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
import yaml


class Account(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]+$")]
    credential_file: Path = Field(repr=False)


class CamsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    output_dir: Path
    accounts: list[Account] = Field(min_length=1)
    cycle_accounts: dict[int, str]
    first_init: str
    lookback_days: int = Field(default=1, ge=0, le=2)
    publish_lag_hours: float = Field(default=10, ge=10, le=24)
    poll_seconds: int = Field(default=30, ge=10, le=300)
    max_run_seconds: int = Field(default=2400, ge=0, le=86400)
    transfer_seconds: int = Field(default=240, ge=10, le=600)
    max_asset_bytes: int = Field(default=1024**3, ge=1, le=4 * 1024**3)

    @field_validator("first_init", mode="before")
    @classmethod
    def valid_floor(cls, value: object) -> str:
        value = str(value)
        parsed = dt.datetime.strptime(value, "%Y%m%d%H")
        if len(value) != 10 or parsed.hour not in (0, 12):
            raise ValueError("first_init must be YYYYMMDD00 or YYYYMMDD12")
        return value

    @model_validator(mode="after")
    def valid_accounts(self) -> CamsConfig:
        names = [a.name for a in self.accounts]
        if len(names) != len(set(names)) or set(self.cycle_accounts) != {0, 12}:
            raise ValueError("Unique accounts and explicit 00/12 ownership required")
        if not set(self.cycle_accounts.values()) <= set(names):
            raise ValueError("Unknown cycle account")
        return self

    def account_for(self, cycle: int) -> Account:
        name = self.cycle_accounts[cycle]
        return next(a for a in self.accounts if a.name == name)


def load_config(path: Path) -> CamsConfig:
    return CamsConfig.model_validate(yaml.safe_load(path.read_text()))


def candidate_inits(config: CamsConfig, cycle: int, at: dt.datetime) -> list[str]:
    """Recent due inits only, oldest first, with an explicit deployment floor."""
    if cycle not in (0, 12) or at.tzinfo is None:
        raise ValueError("Cycle 00/12 and an aware UTC clock are required")
    due = at.astimezone(dt.UTC) - dt.timedelta(hours=config.publish_lag_hours)
    latest = due.replace(hour=cycle, minute=0, second=0, microsecond=0)
    if latest > due:
        latest -= dt.timedelta(days=1)
    return [key for n in range(config.lookback_days, -1, -1)
            if (key := (latest - dt.timedelta(days=n)).strftime("%Y%m%d%H")) >= config.first_init]
