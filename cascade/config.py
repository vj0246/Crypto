"""Central configuration. Validated at import time; fails fast on bad values.

Secrets are read from the environment only. No API keys are required for the
free-data path (Hyperliquid info API, Deribit public API, Binance public API).
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CASCADE_", env_file=".env", extra="ignore"
    )

    # --- storage -------------------------------------------------------
    data_dir: Path = REPO_ROOT / "data"

    # --- universe ------------------------------------------------------
    assets: tuple[str, ...] = ("BTC", "ETH")

    # --- endpoints -----------------------------------------------------
    hl_info_url: str = "https://api.hyperliquid.xyz/info"
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    deribit_rest_url: str = "https://www.deribit.com/api/v2"
    deribit_ws_url: str = "wss://www.deribit.com/ws/api/v2"
    binance_fapi_url: str = "https://fapi.binance.com"
    binance_ws_url: str = "wss://fstream.binance.com/stream"

    # --- recorder ------------------------------------------------------
    flush_interval_s: float = 30.0
    flush_max_rows: int = 20_000
    ws_ping_interval_s: float = 20.0
    ws_reconnect_max_backoff_s: float = 60.0

    # --- fuel map ------------------------------------------------------
    # Hyperliquid exposes no "all open positions" endpoint. The address
    # universe is built from observed fills and refreshed by polling
    # clearinghouseState for the top-N addresses by recent notional.
    fuel_universe_size: int = 1500
    fuel_poll_concurrency: int = 8
    fuel_poll_interval_s: float = 60.0
    fuel_bands: tuple[float, ...] = (0.005, 0.01, 0.02, 0.05, 0.10)

    # --- hawkes --------------------------------------------------------
    # Sum-of-exponentials decay rates (1/second). Log-spaced over ~1s..~1h
    # so the mixture can mimic a power-law kernel.
    hawkes_betas: tuple[float, ...] = (1.0, 0.1, 0.01, 0.001)
    hawkes_max_spectral_radius: float = 0.995
    hawkes_fuel_gamma_cap: float = 3.0

    # --- pricing -------------------------------------------------------
    cos_n_terms: int = 512
    cos_l_trunc: float = 12.0

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("assets")
    @classmethod
    def _upper(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        if not v:
            raise ValueError("assets must not be empty")
        return tuple(a.upper() for a in v)

    @field_validator("hawkes_betas")
    @classmethod
    def _betas_positive_descending(cls, v: tuple[float, ...]) -> tuple[float, ...]:
        if not v or any(b <= 0 for b in v):
            raise ValueError("hawkes_betas must be non-empty and strictly positive")
        return tuple(sorted(v, reverse=True))

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def events_dir(self) -> Path:
        return self.data_dir / "events"

    @property
    def artifacts_dir(self) -> Path:
        return self.data_dir / "artifacts"

    def ensure_dirs(self) -> None:
        for d in (self.raw_dir, self.events_dir, self.artifacts_dir):
            d.mkdir(parents=True, exist_ok=True)


settings = Settings()
