from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib

@dataclass(frozen=True)
class PipelinePaths:
    raw_dir: Path
    proc_dir: Path
    db_path: Path

@dataclass(frozen=True)
class WeatherConfig:
    latitude: float
    longitude: float
    days: int
    timezone: str

@dataclass(frozen=True)
class LoggingConfig:
    level: str

@dataclass(frozen=True)
class PipelineConfig:
    paths: PipelinePaths
    weather: WeatherConfig
    logging: LoggingConfig

def repo_root() -> Path:
    # simple: repo root is parent of this file’s folder (src/)
    return Path(__file__).resolve().parent.parent

def load_config(config_path: str | Path = "config/pipeline.toml") -> PipelineConfig:
    config_path = Path(config_path)
    if not config_path.is_absolute():
        config_path = repo_root() / config_path

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))

    paths = data["paths"]
    weather = data["weather"]
    logging_cfg = data.get("logging", {"level": "INFO"})

    root = repo_root()
    raw_dir = (root / paths["raw_dir"]).resolve()
    proc_dir = (root / paths["proc_dir"]).resolve()
    db_path = (root / paths["db_path"]).resolve()

    return PipelineConfig(
        paths=PipelinePaths(raw_dir=raw_dir, proc_dir=proc_dir, db_path=db_path),
        weather=WeatherConfig(
            latitude=float(weather["latitude"]),
            longitude=float(weather["longitude"]),
            days=int(weather["days"]),
            timezone=str(weather.get("timezone", "Europe/Rome")),
        ),
        logging=LoggingConfig(level=str(logging_cfg.get("level", "INFO"))),
    )