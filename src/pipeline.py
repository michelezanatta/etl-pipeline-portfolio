from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from src.config import load_config
from src.utils.logging import configure_logging, log_step
from src.extract import extract_to_parquet
from src.transform import transform_dir
from src.create_db import ensure_schema
from src.load import load_dir_to_duckdb

logger = logging.getLogger(__name__)

TABLES = [
    "weather_hourly_past",
    "weather_hourly_forecast",
    "weather_daily_past",
    "weather_daily_forecast",
]

def _post_load_checks(db_path: Path) -> None:
    con = duckdb.connect(str(db_path), read_only=True)

    existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    missing = [t for t in TABLES if t not in existing_tables]
    if missing:
        raise RuntimeError(f"Missing tables in DuckDB: {missing}")

    counts = {}
    for t in TABLES:
        counts[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    logger.info("Row counts: %s", counts)
    con.close()

@log_step()
def run_pipeline(config_path: str | Path = "config/pipeline.toml") -> None:
    cfg = load_config(config_path)
    configure_logging(cfg.logging.level)

    logger.info("Starting pipeline with db_path=%s", cfg.paths.db_path)
    extract_to_parquet(
        raw_dir=cfg.paths.raw_dir,
        latitude=cfg.weather.latitude,
        longitude=cfg.weather.longitude,
        days=cfg.weather.days,
    )
    transform_dir(cfg.paths.raw_dir, cfg.paths.proc_dir)
    ensure_schema(cfg.paths.db_path)
    load_dir_to_duckdb(cfg.paths.proc_dir, cfg.paths.db_path)
    _post_load_checks(cfg.paths.db_path)

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/pipeline.toml", help="Path to pipeline TOML")
    return p.parse_args()

try:
    from prefect import flow, task
except ImportError:
    flow = None
    task = None

if flow and task:
    @task
    def t_extract(config_path: str) -> None:
        cfg = load_config(config_path)
        extract_to_parquet(
            raw_dir=cfg.paths.raw_dir,
            latitude=cfg.weather.latitude,
            longitude=cfg.weather.longitude,
            days=cfg.weather.days,
        )

    @task
    def t_transform(config_path: str) -> None:
        cfg = load_config(config_path)
        transform_dir(cfg.paths.raw_dir, cfg.paths.proc_dir)

    @task
    def t_ensure_schema(config_path: str) -> None:
        cfg = load_config(config_path)
        ensure_schema(cfg.paths.db_path)

    @task
    def t_load(config_path: str) -> None:
        cfg = load_config(config_path)
        load_dir_to_duckdb(cfg.paths.proc_dir, cfg.paths.db_path)

    @flow(name="weather_etl")
    def weather_etl_flow(config_path: str = "config/pipeline.toml") -> None:
        cfg = load_config(config_path)
        configure_logging(cfg.logging.level)

        t_extract(config_path)
        t_transform(config_path)
        t_ensure_schema(config_path)
        t_load(config_path)

# python3 -m src.pipeline --config config/pipeline.toml
if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(args.config)