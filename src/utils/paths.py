from __future__ import annotations
from pathlib import Path

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    if p.exists() and not p.is_dir():
        raise ValueError(f"Path exists but is not a directory: {p}")
    p.mkdir(parents=True, exist_ok=True)
    return p

def assert_dir_exists(path: str | Path) -> Path:
    p = Path(path)
    if not p.exists() or not p.is_dir():
        raise ValueError(f"Directory does not exist or is not a directory: {p}")
    return p