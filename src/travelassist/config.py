from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .paths import path_from_root


@dataclass
class AppConfig:
    raw: dict[str, Any]


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def load_settings() -> dict[str, Any]:
    return _load_yaml(path_from_root("config/settings.yaml"))


def load_kpis() -> dict[str, Any]:
    return _load_yaml(path_from_root("config/kpis.yaml"))