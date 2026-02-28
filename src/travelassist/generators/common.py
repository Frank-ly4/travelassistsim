from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import numpy as np
from faker import Faker

from travelassist.config import load_settings
from travelassist.paths import path_from_root


def get_settings() -> dict[str, Any]:
    return load_settings()


def init_random_seed(settings: dict[str, Any]) -> int:
    seed = int(settings.get("data_generation", {}).get("random_seed", 42))
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)
    return seed


def get_faker() -> Faker:
    return Faker()


def ensure_step2a_dirs() -> None:
    folders = [
        "data/raw/dimensions",
        "data/raw/cases",
    ]
    for folder in folders:
        path_from_root(folder).mkdir(parents=True, exist_ok=True)


def weighted_choice(items: list[Any], weights: list[float]) -> Any:
    return random.choices(items, weights=weights, k=1)[0]


def normalize_probs(weights: list[float]) -> list[float]:
    total = sum(weights)
    if total <= 0:
        raise ValueError("Weights must sum to > 0.")
    return [w / total for w in weights]


def write_csv(df, relative_path: str) -> Path:
    out_path = path_from_root(relative_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path