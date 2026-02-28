from pathlib import Path


def project_root() -> Path:
    # src/travelassist/paths.py -> root is 3 levels up
    return Path(__file__).resolve().parents[2]


def path_from_root(relative_path: str) -> Path:
    return project_root() / relative_path