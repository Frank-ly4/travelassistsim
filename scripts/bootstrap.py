from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

FOLDERS = [
    "config",
    "docs",
    "data/raw/cases",
    "data/raw/contacts",
    "data/raw/status_history",
    "data/raw/documents",
    "data/raw/notes",
    "data/clean",
    "data/curated",
    "output/logs",
    "output/reports",
    "src/travelassist",
    "tests",
]

def main() -> None:
    for rel in FOLDERS:
        (ROOT / rel).mkdir(parents=True, exist_ok=True)
    print("Bootstrap folders created.")

if __name__ == "__main__":
    main()