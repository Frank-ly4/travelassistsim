from __future__ import annotations

import json

from rich import print as rprint

from .config import load_kpis, load_settings
from .generators.cases import generate_fact_cases
from .generators.dimensions import generate_dimensions
from .paths import path_from_root


def init_dirs() -> None:
    folders = [
        "data/raw/cases",
        "data/raw/contacts",
        "data/raw/status_history",
        "data/raw/documents",
        "data/raw/notes",
        "data/raw/dimensions",
        "data/clean",
        "data/curated",
        "output/logs",
        "output/reports",
    ]
    for folder in folders:
        p = path_from_root(folder)
        p.mkdir(parents=True, exist_ok=True)

    # Create .gitkeep files in parent folders
    for keep in ["data/raw", "data/clean", "data/curated", "output/logs", "output/reports"]:
        keep_path = path_from_root(keep) / ".gitkeep"
        keep_path.parent.mkdir(parents=True, exist_ok=True)
        if not keep_path.exists():
            keep_path.write_text("", encoding="utf-8")

    rprint("[green]Directory scaffold initialized.[/green]")


def show_config() -> None:
    settings = load_settings()
    rprint("[bold cyan]Settings[/bold cyan]")
    rprint(json.dumps(settings, indent=2))


def show_kpis() -> None:
    kpis = load_kpis()
    rprint("[bold magenta]KPIs[/bold magenta]")
    for item in kpis.get("kpis", []):
        rprint(f"- [bold]{item['id']}[/bold]: {item['name']} ({item['category']})")


def verify_step1() -> None:
    required = [
        "config/settings.yaml",
        "config/kpis.yaml",
        "docs/project_scope.md",
        "docs/architecture.md",
    ]
    missing = [p for p in required if not path_from_root(p).exists()]
    if missing:
        rprint("[red]Step 1 incomplete. Missing files:[/red]")
        for m in missing:
            rprint(f"  - {m}")
        raise SystemExit(1)
    rprint("[green]Step 1 verification passed.[/green]")


def generate_dimensions_cmd() -> None:
    generate_dimensions()
    rprint("[green]Generated dimension tables in data/raw/dimensions/[/green]")


def generate_cases_cmd() -> None:
    df = generate_fact_cases()
    rprint(f"[green]Generated fact_case rows:[/green] {len(df)} -> data/raw/cases/fact_case.csv")


def generate_step2a_cmd() -> None:
    generate_dimensions()
    df = generate_fact_cases()
    rprint("[green]Step 2A generation complete.[/green]")
    rprint(f"  - Dimensions: data/raw/dimensions/")
    rprint(f"  - Cases: data/raw/cases/fact_case.csv ({len(df)} rows incl. a few duplicates)")


def main(argv: list[str] | None = None) -> None:
    import sys

    args = argv if argv is not None else sys.argv[1:]
    command = args[0] if args else "help"

    commands = {
        "init-dirs": init_dirs,
        "show-config": show_config,
        "show-kpis": show_kpis,
        "verify-step1": verify_step1,
        "generate-dimensions": generate_dimensions_cmd,
        "generate-cases": generate_cases_cmd,
        "generate-step2a": generate_step2a_cmd,
    }

    if command in ("help", "-h", "--help"):
        rprint(
            "[bold]TravelAssist CLI[/bold]\n"
            "Commands:\n"
            "  init-dirs\n"
            "  show-config\n"
            "  show-kpis\n"
            "  verify-step1\n"
            "  generate-dimensions\n"
            "  generate-cases\n"
            "  generate-step2a"
        )
        return

    if command not in commands:
        rprint(f"[red]Unknown command:[/red] {command}")
        raise SystemExit(1)

    commands[command]()