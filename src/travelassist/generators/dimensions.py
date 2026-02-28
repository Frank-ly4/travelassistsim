from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from travelassist.generators.common import (
    ensure_step2a_dirs,
    get_faker,
    get_settings,
    init_random_seed,
    write_csv,
)
from travelassist.logging_utils import setup_logger
from travelassist.paths import path_from_root


@dataclass
class GeneratedDimensions:
    case_types: pd.DataFrame
    statuses: pd.DataFrame
    priorities: pd.DataFrame
    channels: pd.DataFrame
    policy_plans: pd.DataFrame
    teams: pd.DataFrame
    agents: pd.DataFrame
    countries: pd.DataFrame
    providers: pd.DataFrame


def _build_dim_case_type() -> pd.DataFrame:
    rows = [
        # case_type_id, case_type_name, domain, complexity, requires_documents, typical_cost_band
        (1, "Medical Emergency", "Medical", "High", True, "High"),
        (2, "Outpatient Sickness", "Medical", "Low", True, "Medium"),
        (3, "Flight Cancellation", "Travel", "Low", False, "Low"),
        (4, "Lost Baggage", "Travel", "Low", False, "Low"),
        (5, "Medical Evacuation", "Medical", "Critical", True, "Very High"),
        (6, "Hospital Admission Guarantee", "Medical", "High", True, "High"),
        (7, "Trip Interruption", "Travel", "Medium", True, "Medium"),
        (8, "Repatriation Coordination", "Medical", "Critical", True, "Very High"),
        (9, "Prescription Assistance", "Medical", "Low", False, "Low"),
        (10, "Telemedicine Referral", "Medical", "Low", False, "Low"),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "case_type_id",
            "case_type_name",
            "domain",
            "complexity",
            "requires_documents",
            "typical_cost_band",
        ],
    )


def _build_dim_status() -> pd.DataFrame:
    rows = [
        # status_id, status_name, stage_order, is_terminal, stage_group
        (1, "Case Created", 1, False, "Intake"),
        (2, "Triage Started", 2, False, "Intake"),
        (3, "Provider Contacted", 3, False, "External Coordination"),
        (4, "Pending Documents", 4, False, "Documentation"),
        (5, "Under Review", 5, False, "Review"),
        (6, "Escalated", 6, False, "Escalation"),
        (7, "Approved", 7, False, "Decision"),
        (8, "Denied", 7, False, "Decision"),
        (9, "Arranged / Reimbursed", 8, False, "Fulfillment"),
        (10, "Closed", 9, True, "Closure"),
        (11, "Reopened", 10, False, "Closure"),
    ]
    return pd.DataFrame(
        rows,
        columns=["status_id", "status_name", "stage_order", "is_terminal", "stage_group"],
    )


def _build_dim_priority() -> pd.DataFrame:
    rows = [
        (1, "Low", 720),
        (2, "Medium", 240),
        (3, "High", 60),
        (4, "Critical", 15),
    ]
    return pd.DataFrame(rows, columns=["priority_id", "priority_name", "sla_first_response_minutes"])


def _build_dim_channel() -> pd.DataFrame:
    rows = [
        (1, "Phone", "Synchronous"),
        (2, "Email", "Asynchronous"),
        (3, "App", "Digital"),
        (4, "Web Portal", "Digital"),
        (5, "Provider Hotline", "Synchronous"),
    ]
    return pd.DataFrame(rows, columns=["channel_id", "channel_name", "channel_group"])


def _build_dim_policy_plan() -> pd.DataFrame:
    rows = [
        (1, "Basic", 0.75, False),
        (2, "Standard", 1.00, True),
        (3, "Premium", 1.35, True),
        (4, "Corporate", 1.20, True),
        (5, "VIP", 1.60, True),
    ]
    return pd.DataFrame(
        rows,
        columns=["policy_plan_id", "policy_plan_name", "coverage_multiplier", "priority_support_included"],
    )


def _build_dim_team() -> pd.DataFrame:
    rows = [
        (1, "Frontline Intake", "Intake"),
        (2, "Medical Assistance", "Medical Ops"),
        (3, "Travel Assistance", "Travel Ops"),
        (4, "Escalations Desk", "Escalations"),
        (5, "Cost Containment", "Finance Ops"),
        (6, "Night Shift Global", "Operations"),
    ]
    return pd.DataFrame(rows, columns=["team_id", "team_name", "department"])


def _build_dim_country() -> pd.DataFrame:
    # Controlled list = cleaner analytics later
    rows = [
        ("TH", "Thailand", "APAC", 1.00, 1.00),
        ("JP", "Japan", "APAC", 1.20, 0.95),
        ("KR", "South Korea", "APAC", 1.10, 0.95),
        ("SG", "Singapore", "APAC", 1.30, 0.90),
        ("MY", "Malaysia", "APAC", 0.95, 1.05),
        ("VN", "Vietnam", "APAC", 0.85, 1.10),
        ("ID", "Indonesia", "APAC", 0.90, 1.15),
        ("PH", "Philippines", "APAC", 0.88, 1.10),
        ("CN", "China", "APAC", 1.05, 1.05),
        ("IN", "India", "APAC", 0.92, 1.10),
        ("AE", "UAE", "MEA", 1.25, 0.95),
        ("TR", "Turkey", "EMEA", 0.98, 1.05),
        ("GB", "United Kingdom", "EMEA", 1.35, 0.95),
        ("FR", "France", "EMEA", 1.30, 0.95),
        ("DE", "Germany", "EMEA", 1.25, 0.95),
        ("ES", "Spain", "EMEA", 1.10, 1.00),
        ("IT", "Italy", "EMEA", 1.15, 1.00),
        ("US", "United States", "Americas", 1.50, 1.05),
        ("CA", "Canada", "Americas", 1.20, 1.00),
        ("MX", "Mexico", "Americas", 0.95, 1.10),
        ("BR", "Brazil", "Americas", 1.00, 1.15),
        ("AU", "Australia", "APAC", 1.25, 0.95),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "country_code",
            "country_name",
            "region_group",
            "cost_index",
            "response_delay_multiplier",
        ],
    )


def _build_dim_agents(fake, teams_df: pd.DataFrame, n_agents: int = 60) -> pd.DataFrame:
    team_ids = teams_df["team_id"].tolist()
    team_names = dict(zip(teams_df["team_id"], teams_df["team_name"]))

    rows = []
    for agent_id in range(1, n_agents + 1):
        team_id = int(fake.random_element(elements=team_ids))
        seniority = fake.random_element(elements=["Junior", "Associate", "Senior", "Lead"])
        language_band = fake.random_element(elements=["EN", "EN+TH", "EN+JP", "EN+FR", "Multi"])
        shift = fake.random_element(elements=["Day", "Evening", "Night"])
        rows.append(
            {
                "agent_id": agent_id,
                "agent_code": f"AGT-{agent_id:04d}",
                "agent_name": fake.name(),
                "team_id": team_id,
                "team_name": team_names[team_id],
                "seniority": seniority,
                "language_band": language_band,
                "shift_name": shift,
                "active_flag": True,
            }
        )
    return pd.DataFrame(rows)


def _build_dim_providers(fake, countries_df: pd.DataFrame, n_providers: int = 180) -> pd.DataFrame:
    country_rows = countries_df.to_dict(orient="records")

    provider_types = ["Hospital", "Clinic", "Ambulance", "Air Ambulance", "Travel Vendor", "Pharmacy"]
    provider_type_weights = [0.34, 0.24, 0.12, 0.04, 0.18, 0.08]

    rows = []
    for provider_id in range(1, n_providers + 1):
        c = fake.random_element(elements=country_rows)
        provider_type = fake.random_element(elements=provider_types)
        network_status = fake.random_element(elements=["In-Network", "Out-of-Network"])
        response_reliability = fake.random_int(min=55, max=99)  # proxy score
        cost_index_adj = round(fake.pyfloat(min_value=0.85, max_value=1.35, right_digits=2), 2)

        suffix = fake.random_element(elements=["Group", "Center", "Hospital", "Clinic", "Services"])
        provider_name = f"{fake.company()} {suffix}"

        rows.append(
            {
                "provider_id": provider_id,
                "provider_name": provider_name,
                "provider_type": provider_type,
                "country_code": c["country_code"],
                "country_name": c["country_name"],
                "region_group": c["region_group"],
                "network_status": network_status,
                "response_reliability_score": response_reliability,
                "provider_cost_index_adj": cost_index_adj,
                "is_active": True,
            }
        )
    return pd.DataFrame(rows)


def generate_dimensions() -> GeneratedDimensions:
    settings = get_settings()
    seed = init_random_seed(settings)
    fake = get_faker()
    ensure_step2a_dirs()

    logger = setup_logger("travelassist.step2a.dimensions")
    logger.info("Generating dimensions with seed=%s", seed)

    case_types = _build_dim_case_type()
    statuses = _build_dim_status()
    priorities = _build_dim_priority()
    channels = _build_dim_channel()
    policy_plans = _build_dim_policy_plan()
    teams = _build_dim_team()
    countries = _build_dim_country()
    agents = _build_dim_agents(fake=fake, teams_df=teams, n_agents=60)
    providers = _build_dim_providers(fake=fake, countries_df=countries, n_providers=180)

    outputs = {
        "data/raw/dimensions/dim_case_type.csv": case_types,
        "data/raw/dimensions/dim_status.csv": statuses,
        "data/raw/dimensions/dim_priority.csv": priorities,
        "data/raw/dimensions/dim_channel.csv": channels,
        "data/raw/dimensions/dim_policy_plan.csv": policy_plans,
        "data/raw/dimensions/dim_team.csv": teams,
        "data/raw/dimensions/dim_agent.csv": agents,
        "data/raw/dimensions/dim_country.csv": countries,
        "data/raw/dimensions/dim_provider.csv": providers,
    }

    for rel_path, df in outputs.items():
        out_path = write_csv(df, rel_path)
        logger.info("Wrote %-45s rows=%s", str(path_from_root(rel_path).relative_to(path_from_root("."))), len(df))

    logger.info("Dimension generation complete.")
    return GeneratedDimensions(
        case_types=case_types,
        statuses=statuses,
        priorities=priorities,
        channels=channels,
        policy_plans=policy_plans,
        teams=teams,
        agents=agents,
        countries=countries,
        providers=providers,
    )


def load_dimensions_from_csv() -> GeneratedDimensions:
    base = "data/raw/dimensions"
    return GeneratedDimensions(
        case_types=pd.read_csv(path_from_root(f"{base}/dim_case_type.csv")),
        statuses=pd.read_csv(path_from_root(f"{base}/dim_status.csv")),
        priorities=pd.read_csv(path_from_root(f"{base}/dim_priority.csv")),
        channels=pd.read_csv(path_from_root(f"{base}/dim_channel.csv")),
        policy_plans=pd.read_csv(path_from_root(f"{base}/dim_policy_plan.csv")),
        teams=pd.read_csv(path_from_root(f"{base}/dim_team.csv")),
        agents=pd.read_csv(path_from_root(f"{base}/dim_agent.csv")),
        countries=pd.read_csv(path_from_root(f"{base}/dim_country.csv")),
        providers=pd.read_csv(path_from_root(f"{base}/dim_provider.csv")),
    )