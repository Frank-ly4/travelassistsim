from __future__ import annotations

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from travelassist.generators.common import (
    ensure_step2a_dirs,
    get_faker,
    get_settings,
    init_random_seed,
    write_csv,
)
from travelassist.generators.dimensions import GeneratedDimensions, load_dimensions_from_csv
from travelassist.logging_utils import setup_logger


def _weighted_case_type(case_types_df: pd.DataFrame) -> dict:
    rows = case_types_df.to_dict(orient="records")
    # weights chosen to create realistic mix and meaningful KPI patterns
    name_to_weight = {
        "Medical Emergency": 0.18,
        "Outpatient Sickness": 0.22,
        "Flight Cancellation": 0.16,
        "Lost Baggage": 0.10,
        "Medical Evacuation": 0.03,
        "Hospital Admission Guarantee": 0.08,
        "Trip Interruption": 0.10,
        "Repatriation Coordination": 0.02,
        "Prescription Assistance": 0.06,
        "Telemedicine Referral": 0.05,
    }
    weights = [name_to_weight.get(r["case_type_name"], 0.05) for r in rows]
    return random.choices(rows, weights=weights, k=1)[0]


def _priority_for_case(case_type_name: str) -> str:
    if case_type_name in {"Medical Evacuation", "Repatriation Coordination"}:
        return "Critical"
    if case_type_name in {"Medical Emergency", "Hospital Admission Guarantee"}:
        return random.choices(["High", "Critical"], weights=[0.7, 0.3], k=1)[0]
    if case_type_name in {"Outpatient Sickness", "Trip Interruption"}:
        return random.choices(["Medium", "High"], weights=[0.75, 0.25], k=1)[0]
    return random.choices(["Low", "Medium"], weights=[0.65, 0.35], k=1)[0]


def _initial_channel_id(channels_df: pd.DataFrame) -> int:
    rows = channels_df.to_dict(orient="records")
    weights_by_name = {
        "Phone": 0.50,
        "Email": 0.25,
        "App": 0.15,
        "Web Portal": 0.08,
        "Provider Hotline": 0.02,
    }
    chosen = random.choices(rows, weights=[weights_by_name.get(r["channel_name"], 0.05) for r in rows], k=1)[0]
    return int(chosen["channel_id"])


def _choose_country(countries_df: pd.DataFrame) -> dict:
    rows = countries_df.to_dict(orient="records")
    # weighted toward APAC + some EMEA + US to reflect common travel operations diversity
    weights_by_code = {
        "TH": 0.10, "JP": 0.08, "KR": 0.06, "SG": 0.04, "MY": 0.05, "VN": 0.05, "ID": 0.05, "PH": 0.04,
        "CN": 0.05, "IN": 0.04, "AE": 0.03, "TR": 0.03, "GB": 0.04, "FR": 0.03, "DE": 0.03, "ES": 0.03,
        "IT": 0.03, "US": 0.08, "CA": 0.03, "MX": 0.03, "BR": 0.03, "AU": 0.03,
    }
    weights = [weights_by_code.get(r["country_code"], 0.02) for r in rows]
    return random.choices(rows, weights=weights, k=1)[0]


def _choose_provider_for_country(providers_df: pd.DataFrame, country_code: str) -> dict:
    subset = providers_df[providers_df["country_code"] == country_code]
    if subset.empty:
        subset = providers_df
    rows = subset.to_dict(orient="records")

    # bias toward in-network providers
    weights = []
    for r in rows:
        w = 2.2 if r["network_status"] == "In-Network" else 1.0
        # slightly favor higher reliability
        w *= max(0.5, float(r["response_reliability_score"]) / 80.0)
        weights.append(w)
    return random.choices(rows, weights=weights, k=1)[0]


def _assign_team_agent(case_type_name: str, teams_df: pd.DataFrame, agents_df: pd.DataFrame) -> tuple[int, int]:
    team_name_pref = {
        "Medical Emergency": "Medical Assistance",
        "Outpatient Sickness": "Medical Assistance",
        "Medical Evacuation": "Escalations Desk",
        "Hospital Admission Guarantee": "Medical Assistance",
        "Repatriation Coordination": "Escalations Desk",
        "Flight Cancellation": "Travel Assistance",
        "Lost Baggage": "Travel Assistance",
        "Trip Interruption": "Travel Assistance",
        "Prescription Assistance": "Medical Assistance",
        "Telemedicine Referral": "Medical Assistance",
    }.get(case_type_name, "Frontline Intake")

    matching_team = teams_df[teams_df["team_name"] == team_name_pref]
    if matching_team.empty:
        matching_team = teams_df.sample(1, random_state=random.randint(1, 9999))

    team_id = int(matching_team.iloc[0]["team_id"])

    agents_subset = agents_df[(agents_df["team_id"] == team_id) & (agents_df["active_flag"] == True)]  # noqa: E712
    if agents_subset.empty:
        agents_subset = agents_df[agents_df["active_flag"] == True]  # noqa: E712

    agent_id = int(agents_subset.sample(1, random_state=random.randint(1, 999999)).iloc[0]["agent_id"])
    return team_id, agent_id


def _maybe_closed(created_ts: datetime, case_type_name: str) -> tuple[bool, datetime | None]:
    # More complex case types less likely to close quickly
    complex_cases = {"Medical Evacuation", "Repatriation Coordination", "Hospital Admission Guarantee"}
    close_prob = 0.82 if case_type_name not in complex_cases else 0.62
    is_closed = random.random() < close_prob
    if not is_closed:
        return False, None

    # closure duration (hours) by case category
    if case_type_name in {"Lost Baggage", "Flight Cancellation", "Telemedicine Referral"}:
        hours_to_close = max(1, int(np.random.lognormal(mean=2.5, sigma=0.5)))  # often same day
    elif case_type_name in {"Outpatient Sickness", "Prescription Assistance", "Trip Interruption"}:
        hours_to_close = max(2, int(np.random.lognormal(mean=3.0, sigma=0.6)))
    else:
        hours_to_close = max(6, int(np.random.lognormal(mean=4.1, sigma=0.7)))
    return True, created_ts + timedelta(hours=hours_to_close)


def generate_fact_cases(dimensions: GeneratedDimensions | None = None) -> pd.DataFrame:
    settings = get_settings()
    seed = init_random_seed(settings)
    fake = get_faker()
    ensure_step2a_dirs()

    logger = setup_logger("travelassist.step2a.cases")
    logger.info("Generating fact_case with seed=%s", seed)

    if dimensions is None:
        dimensions = load_dimensions_from_csv()

    row_target = int(settings.get("data_generation", {}).get("row_targets", {}).get("cases", 25000))
    logger.info("Target cases=%s", row_target)

    case_types_df = dimensions.case_types.copy()
    priorities_df = dimensions.priorities.copy()
    channels_df = dimensions.channels.copy()
    policy_plans_df = dimensions.policy_plans.copy()
    teams_df = dimensions.teams.copy()
    agents_df = dimensions.agents.copy()
    countries_df = dimensions.countries.copy()
    providers_df = dimensions.providers.copy()
    statuses_df = dimensions.statuses.copy()

    priority_name_to_id = dict(zip(priorities_df["priority_name"], priorities_df["priority_id"]))

    # current status choices for fact_case snapshot (status history comes in Step 2B)
    status_name_to_id = dict(zip(statuses_df["status_name"], statuses_df["status_id"]))
    snapshot_status_weights = {
        "Case Created": 0.02,
        "Triage Started": 0.04,
        "Provider Contacted": 0.07,
        "Pending Documents": 0.08,
        "Under Review": 0.06,
        "Escalated": 0.03,
        "Approved": 0.05,
        "Denied": 0.02,
        "Arranged / Reimbursed": 0.06,
        "Closed": 0.55,
        "Reopened": 0.02,
    }
    status_names = list(snapshot_status_weights.keys())
    status_weights = list(snapshot_status_weights.values())

    # Date range for generated data
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    end_date = datetime(2025, 12, 31, 23, 59, 59)
    total_seconds = int((end_date - start_date).total_seconds())

    records: list[dict] = []
    for i in range(1, row_target + 1):
        case_type = _weighted_case_type(case_types_df)
        case_type_name = str(case_type["case_type_name"])

        # created timestamp
        offset_sec = random.randint(0, total_seconds)
        created_ts = start_date + timedelta(seconds=offset_sec)

        # assign priority based on case type
        priority_name = _priority_for_case(case_type_name)
        priority_id = int(priority_name_to_id[priority_name])

        # country/provider
        origin_country = _choose_country(countries_df)
        provider = _choose_provider_for_country(providers_df, country_code=str(origin_country["country_code"]))

        # team/agent
        team_id, agent_id = _assign_team_agent(case_type_name, teams_df, agents_df)

        # intake channel and policy plan
        channel_id = _initial_channel_id(channels_df)
        policy_plan_id = int(
            random.choices(
                policy_plans_df["policy_plan_id"].tolist(),
                weights=[0.28, 0.34, 0.20, 0.12, 0.06],
                k=1,
            )[0]
        )

        # close/open snapshot
        is_closed, closed_ts = _maybe_closed(created_ts, case_type_name)

        current_status_name = "Closed" if is_closed else random.choices(status_names, weights=status_weights, k=1)[0]
        current_status_id = int(status_name_to_id[current_status_name])

        # simple cost estimate baseline (actuals will come in later event tables)
        country_cost_idx = float(origin_country["cost_index"])
        provider_cost_adj = float(provider["provider_cost_index_adj"])
        base_cost_by_type = {
            "Medical Emergency": 1800,
            "Outpatient Sickness": 300,
            "Flight Cancellation": 220,
            "Lost Baggage": 120,
            "Medical Evacuation": 18000,
            "Hospital Admission Guarantee": 3500,
            "Trip Interruption": 700,
            "Repatriation Coordination": 12000,
            "Prescription Assistance": 90,
            "Telemedicine Referral": 70,
        }.get(case_type_name, 250)

        estimated_cost_usd = round(base_cost_by_type * country_cost_idx * provider_cost_adj * random.uniform(0.8, 1.2), 2)

        # escalation and reopen indicators (placeholder snapshot flags; event truth comes later)
        escalated_flag = bool(
            case_type_name in {"Medical Evacuation", "Repatriation Coordination"}
            or random.random() < 0.12
        )
        reopened_flag = bool(is_closed and random.random() < 0.04)

        records.append(
            {
                "case_id": f"CAS-{100000 + i}",
                "customer_id": str(fake.uuid4()),
                "case_type_id": int(case_type["case_type_id"]),
                "priority_id": priority_id,
                "policy_plan_id": policy_plan_id,
                "origin_country_code": str(origin_country["country_code"]),
                "primary_provider_id": int(provider["provider_id"]),
                "intake_channel_id": channel_id,
                "assigned_team_id": team_id,
                "assigned_agent_id": agent_id,
                "case_created_ts": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "case_closed_ts": closed_ts.strftime("%Y-%m-%d %H:%M:%S") if closed_ts else None,
                "current_status_id": current_status_id,
                "estimated_case_cost_usd": estimated_cost_usd,
                "escalated_flag": escalated_flag,
                "reopened_flag": reopened_flag,
                "is_vip_case_flag": bool(policy_plan_id == 5),
                "source_system": "TravelAssistSim",
            }
        )

    df_cases = pd.DataFrame.from_records(records)

    # Deliberate data imperfections (small %)
    if len(df_cases) >= 100:
        # provider missing on a few rows
        idx_missing_provider = df_cases.sample(frac=0.005, random_state=seed).index
        df_cases.loc[idx_missing_provider, "primary_provider_id"] = pd.NA

        # inconsistent country code casing on a few rows (for later cleaning exercises)
        idx_lower_cc = df_cases.sample(frac=0.003, random_state=seed + 1).index
        df_cases.loc[idx_lower_cc, "origin_country_code"] = (
            df_cases.loc[idx_lower_cc, "origin_country_code"].astype(str).str.lower()
        )

        # duplicate a small handful of rows (simulate ingest duplicates)
        dup_sample = df_cases.sample(n=min(20, max(1, len(df_cases) // 1000)), random_state=seed + 2)
        df_cases = pd.concat([df_cases, dup_sample], ignore_index=True)

    out_path = write_csv(df_cases, "data/raw/cases/fact_case.csv")
    logger.info("Wrote fact_case to %s rows=%s", out_path, len(df_cases))
    return df_cases