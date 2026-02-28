from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from travelassist.logging_utils import setup_logger
from travelassist.paths import path_from_root


@dataclass
class FKCheckResult:
    fact_column: str
    dim_table: str
    dim_key_column: str
    fact_rows: int
    fact_null_rows: int
    fact_distinct_non_null: int
    dim_distinct_keys: int
    unmatched_distinct_count: int
    unmatched_row_count: int
    pass_check: bool


# ---------- Helpers ----------

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _normalize_key_series(series: pd.Series) -> pd.Series:
    """
    Normalize mixed key types (e.g., 38.0 vs 38, strings, country codes).
    Returns pandas StringDtype series with standardized uppercase text keys.
    """
    s = series.astype("string").str.strip()

    # Normalize common null-ish strings
    s = s.replace(
        {
            "": pd.NA,
            "NAN": pd.NA,
            "NaN": pd.NA,
            "None": pd.NA,
            "NONE": pd.NA,
            "<NA>": pd.NA,
        }
    )

    # Remove trailing .0 on numeric-like IDs written as float strings
    s = s.str.replace(r"\.0$", "", regex=True)

    # Uppercase for robust matching (safe for numeric strings too)
    s = s.str.upper()

    return s


def _read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    return pd.read_csv(path)


def _infer_dim_name_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: first non-id-ish string column
    for c in df.columns:
        cl = c.lower()
        if cl.endswith("_id") or cl.endswith("_code"):
            continue
        if pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object:
            return c
    return None


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


# ---------- Main profiler ----------

def run_data_understanding() -> dict[str, Any]:
    logger = setup_logger("travelassist.step2a.data_understanding")

    root = path_from_root("")
    dims_dir = path_from_root("data/raw/dimensions")
    cases_path = path_from_root("data/raw/cases/fact_case.csv")
    out_dir = path_from_root("output/reports/step2a_data_understanding")
    _ensure_dir(out_dir)

    logger.info("Starting Step 2A data understanding profile")
    logger.info("Project root: %s", root)

    # Load dimensions
    dim_files = sorted(dims_dir.glob("dim_*.csv"))
    if not dim_files:
        raise FileNotFoundError(f"No dimension files found in {dims_dir}")

    dims: dict[str, pd.DataFrame] = {}
    for fp in dim_files:
        df = pd.read_csv(fp)
        dims[fp.stem] = df
        logger.info("Loaded %-20s rows=%s cols=%s", fp.name, len(df), len(df.columns))

    # Load fact_case
    fact = _read_csv_required(cases_path)
    logger.info("Loaded fact_case.csv rows=%s cols=%s", len(fact), len(fact.columns))

    # ---------- Basic fact profile ----------
    basic_summary: dict[str, Any] = {
        "fact_case_rows": int(len(fact)),
        "fact_case_columns": int(len(fact.columns)),
        "fact_case_column_names": list(fact.columns),
    }

    # Duplicate checks
    duplicate_case_id_mask = (
        fact.duplicated(subset=["case_id"], keep=False)
        if "case_id" in fact.columns
        else pd.Series(False, index=fact.index)
    )
    duplicate_full_row_mask = fact.duplicated(keep=False)

    dup_case_id_df = fact.loc[duplicate_case_id_mask].copy()
    dup_full_df = fact.loc[duplicate_full_row_mask].copy()

    # Robust duplicate count logic across pandas versions
    if "case_id" in fact.columns and not dup_case_id_df.empty:
        vc = fact["case_id"].value_counts(dropna=False)

        # Build a stable 2-column DataFrame regardless of pandas version naming behavior
        dup_case_id_counts = pd.DataFrame({
            "case_id": vc.index.astype("string"),
            "duplicate_count": vc.values,
        })

        dup_case_id_counts["duplicate_count"] = pd.to_numeric(
            dup_case_id_counts["duplicate_count"], errors="coerce"
        )

        dup_case_id_counts = dup_case_id_counts[
            dup_case_id_counts["duplicate_count"] > 1
        ].sort_values(["duplicate_count", "case_id"], ascending=[False, True])
    else:
        dup_case_id_counts = pd.DataFrame(columns=["case_id", "duplicate_count"])

    dup_case_id_df.to_csv(out_dir / "duplicate_case_id_rows.csv", index=False)
    dup_case_id_counts.to_csv(out_dir / "duplicate_case_id_counts.csv", index=False)
    dup_full_df.to_csv(out_dir / "duplicate_full_rows.csv", index=False)

    basic_summary["duplicate_case_id_rows"] = int(duplicate_case_id_mask.sum()) if "case_id" in fact.columns else 0
    basic_summary["duplicate_case_id_distinct_ids"] = int(dup_case_id_counts.shape[0])
    basic_summary["duplicate_full_rows"] = int(duplicate_full_row_mask.sum())

    logger.info(
        "Duplicates: case_id_rows=%s distinct_dup_case_ids=%s full_duplicate_rows=%s",
        basic_summary["duplicate_case_id_rows"],
        basic_summary["duplicate_case_id_distinct_ids"],
        basic_summary["duplicate_full_rows"],
    )

    # Null profile
    null_profile = pd.DataFrame({
        "column_name": fact.columns,
        "dtype": [str(fact[c].dtype) for c in fact.columns],
        "null_count": [int(fact[c].isna().sum()) for c in fact.columns],
        "null_pct": [round(float(fact[c].isna().mean() * 100), 4) for c in fact.columns],
        "n_unique_excluding_null": [int(fact[c].nunique(dropna=True)) for c in fact.columns],
    }).sort_values(["null_pct", "column_name"], ascending=[False, True])
    null_profile.to_csv(out_dir / "fact_case_null_profile.csv", index=False)

    # ---------- Foreign key checks ----------
    fk_mappings = [
        # fact column, dim table, preferred dim key column
        ("case_type_id", "dim_case_type", "case_type_id"),
        ("priority_id", "dim_priority", "priority_id"),
        ("policy_plan_id", "dim_policy_plan", "policy_plan_id"),
        ("origin_country_code", "dim_country", "country_code"),
        ("primary_provider_id", "dim_provider", "provider_id"),
        ("intake_channel_id", "dim_channel", "channel_id"),
        ("assigned_team_id", "dim_team", "team_id"),
        ("assigned_agent_id", "dim_agent", "agent_id"),
        ("current_status_id", "dim_status", "status_id"),
    ]

    fk_results: list[FKCheckResult] = []
    fk_unmatched_samples: list[pd.DataFrame] = []

    for fact_col, dim_name, preferred_dim_key in fk_mappings:
        if fact_col not in fact.columns:
            logger.warning("Skipping FK check; missing fact column: %s", fact_col)
            continue
        if dim_name not in dims:
            logger.warning("Skipping FK check; missing dim file: %s", dim_name)
            continue

        dim_df = dims[dim_name]
        dim_key_col = preferred_dim_key if preferred_dim_key in dim_df.columns else dim_df.columns[0]

        fact_keys = _normalize_key_series(fact[fact_col])
        dim_keys = _normalize_key_series(dim_df[dim_key_col])

        fact_non_null = fact_keys.dropna()
        dim_non_null = dim_keys.dropna()

        dim_key_set = set(dim_non_null.unique().tolist())
        unmatched_mask = fact_keys.notna() & ~fact_keys.isin(dim_key_set)

        unmatched_distinct = sorted(pd.Series(fact_keys[unmatched_mask].unique()).dropna().astype(str).tolist())
        unmatched_row_count = int(unmatched_mask.sum())

        fk_results.append(
            FKCheckResult(
                fact_column=fact_col,
                dim_table=dim_name,
                dim_key_column=dim_key_col,
                fact_rows=int(len(fact)),
                fact_null_rows=int(fact_keys.isna().sum()),
                fact_distinct_non_null=int(fact_non_null.nunique()),
                dim_distinct_keys=int(dim_non_null.nunique()),
                unmatched_distinct_count=int(len(unmatched_distinct)),
                unmatched_row_count=unmatched_row_count,
                pass_check=(len(unmatched_distinct) == 0),
            )
        )

        if unmatched_row_count > 0:
            sample = fact.loc[unmatched_mask, [c for c in ["case_id", fact_col] if c in fact.columns]].copy()
            sample["dim_table"] = dim_name
            sample["dim_key_column"] = dim_key_col
            sample["normalized_fact_key"] = fact_keys[unmatched_mask].values
            fk_unmatched_samples.append(sample.head(50))

        logger.info(
            "FK %-20s -> %-15s (%s): unmatched_distinct=%s unmatched_rows=%s",
            fact_col, dim_name, dim_key_col, len(unmatched_distinct), unmatched_row_count
        )

    fk_df = pd.DataFrame([r.__dict__ for r in fk_results]).sort_values(["pass_check", "fact_column"])
    fk_df.to_csv(out_dir / "fact_case_fk_checks.csv", index=False)

    if fk_unmatched_samples:
        pd.concat(fk_unmatched_samples, ignore_index=True).to_csv(out_dir / "fact_case_fk_unmatched_samples.csv", index=False)
    else:
        pd.DataFrame(columns=["case_id", "fact_column_value", "dim_table"]).to_csv(
            out_dir / "fact_case_fk_unmatched_samples.csv", index=False
        )

    # ---------- Date + duration profile ----------
    date_profile: dict[str, Any] = {}
    if "case_created_ts" in fact.columns:
        created_ts = _safe_to_datetime(fact["case_created_ts"])
        date_profile["case_created_min"] = None if created_ts.isna().all() else str(created_ts.min())
        date_profile["case_created_max"] = None if created_ts.isna().all() else str(created_ts.max())

        monthly_intake = (
            created_ts.dt.to_period("M")
            .astype("string")
            .value_counts(dropna=True)
            .sort_index()
            .reset_index()
        )
        monthly_intake.columns = ["year_month", "case_count"]
        monthly_intake.to_csv(out_dir / "fact_case_monthly_intake.csv", index=False)
    else:
        monthly_intake = pd.DataFrame(columns=["year_month", "case_count"])
        monthly_intake.to_csv(out_dir / "fact_case_monthly_intake.csv", index=False)

    if {"case_created_ts", "case_closed_ts"}.issubset(set(fact.columns)):
        created_ts = _safe_to_datetime(fact["case_created_ts"])
        closed_ts = _safe_to_datetime(fact["case_closed_ts"])
        duration_hours = (closed_ts - created_ts).dt.total_seconds() / 3600.0

        duration_df = pd.DataFrame({"case_id": fact.get("case_id"), "duration_hours": duration_hours})
        duration_df.to_csv(out_dir / "fact_case_duration_hours.csv", index=False)

        valid_duration = duration_hours.dropna()
        date_profile["duration_hours_valid_rows"] = int(valid_duration.shape[0])
        date_profile["duration_hours_negative_rows"] = int((valid_duration < 0).sum())
        if not valid_duration.empty:
            date_profile["duration_hours_min"] = round(float(valid_duration.min()), 4)
            date_profile["duration_hours_p50"] = round(float(valid_duration.quantile(0.50)), 4)
            date_profile["duration_hours_p90"] = round(float(valid_duration.quantile(0.90)), 4)
            date_profile["duration_hours_p99"] = round(float(valid_duration.quantile(0.99)), 4)
            date_profile["duration_hours_max"] = round(float(valid_duration.max()), 4)

            # Save binned durations for quick charting later
            bins = [-1e9, 0, 1, 4, 12, 24, 48, 72, 168, 1e9]
            labels = [
                "<0h",
                "0-1h",
                "1-4h",
                "4-12h",
                "12-24h",
                "24-48h",
                "48-72h",
                "72-168h",
                "168h+",
            ]
            binned = pd.cut(valid_duration, bins=bins, labels=labels, include_lowest=True, right=True)
            binned.value_counts().sort_index().rename_axis("duration_bucket").reset_index(name="case_count").to_csv(
                out_dir / "fact_case_duration_buckets.csv", index=False
            )
        else:
            pd.DataFrame(columns=["duration_bucket", "case_count"]).to_csv(
                out_dir / "fact_case_duration_buckets.csv", index=False
            )
    else:
        pd.DataFrame(columns=["case_id", "duration_hours"]).to_csv(out_dir / "fact_case_duration_hours.csv", index=False)
        pd.DataFrame(columns=["duration_bucket", "case_count"]).to_csv(out_dir / "fact_case_duration_buckets.csv", index=False)

    # ---------- Basic distributions ----------
    def save_counts(df: pd.DataFrame, column: str, file_name: str) -> None:
        if column not in df.columns:
            pd.DataFrame(columns=[column, "count"]).to_csv(out_dir / file_name, index=False)
            return
        counts = (
            df[column]
            .astype("string")
            .fillna("<NULL>")
            .value_counts(dropna=False)
            .reset_index()
        )
        counts.columns = [column, "count"]
        counts.to_csv(out_dir / file_name, index=False)

    save_counts(fact, "case_type_id", "dist_case_type_id.csv")
    save_counts(fact, "priority_id", "dist_priority_id.csv")
    save_counts(fact, "current_status_id", "dist_current_status_id.csv")
    save_counts(fact, "intake_channel_id", "dist_intake_channel_id.csv")
    save_counts(fact, "assigned_team_id", "dist_assigned_team_id.csv")
    save_counts(fact, "assigned_agent_id", "dist_assigned_agent_id_top.csv")  # full list; can inspect top later

    for flag_col in ["escalated_flag", "reopened_flag", "is_vip_case_flag"]:
        save_counts(fact, flag_col, f"dist_{flag_col}.csv")

    # Label-enriched distributions (if dims are present)
    label_dist_specs = [
        ("case_type_id", "dim_case_type", "case_type_id", ["case_type_name"]),
        ("priority_id", "dim_priority", "priority_id", ["priority_name", "priority_label"]),
        ("current_status_id", "dim_status", "status_id", ["status_name"]),
        ("intake_channel_id", "dim_channel", "channel_id", ["channel_name"]),
        ("assigned_team_id", "dim_team", "team_id", ["team_name"]),
    ]

    for fact_col, dim_name, dim_key, label_candidates in label_dist_specs:
        if fact_col not in fact.columns or dim_name not in dims:
            continue
        dim_df = dims[dim_name].copy()
        if dim_key not in dim_df.columns:
            dim_key = dim_df.columns[0]
        label_col = _infer_dim_name_column(dim_df, label_candidates) or dim_key

        temp = fact[[fact_col]].copy()
        temp["_fact_key_norm"] = _normalize_key_series(temp[fact_col])

        dim_temp = dim_df[[dim_key, label_col]].copy()
        dim_temp["_dim_key_norm"] = _normalize_key_series(dim_temp[dim_key])

        merged = temp.merge(
            dim_temp[["_dim_key_norm", label_col]].drop_duplicates(),
            left_on="_fact_key_norm",
            right_on="_dim_key_norm",
            how="left",
        )
        merged[label_col] = merged[label_col].fillna("<UNMATCHED_OR_NULL>")

        out = (
            merged.groupby([label_col], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )

        safe_name = f"dist_{fact_col}_with_{label_col}.csv"
        out.to_csv(out_dir / safe_name, index=False)

    # ---------- Cost profile ----------
    if "estimated_case_cost_usd" in fact.columns:
        cost = pd.to_numeric(fact["estimated_case_cost_usd"], errors="coerce")
        cost_summary = pd.DataFrame(
            [
                {
                    "metric": "non_null_rows",
                    "value": int(cost.notna().sum()),
                },
                {"metric": "null_rows", "value": int(cost.isna().sum())},
                {"metric": "min", "value": None if cost.dropna().empty else float(cost.min())},
                {"metric": "p50", "value": None if cost.dropna().empty else float(cost.quantile(0.50))},
                {"metric": "p90", "value": None if cost.dropna().empty else float(cost.quantile(0.90))},
                {"metric": "p95", "value": None if cost.dropna().empty else float(cost.quantile(0.95))},
                {"metric": "p99", "value": None if cost.dropna().empty else float(cost.quantile(0.99))},
                {"metric": "max", "value": None if cost.dropna().empty else float(cost.max())},
                {"metric": "mean", "value": None if cost.dropna().empty else float(cost.mean())},
            ]
        )
        cost_summary.to_csv(out_dir / "fact_case_cost_summary.csv", index=False)

        # Cost by case type (label-enriched if possible)
        c = fact[["case_type_id", "estimated_case_cost_usd"]].copy()
        c["estimated_case_cost_usd"] = pd.to_numeric(c["estimated_case_cost_usd"], errors="coerce")
        if "dim_case_type" in dims:
            dct = dims["dim_case_type"].copy()
            key_col = "case_type_id" if "case_type_id" in dct.columns else dct.columns[0]
            name_col = _infer_dim_name_column(dct, ["case_type_name"]) or key_col
            merged_cost = c.merge(dct[[key_col, name_col]], left_on="case_type_id", right_on=key_col, how="left")
            by_case_type = (
                merged_cost.groupby([name_col], dropna=False)["estimated_case_cost_usd"]
                .agg(["count", "mean", "median", "min", "max"])
                .reset_index()
                .sort_values("mean", ascending=False)
            )
            by_case_type.to_csv(out_dir / "fact_case_cost_by_case_type.csv", index=False)

    # ---------- Dimension summary ----------
    dim_summary_rows = []
    for dim_name, dim_df in dims.items():
        dim_summary_rows.append(
            {
                "dim_table": dim_name,
                "row_count": int(len(dim_df)),
                "column_count": int(len(dim_df.columns)),
                "columns": ", ".join(dim_df.columns.astype(str).tolist()),
            }
        )
    dim_summary_df = pd.DataFrame(dim_summary_rows).sort_values("dim_table")
    dim_summary_df.to_csv(out_dir / "dimension_summary.csv", index=False)

    # ---------- Markdown summary ----------
    fk_pass_count = int(fk_df["pass_check"].sum()) if not fk_df.empty else 0
    fk_total_count = int(len(fk_df))

    lines: list[str] = []
    lines.append("# Step 2A Data Understanding Summary")
    lines.append("")
    lines.append("## Files Profiled")
    lines.append(f"- fact_case: `data/raw/cases/fact_case.csv` ({len(fact)} rows)")
    lines.append(f"- dimensions: `{len(dims)}` files in `data/raw/dimensions/`")
    lines.append("")
    lines.append("## Basic Summary")
    lines.append(f"- Rows in fact_case: **{basic_summary['fact_case_rows']}**")
    lines.append(f"- Columns in fact_case: **{basic_summary['fact_case_columns']}**")
    lines.append(f"- Duplicate rows by `case_id` (all duplicated occurrences): **{basic_summary['duplicate_case_id_rows']}**")
    lines.append(f"- Distinct duplicated `case_id`s: **{basic_summary['duplicate_case_id_distinct_ids']}**")
    lines.append(f"- Fully duplicated rows (all columns): **{basic_summary['duplicate_full_rows']}**")
    lines.append("")
    lines.append("## Foreign Key Checks")
    lines.append(f"- Passed: **{fk_pass_count}/{fk_total_count}**")
    if not fk_df.empty:
        failing = fk_df[~fk_df["pass_check"]]
        if failing.empty:
            lines.append("- All configured foreign key checks passed ✅")
        else:
            lines.append("- Failing checks:")
            for _, r in failing.iterrows():
                lines.append(
                    f"  - `{r['fact_column']}` -> `{r['dim_table']}.{r['dim_key_column']}` "
                    f"(unmatched rows={int(r['unmatched_row_count'])}, unmatched distinct={int(r['unmatched_distinct_count'])})"
                )
    lines.append("")
    lines.append("## Date / Duration Profile")
    for k, v in date_profile.items():
        lines.append(f"- {k}: **{v}**")
    lines.append("")
    lines.append("## Generated Output Files")
    for fp in sorted(out_dir.glob("*.csv")):
        lines.append(f"- `{fp.relative_to(root)}`")
    lines.append(f"- `{(out_dir / 'step2a_summary.md').relative_to(root)}`")
    lines.append("")

    summary_md_path = out_dir / "step2a_summary.md"
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")

    logger.info("Wrote summary report: %s", summary_md_path)
    logger.info("Step 2A data understanding complete")

    return {
        "out_dir": str(out_dir),
        "fact_rows": int(len(fact)),
        "dims_count": int(len(dims)),
        "fk_checks_passed": fk_pass_count,
        "fk_checks_total": fk_total_count,
        "duplicate_case_id_rows": int(basic_summary["duplicate_case_id_rows"]),
    }


def main() -> None:
    result = run_data_understanding()
    print("\nStep 2A data understanding complete.")
    print(f"  - Output folder: {result['out_dir']}")
    print(f"  - fact_case rows: {result['fact_rows']}")
    print(f"  - FK checks passed: {result['fk_checks_passed']}/{result['fk_checks_total']}")
    print(f"  - Duplicate case_id rows: {result['duplicate_case_id_rows']}")


if __name__ == "__main__":
    main()