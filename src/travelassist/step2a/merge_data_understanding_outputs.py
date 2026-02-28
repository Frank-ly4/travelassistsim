from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from travelassist.logging_utils import setup_logger
from travelassist.paths import path_from_root


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_csv_if_exists(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    return pd.read_csv(path)


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _standardize_count_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the count column is named 'count'."""
    out = df.copy()
    if "count" in out.columns:
        return out

    # Try common alternates
    candidates = [c for c in out.columns if c.lower() in {"case_count", "rows", "n", "freq"}]
    if candidates:
        out = out.rename(columns={candidates[0]: "count"})
        return out

    # If exactly 2 columns and one isn't the key, assume second is count
    if out.shape[1] == 2:
        out.columns = [out.columns[0], "count"]
        return out

    return out


def _merge_dist_pair(
    qa_dir: Path,
    out_dir: Path,
    raw_file: str,
    labeled_file: str,
    output_file: str,
    raw_key_col: str,
    label_col_candidates: list[str],
) -> bool:
    """
    Merge:
      - raw distribution by ID (e.g., dist_case_type_id.csv)
      - labeled distribution by name (e.g., dist_case_type_id_with_case_type_name.csv)
    into a single analyst-friendly file with ID + label + count.
    """
    raw_path = qa_dir / raw_file
    labeled_path = qa_dir / labeled_file

    raw_df = _read_csv_if_exists(raw_path)
    labeled_df = _read_csv_if_exists(labeled_path)

    if raw_df is None or labeled_df is None:
        return False

    raw_df = _standardize_count_column(raw_df)
    labeled_df = _standardize_count_column(labeled_df)

    if raw_key_col not in raw_df.columns or "count" not in raw_df.columns:
        return False

    label_col = _pick_first_existing_column(labeled_df, label_col_candidates)
    if label_col is None or "count" not in labeled_df.columns:
        return False

    # Merge on count (works here because each ID maps to one label and counts should align)
    # We add occurrence index to protect against repeated counts causing cross-joins.
    left = raw_df[[raw_key_col, "count"]].copy()
    right = labeled_df[[label_col, "count"]].copy()

    left["count"] = pd.to_numeric(left["count"], errors="coerce")
    right["count"] = pd.to_numeric(right["count"], errors="coerce")

    left = left.sort_values([raw_key_col], kind="stable").reset_index(drop=True)
    right = right.sort_values([label_col], kind="stable").reset_index(drop=True)

    left["_occ"] = left.groupby("count", dropna=False).cumcount()
    right["_occ"] = right.groupby("count", dropna=False).cumcount()

    merged = left.merge(
        right,
        on=["count", "_occ"],
        how="outer",
        validate="one_to_one",
    ).drop(columns=["_occ"])

    # Sort by count descending for readability
    merged = merged.sort_values(["count", raw_key_col], ascending=[False, True], na_position="last")

    merged.to_csv(out_dir / output_file, index=False)
    return True


def _merge_flags_summary(qa_dir: Path, out_dir: Path) -> None:
    """
    Combine the three boolean flag distributions into one file for easier reading.
    """
    flag_files = [
        ("escalated_flag", "dist_escalated_flag.csv"),
        ("reopened_flag", "dist_reopened_flag.csv"),
        ("is_vip_case_flag", "dist_is_vip_case_flag.csv"),
    ]

    rows = []
    for flag_name, fn in flag_files:
        df = _read_csv_if_exists(qa_dir / fn)
        if df is None:
            continue
        df = _standardize_count_column(df)
        if "count" not in df.columns or df.shape[1] < 2:
            continue

        value_col = [c for c in df.columns if c != "count"][0]
        temp = df[[value_col, "count"]].copy()
        temp = temp.rename(columns={value_col: "flag_value"})
        temp["flag_name"] = flag_name
        rows.append(temp[["flag_name", "flag_value", "count"]])

    if rows:
        out = pd.concat(rows, ignore_index=True)
        out["count"] = pd.to_numeric(out["count"], errors="coerce")
        out = out.sort_values(["flag_name", "count"], ascending=[True, False])
        out.to_csv(out_dir / "dist_flags_combined.csv", index=False)


def _copy_selected_files(qa_dir: Path, out_dir: Path) -> None:
    """
    Copy a few already-human-readable files for convenience (optional).
    """
    keep_files = [
        "fact_case_cost_summary.csv",
        "fact_case_cost_by_case_type.csv",
        "fact_case_monthly_intake.csv",
        "fact_case_duration_buckets.csv",
        "fact_case_fk_checks.csv",
        "fact_case_null_profile.csv",
        "duplicate_case_id_counts.csv",
    ]
    for fn in keep_files:
        src = qa_dir / fn
        if src.exists():
            df = pd.read_csv(src)
            df.to_csv(out_dir / fn, index=False)


def _write_manifest(out_dir: Path, created_files: list[str]) -> None:
    lines = [
        "# Step 2A Data Understanding (Merged Review Outputs)",
        "",
        "This folder contains analyst-friendly merged views built from raw QA outputs in:",
        "`output/reports/step2a_data_understanding/`",
        "",
        "## Files",
    ]
    for fn in sorted(created_files):
        lines.append(f"- `{fn}`")
    (out_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")


def run_merge_step2a_outputs() -> dict[str, object]:
    logger = setup_logger("travelassist.step2a.merge_data_understanding_outputs")

    qa_dir = path_from_root("output/reports/step2a_data_understanding")
    out_dir = path_from_root("output/reports/step2a_data_understanding_merged")
    _ensure_dir(out_dir)

    if not qa_dir.exists():
        raise FileNotFoundError(
            f"QA output folder not found: {qa_dir}\n"
            "Run the Step 2A data understanding profiler first."
        )

    logger.info("Reading QA outputs from: %s", qa_dir)
    logger.info("Writing merged outputs to: %s", out_dir)

    created_files: list[str] = []

    merge_specs = [
        {
            "raw_file": "dist_case_type_id.csv",
            "labeled_file": "dist_case_type_id_with_case_type_name.csv",
            "output_file": "dist_case_type_combined.csv",
            "raw_key_col": "case_type_id",
            "label_col_candidates": ["case_type_name"],
        },
        {
            "raw_file": "dist_priority_id.csv",
            "labeled_file": "dist_priority_id_with_priority_name.csv",
            "output_file": "dist_priority_combined.csv",
            "raw_key_col": "priority_id",
            "label_col_candidates": ["priority_name", "priority_label"],
        },
        {
            "raw_file": "dist_current_status_id.csv",
            "labeled_file": "dist_current_status_id_with_status_name.csv",
            "output_file": "dist_status_combined.csv",
            "raw_key_col": "current_status_id",
            "label_col_candidates": ["status_name"],
        },
        {
            "raw_file": "dist_intake_channel_id.csv",
            "labeled_file": "dist_intake_channel_id_with_channel_name.csv",
            "output_file": "dist_channel_combined.csv",
            "raw_key_col": "intake_channel_id",
            "label_col_candidates": ["channel_name"],
        },
        {
            "raw_file": "dist_assigned_team_id.csv",
            "labeled_file": "dist_assigned_team_id_with_team_name.csv",
            "output_file": "dist_team_combined.csv",
            "raw_key_col": "assigned_team_id",
            "label_col_candidates": ["team_name"],
        },
    ]

    for spec in merge_specs:
        ok = _merge_dist_pair(
            qa_dir=qa_dir,
            out_dir=out_dir,
            raw_file=spec["raw_file"],
            labeled_file=spec["labeled_file"],
            output_file=spec["output_file"],
            raw_key_col=spec["raw_key_col"],
            label_col_candidates=spec["label_col_candidates"],
        )
        if ok:
            created_files.append(spec["output_file"])
            logger.info("Created merged file: %s", spec["output_file"])
        else:
            logger.warning(
                "Skipped merge (missing/invalid inputs): %s + %s",
                spec["raw_file"],
                spec["labeled_file"],
            )

    _merge_flags_summary(qa_dir, out_dir)
    if (out_dir / "dist_flags_combined.csv").exists():
        created_files.append("dist_flags_combined.csv")
        logger.info("Created merged file: dist_flags_combined.csv")

    # Copy a few useful existing QA files into the merged folder for convenience
    _copy_selected_files(qa_dir, out_dir)
    for fn in [
        "fact_case_cost_summary.csv",
        "fact_case_cost_by_case_type.csv",
        "fact_case_monthly_intake.csv",
        "fact_case_duration_buckets.csv",
        "fact_case_fk_checks.csv",
        "fact_case_null_profile.csv",
        "duplicate_case_id_counts.csv",
    ]:
        if (out_dir / fn).exists():
            created_files.append(fn)

    _write_manifest(out_dir, created_files)
    created_files.append("README.md")

    logger.info("Merged review outputs complete. Files created: %s", len(created_files))

    return {
        "source_dir": str(qa_dir),
        "out_dir": str(out_dir),
        "created_files_count": len(created_files),
        "created_files": sorted(created_files),
    }


def main() -> None:
    result = run_merge_step2a_outputs()
    print("\nStep 2A merged review outputs complete.")
    print(f"  - Source QA folder: {result['source_dir']}")
    print(f"  - Output folder: {result['out_dir']}")
    print(f"  - Files created: {result['created_files_count']}")


if __name__ == "__main__":
    main()