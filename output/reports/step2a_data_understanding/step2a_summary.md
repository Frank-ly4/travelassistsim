# Step 2A Data Understanding Summary

## Files Profiled
- fact_case: `data/raw/cases/fact_case.csv` (25020 rows)
- dimensions: `9` files in `data/raw/dimensions/`

## Basic Summary
- Rows in fact_case: **25020**
- Columns in fact_case: **18**
- Duplicate rows by `case_id` (all duplicated occurrences): **40**
- Distinct duplicated `case_id`s: **20**
- Fully duplicated rows (all columns): **40**

## Foreign Key Checks
- Passed: **9/9**
- All configured foreign key checks passed ✅

## Date / Duration Profile
- case_created_min: **2025-01-01 00:19:58**
- case_created_max: **2025-12-31 23:56:33**
- duration_hours_valid_rows: **19867**
- duration_hours_negative_rows: **0**
- duration_hours_min: **1.0**
- duration_hours_p50: **20.0**
- duration_hours_p90: **81.0**
- duration_hours_p99: **213.34**
- duration_hours_max: **942.0**

## Generated Output Files
- `output\reports\step2a_data_understanding\dimension_summary.csv`
- `output\reports\step2a_data_understanding\dist_assigned_agent_id_top.csv`
- `output\reports\step2a_data_understanding\dist_assigned_team_id.csv`
- `output\reports\step2a_data_understanding\dist_assigned_team_id_with_team_name.csv`
- `output\reports\step2a_data_understanding\dist_case_type_id.csv`
- `output\reports\step2a_data_understanding\dist_case_type_id_with_case_type_name.csv`
- `output\reports\step2a_data_understanding\dist_current_status_id.csv`
- `output\reports\step2a_data_understanding\dist_current_status_id_with_status_name.csv`
- `output\reports\step2a_data_understanding\dist_escalated_flag.csv`
- `output\reports\step2a_data_understanding\dist_intake_channel_id.csv`
- `output\reports\step2a_data_understanding\dist_intake_channel_id_with_channel_name.csv`
- `output\reports\step2a_data_understanding\dist_is_vip_case_flag.csv`
- `output\reports\step2a_data_understanding\dist_priority_id.csv`
- `output\reports\step2a_data_understanding\dist_priority_id_with_priority_name.csv`
- `output\reports\step2a_data_understanding\dist_reopened_flag.csv`
- `output\reports\step2a_data_understanding\duplicate_case_id_counts.csv`
- `output\reports\step2a_data_understanding\duplicate_case_id_rows.csv`
- `output\reports\step2a_data_understanding\duplicate_full_rows.csv`
- `output\reports\step2a_data_understanding\fact_case_cost_by_case_type.csv`
- `output\reports\step2a_data_understanding\fact_case_cost_summary.csv`
- `output\reports\step2a_data_understanding\fact_case_duration_buckets.csv`
- `output\reports\step2a_data_understanding\fact_case_duration_hours.csv`
- `output\reports\step2a_data_understanding\fact_case_fk_checks.csv`
- `output\reports\step2a_data_understanding\fact_case_fk_unmatched_samples.csv`
- `output\reports\step2a_data_understanding\fact_case_monthly_intake.csv`
- `output\reports\step2a_data_understanding\fact_case_null_profile.csv`
- `output\reports\step2a_data_understanding\step2a_summary.md`
