import os
from pathlib import Path
import sys
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetSelection,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset_check,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
import snowflake.connector
import yaml

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.saas_sources_pipeline import run_ingestion

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DBT_PROFILES_DIR = Path.home() / ".dbt"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)
dbt_project.prepare_if_dev()


def _get_snowflake_connection():
    profiles_path = DBT_PROFILES_DIR / "profiles.yml"
    creds = {}
    if profiles_path.exists():
        with open(profiles_path, "r", encoding="utf-8") as f:
            profiles = yaml.safe_load(f) or {}
        profile_cfg = next(
            (cfg for name, cfg in profiles.items() if name != "config" and isinstance(cfg, dict)),
            None,
        )
        if profile_cfg and "outputs" in profile_cfg:
            target_name = profile_cfg.get("target", "dev")
            creds = profile_cfg["outputs"].get(target_name) or next(
                iter(profile_cfg["outputs"].values()), {}
            )

    return snowflake.connector.connect(
        account=creds.get("account") or os.environ["SNOWFLAKE_ACCOUNT"],
        user=creds.get("user") or os.environ["SNOWFLAKE_USER"],
        password=creds.get("password") or os.environ["SNOWFLAKE_PASSWORD"],
        role=creds.get("role") or os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=creds.get("warehouse") or os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=creds.get("database") or os.environ.get("SNOWFLAKE_DATABASE", "SAAS_ELT_DB"),
        schema="RAW_SAAS_DATA",
    )


def _check_raw_table(table_name: str) -> AssetCheckResult:
    conn = _get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM SAAS_ELT_DB.RAW_SAAS_DATA.{table_name}")
            row_count = int(cur.fetchone()[0] or 0)
    finally:
        conn.close()

    passed = row_count > 0
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": row_count},
        description=(
            f"{table_name} has {row_count} rows"
            if passed
            else f"{table_name} is empty (0 rows)"
        ),
    )


# 1. Upstream Multi-Asset (1 dlt run -> 3 distinct Snowflake raw tables)
@multi_asset(
    compute_kind="dlt",
    outs={
        "raw_market_rates": AssetOut(
            key=["raw_saas_data", "raw_market_rates"],
            group_name="ingestion",
        ),
        "raw_support_tickets": AssetOut(
            key=["raw_saas_data", "raw_support_tickets"],
            group_name="ingestion",
        ),
        "raw_saas_accounts": AssetOut(
            key=["raw_saas_data", "raw_saas_accounts"],
            group_name="ingestion",
        ),
    },
)
def dlt_multi_source_saas_ingestion(context: AssetExecutionContext):
    context.log.info("Running dlt multi-source ingestion into Snowflake...")
    load_info = run_ingestion(destination="snowflake")
    context.log.info(f"dlt load complete: {load_info}")
    yield MaterializeResult(asset_key=["raw_saas_data", "raw_market_rates"])
    yield MaterializeResult(asset_key=["raw_saas_data", "raw_support_tickets"])
    yield MaterializeResult(asset_key=["raw_saas_data", "raw_saas_accounts"])


@asset_check(
    asset=AssetKey(["raw_saas_data", "raw_market_rates"]),
    description="Verify raw_market_rates contains rows in Snowflake",
    blocking=True,
)
def check_raw_market_rates_rows(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    context.log.info("Checking row count for SAAS_ELT_DB.RAW_SAAS_DATA.RAW_MARKET_RATES...")
    return _check_raw_table("RAW_MARKET_RATES")


@asset_check(
    asset=AssetKey(["raw_saas_data", "raw_support_tickets"]),
    description="Verify raw_support_tickets contains rows in Snowflake",
    blocking=True,
)
def check_raw_support_tickets_rows(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    context.log.info("Checking row count for SAAS_ELT_DB.RAW_SAAS_DATA.RAW_SUPPORT_TICKETS...")
    return _check_raw_table("RAW_SUPPORT_TICKETS")


@asset_check(
    asset=AssetKey(["raw_saas_data", "raw_saas_accounts"]),
    description="Verify raw_saas_accounts contains rows in Snowflake",
    blocking=True,
)
def check_raw_saas_accounts_rows(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    context.log.info("Checking row count for SAAS_ELT_DB.RAW_SAAS_DATA.RAW_SAAS_ACCOUNTS...")
    return _check_raw_table("RAW_SAAS_ACCOUNTS")


# 2. Downstream Transformation Assets (dbt staging, snapshots, marts, tests)
@dbt_assets(manifest=dbt_project.manifest_path)
def saas_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# 3. End-to-End Job & Hourly Schedule
full_elt_job = define_asset_job(
    name="full_saas_elt_pipeline",
    selection=AssetSelection.all(),
)

hourly_elt_schedule = ScheduleDefinition(
    job=full_elt_job,
    cron_schedule="0 * * * *",
)

defs = Definitions(
    assets=[dlt_multi_source_saas_ingestion, saas_dbt_assets],
    asset_checks=[
        check_raw_market_rates_rows,
        check_raw_support_tickets_rows,
        check_raw_saas_accounts_rows,
    ],
    jobs=[full_elt_job],
    schedules=[hourly_elt_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)