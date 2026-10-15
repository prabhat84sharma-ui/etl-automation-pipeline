"""
Automated Pytest Suite for Data Quality Framework.
Tests null checks, uniqueness, range validation, referential integrity,
composite scoring, threshold enforcement, and JSON serialization.

Author: Prabhat Sharma (prabhat84sharma-ui)
"""

import json
from pathlib import Path
import pytest
import pandas as pd
import numpy as np

from src.data_quality import (
    DataQualityEngine,
    DataQualityBreachException,
    QualityReport,
    QualityCheckResult,
)


@pytest.fixture
def clean_orders_df() -> pd.DataFrame:
    """Fixture providing clean orders dataset with zero anomalies."""
    return pd.DataFrame({
        "order_id": [f"ORD-{i:03d}" for i in range(1, 11)],
        "order_line_item_id": [f"ORD-{i:03d}-L1" for i in range(1, 11)],
        "customer_key": [1, 2, 3, 1, 2, 3, 4, 1, 2, 3],
        "product_key": [10, 11, 12, 10, 11, 12, 10, 11, 12, 10],
        "date_key": [20261001 + i for i in range(10)],
        "quantity": [1, 2, 5, 3, 1, 4, 2, 1, 3, 2],
        "unit_price_usd": [50.0, 120.0, 30.0, 80.0, 95.0, 45.0, 110.0, 25.0, 60.0, 75.0],
        "discount_pct": [0.0, 5.0, 10.0, 0.0, 15.0, 0.0, 10.0, 5.0, 0.0, 20.0],
        "total_amount_usd": [50.0, 228.0, 135.0, 240.0, 80.75, 180.0, 198.0, 23.75, 180.0, 120.0],
    })


@pytest.fixture
def clean_customers_df() -> pd.DataFrame:
    """Fixture providing parent customer dimension."""
    return pd.DataFrame({
        "customer_key": [1, 2, 3, 4],
        "customer_id": ["CUST-001", "CUST-002", "CUST-003", "CUST-004"],
        "email": ["alice@corp.com", "bob@corp.com", "carol@corp.com", "dan@corp.com"],
    })


@pytest.fixture
def clean_products_df() -> pd.DataFrame:
    """Fixture providing parent product dimension."""
    return pd.DataFrame({
        "product_key": [10, 11, 12],
        "product_id": ["PROD-10", "PROD-11", "PROD-12"],
        "product_name": ["Laptop Stand", "Wireless Mouse", "USB-C Hub"],
        "unit_price_usd": [50.0, 30.0, 45.0],
    })


# =============================================================================
# 1. Null Checks
# =============================================================================
def test_check_nulls_clean_data(clean_orders_df):
    """Verifies that clean data without nulls passes with 100% score."""
    engine = DataQualityEngine(run_id="TEST-DQ-01")
    results = engine.check_nulls(clean_orders_df, "fact_orders", ["order_id", "total_amount_usd"])

    assert len(results) == 2
    for r in results:
        assert r.status == "PASS"
        assert r.failed_records == 0
        assert r.failure_rate_pct == 0.0
        assert r.quality_score == 100.0


def test_check_nulls_with_missing_values(clean_orders_df):
    """Verifies that null values are correctly detected, flagged, and scored."""
    df_with_nulls = clean_orders_df.copy()
    df_with_nulls.loc[0:1, "order_id"] = np.nan  # 2 nulls out of 10 rows (20%)

    engine = DataQualityEngine(run_id="TEST-DQ-02")
    results = engine.check_nulls(df_with_nulls, "fact_orders", ["order_id"])

    assert len(results) == 1
    res = results[0]
    assert res.failed_records == 2
    assert res.failure_rate_pct == 20.0
    assert res.quality_score == 80.0
    assert res.status == "FAIL"


def test_check_nulls_missing_column(clean_orders_df):
    """Verifies that check fails gracefully if a required column is not present."""
    engine = DataQualityEngine(run_id="TEST-DQ-03")
    results = engine.check_nulls(clean_orders_df, "fact_orders", ["non_existent_column"])

    assert len(results) == 1
    assert results[0].status == "FAIL"
    assert results[0].quality_score == 0.0


# =============================================================================
# 2. Duplicate Checks
# =============================================================================
def test_check_duplicates_unique_keys(clean_orders_df):
    """Verifies that completely unique rows pass uniqueness checks."""
    engine = DataQualityEngine(run_id="TEST-DQ-04")
    res = engine.check_duplicates(clean_orders_df, "fact_orders", ["order_id", "order_line_item_id"])

    assert res.status == "PASS"
    assert res.failed_records == 0
    assert res.quality_score == 100.0


def test_check_duplicates_detects_clones(clean_orders_df):
    """Verifies that duplicate rows are detected and penalized."""
    df_with_dups = pd.concat([clean_orders_df, clean_orders_df.iloc[[0]]], ignore_index=True)

    engine = DataQualityEngine(run_id="TEST-DQ-05")
    res = engine.check_duplicates(df_with_dups, "fact_orders", ["order_id"])

    assert res.status == "FAIL"
    assert res.failed_records == 2  # The 2 identical records
    assert res.quality_score < 100.0


# =============================================================================
# 3. Range Checks
# =============================================================================
def test_check_range_valid(clean_orders_df):
    """Verifies that values within [min, max] boundary pass validation."""
    engine = DataQualityEngine(run_id="TEST-DQ-06")
    res = engine.check_range(clean_orders_df, "fact_orders", "quantity", min_val=1, max_val=10)

    assert res.status == "PASS"
    assert res.failed_records == 0
    assert res.quality_score == 100.0


def test_check_range_violations(clean_orders_df):
    """Verifies that negative or out-of-boundary values fail range checks."""
    df_bad = clean_orders_df.copy()
    df_bad.loc[0, "quantity"] = -5  # Violation: below min_val 1
    df_bad.loc[1, "quantity"] = 0   # Violation

    engine = DataQualityEngine(run_id="TEST-DQ-07")
    res = engine.check_range(df_bad, "fact_orders", "quantity", min_val=1)

    assert res.failed_records == 2
    assert res.status in ["FAIL", "WARN"]
    assert res.quality_score < 100.0


# =============================================================================
# 4. Referential Integrity Checks
# =============================================================================
def test_check_referential_integrity_valid(clean_orders_df, clean_customers_df):
    """Verifies that foreign keys correctly referencing parents pass."""
    engine = DataQualityEngine(run_id="TEST-DQ-08")
    res = engine.check_referential_integrity(
        clean_orders_df, "customer_key", clean_customers_df, "customer_key", "orders_fk_customers"
    )

    assert res.status == "PASS"
    assert res.failed_records == 0
    assert res.quality_score == 100.0


def test_check_referential_integrity_orphans(clean_orders_df, clean_customers_df):
    """Verifies that orphan child rows are caught as integrity failures."""
    df_orphan = clean_orders_df.copy()
    df_orphan.loc[0, "customer_key"] = 9999  # Key not in clean_customers_df

    engine = DataQualityEngine(run_id="TEST-DQ-09")
    res = engine.check_referential_integrity(
        df_orphan, "customer_key", clean_customers_df, "customer_key", "orders_fk_customers"
    )

    assert res.status == "FAIL"
    assert res.failed_records == 1
    assert res.failure_rate_pct == 10.0


# =============================================================================
# 5. Full Evaluation Suite & Reporting
# =============================================================================
def test_evaluate_suite_clean_datasets(clean_orders_df, clean_customers_df, clean_products_df):
    """Tests master suite evaluation returning high quality score."""
    datasets = {
        "fact_orders": clean_orders_df,
        "dim_customer": clean_customers_df,
        "dim_product": clean_products_df,
    }

    engine = DataQualityEngine(run_id="SUITE-PASS-01")
    report = engine.evaluate_suite(datasets)

    assert isinstance(report, QualityReport)
    assert report.status == "PASS"
    assert report.overall_quality_score >= 95.0
    assert report.failed_checks == 0
    assert report.passed_checks > 0


def test_evaluate_suite_strict_mode_exception(clean_orders_df, clean_customers_df, clean_products_df):
    """Verifies that DataQualityBreachException is raised in strict mode when threshold breached."""
    bad_orders = clean_orders_df.copy()
    # Introduce major corruption
    bad_orders["order_id"] = None
    bad_orders["total_amount_usd"] = None

    datasets = {
        "fact_orders": bad_orders,
        "dim_customer": clean_customers_df,
        "dim_product": clean_products_df,
    }

    engine = DataQualityEngine(run_id="SUITE-FAIL-01")
    engine.strict_mode = True
    engine.min_acceptable_score = 90.0

    with pytest.raises(DataQualityBreachException):
        engine.evaluate_suite(datasets)


def test_report_json_serialization(clean_orders_df, tmp_path):
    """Verifies that QualityReport can be serialized and saved as valid JSON."""
    datasets = {"fact_orders": clean_orders_df}
    engine = DataQualityEngine(run_id="JSON-TEST")
    report = engine.evaluate_suite(datasets)

    saved_path = report.save_json(output_dir=str(tmp_path))
    assert Path(saved_path).is_file()

    with open(saved_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["run_id"] == "JSON-TEST"
    assert "overall_quality_score" in data
    assert "summary" in data
    assert len(data["results"]) > 0
