"""
Data Quality & Observability Framework.
Provides automated data validation including completeness (nulls),
uniqueness (duplicates), validity (ranges & patterns), and referential integrity.
Calculates overall Quality Health Index and generates JSON/HTML audit reports.

Author: Prabhat Sharma (prabhat84sharma-ui)
"""

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union
import numpy as np
import pandas as pd

from src.config import get_config

logger = logging.getLogger(__name__)


class DataQualityBreachException(Exception):
    """Raised when data quality checks violate critical thresholds."""
    pass


@dataclass
class QualityCheckResult:
    """Represents the outcome of an individual data quality check."""
    check_name: str
    check_type: str  # NULL_CHECK, DUPLICATE_CHECK, RANGE_CHECK, INTEGRITY, PATTERN
    table_name: str
    column_name: Optional[str]
    total_records: int
    failed_records: int
    failure_rate_pct: float
    quality_score: float  # 0.0 to 100.0
    status: str  # PASS, WARN, FAIL
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    """Comprehensive data quality scorecard for an ETL pipeline run."""
    run_id: str
    evaluated_at: str
    overall_quality_score: float
    status: str  # PASS, WARN, FAIL
    total_checks: int
    passed_checks: int
    warned_checks: int
    failed_checks: int
    check_results: List[QualityCheckResult]

    def to_dict(self) -> Dict[str, Any]:
        """Converts report object to dictionary."""
        return {
            "run_id": self.run_id,
            "evaluated_at": self.evaluated_at,
            "overall_quality_score": round(self.overall_quality_score, 2),
            "status": self.status,
            "summary": {
                "total_checks": self.total_checks,
                "passed_checks": self.passed_checks,
                "warned_checks": self.warned_checks,
                "failed_checks": self.failed_checks,
            },
            "results": [asdict(r) for r in self.check_results],
        }

    def save_json(self, output_dir: Optional[str] = None) -> Path:
        """Serializes quality report to disk as JSON."""
        config = get_config()
        out_path = Path(output_dir or config.get("app.reports_dir", "reports"))
        out_path.mkdir(parents=True, exist_ok=True)
        file_path = out_path / f"dq_report_{self.run_id}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

        logger.info("Data quality JSON report saved to: %s", file_path)
        return file_path


class DataQualityEngine:
    """
    Core validation engine implementing rules-based validation and scoring.
    """

    def __init__(self, run_id: Optional[str] = None):
        self.config = get_config()
        self.run_id = run_id or f"RUN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        dq_cfg = self.config.data_quality
        self.min_acceptable_score = float(dq_cfg.get("min_acceptable_score", 90.0))
        self.warning_threshold_score = float(dq_cfg.get("warning_threshold_score", 95.0))
        self.strict_mode = bool(dq_cfg.get("strict_mode", False))
        self.results: List[QualityCheckResult] = []

    # =========================================================================
    # Individual Quality Checks
    # =========================================================================
    def check_nulls(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: Sequence[str],
        max_allowed_null_pct: float = 0.0,
    ) -> List[QualityCheckResult]:
        """
        Validates column completeness (absence of NULLs or NaNs).
        
        Args:
            df: Input DataFrame.
            table_name: Name of evaluated entity.
            columns: Columns required to be non-null.
            max_allowed_null_pct: Tolerable null percentage (default 0%).
        """
        results = []
        total = len(df)
        if total == 0:
            return results

        for col in columns:
            if col not in df.columns:
                res = QualityCheckResult(
                    check_name=f"null_check_{col}",
                    check_type="NULL_CHECK",
                    table_name=table_name,
                    column_name=col,
                    total_records=total,
                    failed_records=total,
                    failure_rate_pct=100.0,
                    quality_score=0.0,
                    status="FAIL",
                    message=f"Required column '{col}' is missing from table '{table_name}'.",
                )
                results.append(res)
                continue

            null_count = int(df[col].isna().sum())
            null_pct = (null_count / total) * 100.0
            score = max(0.0, 100.0 - null_pct)

            if null_pct > max_allowed_null_pct:
                status = "FAIL" if null_pct > 5.0 else "WARN"
            else:
                status = "PASS"

            res = QualityCheckResult(
                check_name=f"null_check_{col}",
                check_type="NULL_CHECK",
                table_name=table_name,
                column_name=col,
                total_records=total,
                failed_records=null_count,
                failure_rate_pct=round(null_pct, 3),
                quality_score=round(score, 2),
                status=status,
                message=(
                    f"Column '{col}' has {null_count}/{total} nulls ({null_pct:.2f}%). "
                    f"Status: {status}."
                ),
            )
            results.append(res)

        self.results.extend(results)
        return results

    def check_duplicates(
        self,
        df: pd.DataFrame,
        table_name: str,
        subset_columns: Sequence[str],
    ) -> QualityCheckResult:
        """
        Validates row uniqueness against a primary or composite key.
        """
        total = len(df)
        if total == 0:
            res = QualityCheckResult(
                check_name=f"duplicate_check_{'_'.join(subset_columns)}",
                check_type="DUPLICATE_CHECK",
                table_name=table_name,
                column_name=",".join(subset_columns),
                total_records=0,
                failed_records=0,
                failure_rate_pct=0.0,
                quality_score=100.0,
                status="PASS",
                message=f"Table '{table_name}' has 0 rows. Check passed.",
            )
            self.results.append(res)
            return res

        valid_cols = [c for c in subset_columns if c in df.columns]
        if len(valid_cols) != len(subset_columns):
            res = QualityCheckResult(
                check_name=f"duplicate_check_{'_'.join(subset_columns)}",
                check_type="DUPLICATE_CHECK",
                table_name=table_name,
                column_name=",".join(subset_columns),
                total_records=total,
                failed_records=total,
                failure_rate_pct=100.0,
                quality_score=0.0,
                status="FAIL",
                message=f"Columns {subset_columns} not found in table '{table_name}'.",
            )
            self.results.append(res)
            return res

        dup_mask = df.duplicated(subset=subset_columns, keep=False)
        dup_count = int(dup_mask.sum())
        dup_pct = (dup_count / total) * 100.0
        score = max(0.0, 100.0 - (dup_pct * 2))  # Duplicates penalize heavily

        status = "PASS" if dup_count == 0 else "FAIL"

        res = QualityCheckResult(
            check_name=f"duplicate_check_{'_'.join(subset_columns)}",
            check_type="DUPLICATE_CHECK",
            table_name=table_name,
            column_name=",".join(subset_columns),
            total_records=total,
            failed_records=dup_count,
            failure_rate_pct=round(dup_pct, 3),
            quality_score=round(score, 2),
            status=status,
            message=(
                f"Duplicate check on {subset_columns} in '{table_name}': "
                f"{dup_count} duplicate rows found ({dup_pct:.2f}%)."
            ),
        )
        self.results.append(res)
        return res

    def check_range(
        self,
        df: pd.DataFrame,
        table_name: str,
        column: str,
        min_val: Optional[Union[int, float]] = None,
        max_val: Optional[Union[int, float]] = None,
    ) -> QualityCheckResult:
        """
        Validates numeric boundaries (e.g. quantity >= 1, amount > 0).
        """
        total = len(df)
        if total == 0 or column not in df.columns:
            res = QualityCheckResult(
                check_name=f"range_check_{column}",
                check_type="RANGE_CHECK",
                table_name=table_name,
                column_name=column,
                total_records=total,
                failed_records=0,
                failure_rate_pct=0.0,
                quality_score=100.0,
                status="PASS",
                message=f"Column '{column}' skipped (no rows or not found).",
            )
            self.results.append(res)
            return res

        series = pd.to_numeric(df[column], errors="coerce")
        out_of_bounds_mask = pd.Series(False, index=df.index)

        if min_val is not None:
            out_of_bounds_mask |= series < min_val
        if max_val is not None:
            out_of_bounds_mask |= series > max_val
        out_of_bounds_mask |= series.isna()

        failed_count = int(out_of_bounds_mask.sum())
        failure_pct = (failed_count / total) * 100.0
        score = max(0.0, 100.0 - failure_pct)

        status = "PASS" if failed_count == 0 else ("FAIL" if failure_pct > 2.0 else "WARN")

        res = QualityCheckResult(
            check_name=f"range_check_{column}",
            check_type="RANGE_CHECK",
            table_name=table_name,
            column_name=column,
            total_records=total,
            failed_records=failed_count,
            failure_rate_pct=round(failure_pct, 3),
            quality_score=round(score, 2),
            status=status,
            message=(
                f"Range validation on '{column}' [{min_val} to {max_val}]: "
                f"{failed_count} violations ({failure_pct:.2f}%)."
            ),
            details={"min_val": min_val, "max_val": max_val},
        )
        self.results.append(res)
        return res

    def check_referential_integrity(
        self,
        child_df: pd.DataFrame,
        child_key: str,
        parent_df: pd.DataFrame,
        parent_key: str,
        relationship_name: str,
    ) -> QualityCheckResult:
        """
        Validates foreign key relationships to prevent orphan records.
        """
        total = len(child_df)
        if total == 0:
            res = QualityCheckResult(
                check_name=f"integrity_{relationship_name}",
                check_type="INTEGRITY",
                table_name=relationship_name,
                column_name=f"{child_key}->{parent_key}",
                total_records=0,
                failed_records=0,
                failure_rate_pct=0.0,
                quality_score=100.0,
                status="PASS",
                message="No child records to validate.",
            )
            self.results.append(res)
            return res

        parent_keys_set = set(parent_df[parent_key].dropna().unique())
        orphan_mask = ~child_df[child_key].isin(parent_keys_set)
        orphan_count = int(orphan_mask.sum())
        failure_pct = (orphan_count / total) * 100.0
        score = max(0.0, 100.0 - failure_pct)

        status = "PASS" if orphan_count == 0 else "FAIL"

        res = QualityCheckResult(
            check_name=f"integrity_{relationship_name}",
            check_type="INTEGRITY",
            table_name=relationship_name,
            column_name=f"{child_key}->{parent_key}",
            total_records=total,
            failed_records=orphan_count,
            failure_rate_pct=round(failure_pct, 3),
            quality_score=round(score, 2),
            status=status,
            message=(
                f"Referential integrity '{relationship_name}' ({child_key} -> {parent_key}): "
                f"{orphan_count} orphan child records detected."
            ),
        )
        self.results.append(res)
        return res

    # =========================================================================
    # Master Quality Suite Evaluation & Scorecard
    # =========================================================================
    def evaluate_suite(self, datasets: Dict[str, pd.DataFrame]) -> QualityReport:
        """
        Executes configured validation rules against all tables in the dataset.
        
        Args:
            datasets: Dict mapping table name to transformed DataFrame.
            
        Returns:
            QualityReport scorecard.
        """
        logger.info("Executing comprehensive data quality test suite for run: %s", self.run_id)
        self.results.clear()

        # 1. Orders table checks
        orders_df = datasets.get("fact_orders", datasets.get("orders", pd.DataFrame()))
        if not orders_df.empty:
            self.check_nulls(
                orders_df,
                "fact_orders",
                ["order_id", "customer_key", "product_key", "date_key", "total_amount_usd"],
            )
            self.check_duplicates(orders_df, "fact_orders", ["order_id", "order_line_item_id"])
            self.check_range(orders_df, "fact_orders", "quantity", min_val=1)
            self.check_range(orders_df, "fact_orders", "total_amount_usd", min_val=0.01)
            self.check_range(orders_df, "fact_orders", "discount_pct", min_val=0.0, max_val=100.0)

        # 2. Customer table checks
        cust_df = datasets.get("dim_customer", pd.DataFrame())
        if not cust_df.empty:
            self.check_nulls(cust_df, "dim_customer", ["customer_id", "email"])
            self.check_duplicates(cust_df, "dim_customer", ["customer_id"])

        # 3. Product table checks
        prod_df = datasets.get("dim_product", pd.DataFrame())
        if not prod_df.empty:
            self.check_nulls(prod_df, "dim_product", ["product_id", "product_name", "unit_price_usd"])
            self.check_duplicates(prod_df, "dim_product", ["product_id"])
            self.check_range(prod_df, "dim_product", "unit_price_usd", min_val=0.01)

        # 4. Referential Integrity Checks
        if not orders_df.empty and not cust_df.empty:
            self.check_referential_integrity(
                orders_df, "customer_key", cust_df, "customer_key", "orders_fk_customer"
            )
        if not orders_df.empty and not prod_df.empty:
            self.check_referential_integrity(
                orders_df, "product_key", prod_df, "product_key", "orders_fk_product"
            )

        # Calculate Overall Quality Score (weighted average)
        if self.results:
            overall_score = float(np.mean([r.quality_score for r in self.results]))
        else:
            overall_score = 100.0

        passed_count = sum(1 for r in self.results if r.status == "PASS")
        warned_count = sum(1 for r in self.results if r.status == "WARN")
        failed_count = sum(1 for r in self.results if r.status == "FAIL")

        if failed_count > 0 or overall_score < self.min_acceptable_score:
            report_status = "FAIL"
        elif warned_count > 0 or overall_score < self.warning_threshold_score:
            report_status = "WARN"
        else:
            report_status = "PASS"

        report = QualityReport(
            run_id=self.run_id,
            evaluated_at=datetime.utcnow().isoformat() + "Z",
            overall_quality_score=round(overall_score, 2),
            status=report_status,
            total_checks=len(self.results),
            passed_checks=passed_count,
            warned_checks=warned_count,
            failed_checks=failed_count,
            check_results=list(self.results),
        )

        logger.info(
            "Data Quality Suite Complete: Score=%.2f%% | Status=%s | Passed=%d, Warned=%d, Failed=%d",
            overall_score, report_status, passed_count, warned_count, failed_count
        )

        # Save JSON artifact
        report.save_json()

        # Enforce strict mode if threshold breached
        if self.strict_mode and report_status == "FAIL":
            raise DataQualityBreachException(
                f"Data Quality Suite failed with score {overall_score:.2f}% "
                f"(Threshold: {self.min_acceptable_score}%). Pipeline execution halted."
            )

        return report
