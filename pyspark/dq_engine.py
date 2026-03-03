import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dataclass
class RuleResult:
    rule_id: str
    rule_type: str
    status: str  # PASS/FAIL
    failed_count: int
    total_count: int
    severity: str
    description: str
    details: str


class DataQualityEngine:
    """
    Rule-driven Data Quality engine for PySpark.

    Outputs:
    - rule_results_df: one row per rule with metrics and status
    - failed_records_df: input rows that failed any row-level rule
    - passed_records_df: input rows that passed all row-level rules
    - dq_score: simple weighted score based on severity
    """

    SEVERITY_WEIGHT = {"LOW": 1, "MEDIUM": 3, "HIGH": 5}

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def load_rules_from_json(json_path: str) -> Dict[str, Any]:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _safe_col(col_name: str):
        return F.col(col_name)

    def _apply_not_null(self, df: DataFrame, rule: Dict[str, Any]) -> Tuple[DataFrame, RuleResult]:
        coln = rule["column"]
        total = df.count()
        failed_df = df.filter(self._safe_col(coln).isNull())
        failed = failed_df.count()

        status = "PASS" if failed == 0 else "FAIL"
        rr = RuleResult(
            rule_id=rule["rule_id"],
            rule_type="NOT_NULL",
            status=status,
            failed_count=failed,
            total_count=total,
            severity=rule.get("severity", "LOW"),
            description=rule.get("description", ""),
            details=f"{coln} nulls={failed}"
        )

        # row-level failure flag
        flagged = df.withColumn(f"_dq_fail_{rule['rule_id']}", self._safe_col(coln).isNull())
        return flagged, rr

    def _apply_range_check(self, df: DataFrame, rule: Dict[str, Any]) -> Tuple[DataFrame, RuleResult]:
        coln = rule["column"]
        minv = rule.get("min_value")
        maxv = rule.get("max_value")

        total = df.count()
        cond = F.lit(False)

        if minv is not None and maxv is not None:
            cond = (self._safe_col(coln) < F.lit(minv)) | (self._safe_col(coln) > F.lit(maxv)) | self._safe_col(coln).isNull()
            detail = f"{coln} outside [{minv},{maxv}] or null"
        elif minv is not None:
            cond = (self._safe_col(coln) < F.lit(minv)) | self._safe_col(coln).isNull()
            detail = f"{coln} < {minv} or null"
        elif maxv is not None:
            cond = (self._safe_col(coln) > F.lit(maxv)) | self._safe_col(coln).isNull()
            detail = f"{coln} > {maxv} or null"
        else:
            detail = f"{coln} range not configured"

        failed = df.filter(cond).count()
        status = "PASS" if failed == 0 else "FAIL"

        rr = RuleResult(
            rule_id=rule["rule_id"],
            rule_type="RANGE_CHECK",
            status=status,
            failed_count=failed,
            total_count=total,
            severity=rule.get("severity", "LOW"),
            description=rule.get("description", ""),
            details=detail
        )

        flagged = df.withColumn(f"_dq_fail_{rule['rule_id']}", cond)
        return flagged, rr

    def _apply_regex_check(self, df: DataFrame, rule: Dict[str, Any]) -> Tuple[DataFrame, RuleResult]:
        coln = rule["column"]
        pattern = rule["pattern"]

        total = df.count()
        # If email is null, count as fail? Here: null is fail for regex rule
        cond = self._safe_col(coln).isNull() | (~self._safe_col(coln).rlike(pattern))
        failed = df.filter(cond).count()
        status = "PASS" if failed == 0 else "FAIL"

        rr = RuleResult(
            rule_id=rule["rule_id"],
            rule_type="REGEX_CHECK",
            status=status,
            failed_count=failed,
            total_count=total,
            severity=rule.get("severity", "LOW"),
            description=rule.get("description", ""),
            details=f"{coln} not matching regex or null"
        )

        flagged = df.withColumn(f"_dq_fail_{rule['rule_id']}", cond)
        return flagged, rr

    def _apply_unique(self, df: DataFrame, rule: Dict[str, Any]) -> Tuple[DataFrame, RuleResult]:
        cols = rule["columns"]
        total = df.count()

        dup_keys = (df.groupBy([F.col(c) for c in cols])
                      .count()
                      .filter(F.col("count") > 1)
                   )

        failed = dup_keys.count()  # number of duplicated key groups
        status = "PASS" if failed == 0 else "FAIL"

        rr = RuleResult(
            rule_id=rule["rule_id"],
            rule_type="UNIQUE",
            status=status,
            failed_count=failed,
            total_count=total,
            severity=rule.get("severity", "LOW"),
            description=rule.get("description", ""),
            details=f"duplicate key groups for {cols} = {failed}"
        )

        # Row-level flag: mark rows that belong to duplicated keys
        d = dup_keys.select(*cols).withColumn("_is_dup_key", F.lit(True))
        flagged = (df.join(d, on=cols, how="left")
                     .withColumn(f"_dq_fail_{rule['rule_id']}", F.coalesce(F.col("_is_dup_key"), F.lit(False)))
                     .drop("_is_dup_key")
                  )
        return flagged, rr

    def _apply_threshold_check(self, df: DataFrame, rule: Dict[str, Any]) -> Tuple[DataFrame, RuleResult]:
        """
        Dataset-level rule. Example: null_rate(email) <= 0.05
        """
        metric = rule["metric"]
        op = rule["operator"]
        threshold = float(rule["threshold"])
        coln = rule.get("column")

        total = df.count()
        if total == 0:
            value = 0.0
        else:
            if metric == "null_rate":
                nulls = df.filter(self._safe_col(coln).isNull()).count()
                value = nulls / float(total)
            else:
                value = 0.0

        passed = self._compare(value, op, threshold)
        status = "PASS" if passed else "FAIL"

        rr = RuleResult(
            rule_id=rule["rule_id"],
            rule_type="THRESHOLD_CHECK",
            status=status,
            failed_count=(0 if passed else 1),  # dataset-level flag
            total_count=1,
            severity=rule.get("severity", "LOW"),
            description=rule.get("description", ""),
            details=f"{metric}({coln}) = {value:.6f} {op} {threshold}"
        )

        # No row-level column added (dataset level)
        return df, rr

    @staticmethod
    def _compare(value: float, op: str, threshold: float) -> bool:
        if op == "<=":
            return value <= threshold
        if op == "<":
            return value < threshold
        if op == ">=":
            return value >= threshold
        if op == ">":
            return value > threshold
        if op == "==":
            return value == threshold
        if op == "!=":
            return value != threshold
        raise ValueError(f"Unsupported operator: {op}")

    def run(self, df: DataFrame, rules_config: Dict[str, Any]) -> Dict[str, Any]:
        rules: List[Dict[str, Any]] = rules_config.get("rules", [])
        results: List[RuleResult] = []

        work_df = df

        # Apply rules
        for rule in rules:
            rtype = rule["rule_type"].upper()

            if rtype == "NOT_NULL":
                work_df, rr = self._apply_not_null(work_df, rule)
            elif rtype == "UNIQUE":
                work_df, rr = self._apply_unique(work_df, rule)
            elif rtype == "RANGE_CHECK":
                work_df, rr = self._apply_range_check(work_df, rule)
            elif rtype == "REGEX_CHECK":
                work_df, rr = self._apply_regex_check(work_df, rule)
            elif rtype == "THRESHOLD_CHECK":
                work_df, rr = self._apply_threshold_check(work_df, rule)
            else:
                rr = RuleResult(
                    rule_id=rule.get("rule_id", "UNKNOWN"),
                    rule_type=rtype,
                    status="FAIL",
                    failed_count=1,
                    total_count=1,
                    severity=rule.get("severity", "LOW"),
                    description=rule.get("description", ""),
                    details=f"Unsupported rule type: {rtype}"
                )
            results.append(rr)

        # Build rule results DF
        res_rows = [
            (r.rule_id, r.rule_type, r.status, r.failed_count, r.total_count, r.severity, r.description, r.details)
            for r in results
        ]
        rule_results_df = self.spark.createDataFrame(
            res_rows,
            ["rule_id", "rule_type", "status", "failed_count", "total_count", "severity", "description", "details"]
        )

        # Row-level failures: any _dq_fail_* column = True
        fail_cols = [c for c in work_df.columns if c.startswith("_dq_fail_")]
        if fail_cols:
            any_fail_expr = F.lit(False)
            for c in fail_cols:
                any_fail_expr = any_fail_expr | F.col(c)

            failed_records_df = work_df.filter(any_fail_expr)
            passed_records_df = work_df.filter(~any_fail_expr)
        else:
            failed_records_df = work_df.limit(0)
            passed_records_df = work_df

        dq_score = self._calculate_score(rule_results_df)

        return {
            "rule_results_df": rule_results_df,
            "failed_records_df": failed_records_df,
            "passed_records_df": passed_records_df,
            "dq_score": dq_score
        }

    def _calculate_score(self, rule_results_df: DataFrame) -> float:
        """
        Simple scoring:
        Start with 100.
        For each failed rule, subtract severity weight.
        Floor at 0.
        """
        failed = (rule_results_df
                  .filter(F.col("status") == "FAIL")
                  .select("severity")
                  .collect())

        penalty = 0
        for row in failed:
            sev = (row["severity"] or "LOW").upper()
            penalty += self.SEVERITY_WEIGHT.get(sev, 1)

        score = max(0.0, 100.0 - float(penalty))
        return score


if __name__ == "__main__":
    # Example usage (Databricks / spark-submit style)
    spark = SparkSession.builder.getOrCreate()

    # Example dataset
    data = [
        ("O1", 1, "C1", 100.0, "a@b.com"),
        ("O1", 1, "C1", 100.0, "a@b.com"),  # duplicate key
        ("O2", 1, None, -10.0, "bademail"), # null + range + regex
        (None, 1, "C3", 250.0, None)        # null order_id + null email
    ]
    cols = ["order_id", "order_item_id", "customer_id", "order_amount", "email"]
    df = spark.createDataFrame(data, cols)

    # Load rules from local JSON (adjust path when running)
    # cfg = DataQualityEngine.load_rules_from_json("rules/rule_config.json")
    # For demo here, read from string not file
    cfg = {
        "rules": [
            {"rule_id": "R001", "rule_type": "NOT_NULL", "column": "order_id", "severity": "HIGH", "description": "order_id not null"},
            {"rule_id": "R002", "rule_type": "NOT_NULL", "column": "customer_id", "severity": "HIGH", "description": "customer_id not null"},
            {"rule_id": "R003", "rule_type": "UNIQUE", "columns": ["order_id", "order_item_id"], "severity": "HIGH", "description": "unique key"},
            {"rule_id": "R004", "rule_type": "RANGE_CHECK", "column": "order_amount", "min_value": 0, "max_value": 1000000, "severity": "MEDIUM", "description": "amount range"},
            {"rule_id": "R005", "rule_type": "REGEX_CHECK", "column": "email", "pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$", "severity": "LOW", "description": "email format"},
            {"rule_id": "R006", "rule_type": "THRESHOLD_CHECK", "metric": "null_rate", "column": "email", "operator": "<=", "threshold": 0.05, "severity": "MEDIUM", "description": "email null rate <= 5%"}
        ]
    }

    engine = DataQualityEngine(spark)
    out = engine.run(df, cfg)

    print("DQ Score:", out["dq_score"])
    out["rule_results_df"].show(truncate=False)
    out["failed_records_df"].show(truncate=False)
