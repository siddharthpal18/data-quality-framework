"""Unit tests for dq_engine.py. Run locally with: pip install pyspark pytest && pytest tests/"""
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# dq_engine.py is a Databricks notebook; its MAGIC lines are comments, so it loads as plain Python.
ENGINE = Path(__file__).resolve().parents[1] / "notebooks" / "dataquality" / "dq_engine.py"
exec(compile(ENGINE.read_text(), str(ENGINE), "exec"))


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = (SparkSession.builder.master("local[1]").appName("dq-tests")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
               .config("spark.ui.enabled", "false").getOrCreate())
    for db in ("bronze", "silver"):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    def save(rows, columns, name):
        session.createDataFrame(rows, columns).write.mode("overwrite").saveAsTable(name)

    save([("C1", 10.0), ("C2", 20.0), ("C2", 5.0), (None, 1.0)], ["CostCenterNumber", "Amount"], "bronze.costcenter")
    save([("C1", 10.0), ("C2", 20.0), ("X", 1.0)], ["CostCenterNumber", "Amount"], "silver.dimcostcenter")
    save([("P1", 1), ("P1", 2), ("P2", 1)], ["PoNumber", "LineItem"], "bronze.purchaseorder")
    save([("V1",), ("V2",)], ["VendId"], "bronze.vendtable")
    save([("V1",), ("V2",)], ["VendorId"], "silver.dimvendor")
    yield session
    session.stop()


def run_one(spark, rule):
    return DQEngine(spark).run([rule], save=False).collect()[0]


def test_primary_key_finds_duplicates(spark):
    result = run_one(spark, DQRule(1, "PrimaryKey", "costcenter", "bronze", attribute1="CostCenterNumber"))
    assert result.status == "Fail"
    assert result.failed_records == 2  # both rows sharing key C2


def test_composite_primary_key_passes(spark):
    result = run_one(spark, DQRule(2, "PrimaryKey", "purchaseorder", "bronze", attribute1="PoNumber, LineItem"))
    assert result.status == "Pass"


def test_null_check_finds_null_rows(spark):
    result = run_one(spark, DQRule(3, "NullCheck", "costcenter", "bronze", attribute1="CostCenterNumber"))
    assert result.status == "Fail" and result.failed_records == 1


def test_record_count_compares_layers(spark):
    failing = run_one(spark, DQRule(4, "RecordCount", "costcenter", "bronze", "silver", target_object="dimcostcenter"))
    passing = run_one(spark, DQRule(5, "RecordCount", "vendtable", "bronze", "silver", target_object="dimvendor"))
    assert (failing.source_value, failing.target_value, failing.status) == (4.0, 3.0, "Fail")
    assert passing.status == "Pass"


def test_sum_check_detects_mismatch(spark):
    result = run_one(spark, DQRule(6, "SumCheck", "costcenter", "bronze", "silver", "Amount", "Amount", "dimcostcenter"))
    assert (result.source_value, result.target_value, result.status) == (36.0, 31.0, "Fail")


@pytest.mark.parametrize("name", ["PrimaryKeyCheck", "primary_key", "Primary Key"])
def test_rule_names_are_normalized(name):
    assert CHECK_REGISTRY[normalize(name)] is PrimaryKeyCheck


def test_placeholders_become_none():
    rule = DQRule(7, "NullCheck", "costcenter", "bronze", target_layer="NA", attribute2="")
    assert rule.target_layer is None and rule.attribute2 is None


def test_unknown_rule_and_missing_table_are_errors_not_crashes(spark):
    results = DQEngine(spark).run([
        DQRule(8, "FreshnessCheck", "costcenter", "bronze"),
        DQRule(9, "PrimaryKey", "missingtable", "bronze", attribute1="Id"),
    ], save=False).collect()
    assert [r.status for r in results] == ["Error", "Error"]
