# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Engine
# MAGIC
# MAGIC A class-based rules engine. Every check type is a subclass of `DQCheck` with the same `run()` method,
# MAGIC and a registry maps rule names from the metadata tables to those classes. Adding a new check type means
# MAGIC writing one new class; adding a rule to a table means inserting one row in `dqr.dqrules`.

# COMMAND ----------

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Dict, List, Optional, Tuple, Type
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

# COMMAND ----------

# MAGIC %md ### Rule and result models

# COMMAND ----------

EMPTY_VALUES = {"", "na", "n/a", "none", "null"}


def _clean(value: Optional[str]) -> Optional[str]:
    """Turns placeholders such as 'NA' or '' into None."""
    if value is None:
        return None
    value = str(value).strip()
    return None if value.lower() in EMPTY_VALUES else value


@dataclass
class DQRule:
    rule_id: int
    rule_name: str
    object_name: str
    source_layer: str
    target_layer: Optional[str] = None
    attribute1: Optional[str] = None
    attribute2: Optional[str] = None
    target_object: Optional[str] = None

    def __post_init__(self):
        self.target_layer = _clean(self.target_layer)
        self.attribute1 = _clean(self.attribute1)
        self.attribute2 = _clean(self.attribute2)
        self.target_object = _clean(self.target_object)

    @property
    def columns(self) -> List[str]:
        """attribute1 can hold one column or a comma-separated list (composite keys)."""
        return [c.strip() for c in (self.attribute1 or "").split(",") if c.strip()]


@dataclass
class DQResult:
    run_id: str
    rule_id: int
    rule_name: str
    object_name: str
    source_layer: str
    target_layer: Optional[str]
    source_value: Optional[float]
    target_value: Optional[float]
    failed_records: int
    status: str  # Pass, Fail or Error
    message: str
    run_datetime: datetime


RESULT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("rule_id", IntegerType(), False),
    StructField("rule_name", StringType(), False),
    StructField("object_name", StringType(), False),
    StructField("source_layer", StringType(), True),
    StructField("target_layer", StringType(), True),
    StructField("source_value", DoubleType(), True),
    StructField("target_value", DoubleType(), True),
    StructField("failed_records", LongType(), False),
    StructField("status", StringType(), False),
    StructField("message", StringType(), True),
    StructField("run_datetime", TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md ### Base class and registry

# COMMAND ----------

CHECK_REGISTRY: Dict[str, Type["DQCheck"]] = {}


def normalize(name: Optional[str]) -> str:
    """'PrimaryKeyCheck', 'primary_key' and 'Primary Key' all become 'primarykey'."""
    key = (name or "").replace(" ", "").replace("_", "").lower()
    return key[:-5] if key.endswith("check") and len(key) > 5 else key


def register(*names: str):
    """Class decorator that maps one or more rule names to a check class."""
    def decorator(cls):
        for name in names:
            CHECK_REGISTRY[normalize(name)] = cls
        return cls
    return decorator


class DQCheck(ABC):
    """Shared behaviour for every check: load tables, evaluate, build a result, handle errors."""

    def __init__(self, spark: SparkSession, rule: DQRule, run_id: str):
        self.spark = spark
        self.rule = rule
        self.run_id = run_id

    def table(self, layer: str, object_name: str) -> DataFrame:
        return self.spark.table(f"{layer}.{object_name}")

    def source_df(self) -> DataFrame:
        return self.table(self.rule.source_layer, self.rule.object_name)

    def target_df(self) -> DataFrame:
        if not self.rule.target_layer:
            raise ValueError(f"Rule {self.rule.rule_id} needs a target layer")
        return self.table(self.rule.target_layer, self.rule.target_object or self.rule.object_name)

    @abstractmethod
    def evaluate(self) -> Tuple[Optional[float], Optional[float], Optional[DataFrame]]:
        """Returns (source_value, target_value, bad_records_or_None)."""

    @abstractmethod
    def passed(self, source_value: Optional[float], target_value: Optional[float]) -> bool:
        """Decides Pass or Fail from the measured values."""

    def describe(self, source_value, target_value) -> str:
        return f"source={source_value}, target={target_value}"

    def run(self) -> Tuple[DQResult, Optional[DataFrame]]:
        started = datetime.now()
        try:
            source_value, target_value, bad_records = self.evaluate()
            ok = self.passed(source_value, target_value)
            failed = 0 if (ok or bad_records is None) else bad_records.count()
            status, message = ("Pass" if ok else "Fail"), self.describe(source_value, target_value)
        except Exception as error:  # one broken rule must not stop the whole run
            source_value = target_value = bad_records = None
            failed, status, message = 0, "Error", f"{type(error).__name__}: {error}"[:500]
        result = DQResult(
            run_id=self.run_id, rule_id=self.rule.rule_id, rule_name=self.rule.rule_name,
            object_name=self.rule.object_name, source_layer=self.rule.source_layer,
            target_layer=self.rule.target_layer, source_value=source_value,
            target_value=target_value, failed_records=failed, status=status,
            message=message, run_datetime=started,
        )
        return result, (bad_records if status == "Fail" else None)

# COMMAND ----------

# MAGIC %md ### Check types

# COMMAND ----------

@register("PrimaryKey", "PrimaryKeyCheck")
class PrimaryKeyCheck(DQCheck):
    """Fails when any key (single or composite) appears more than once."""

    def evaluate(self):
        keys = self.rule.columns
        if not keys:
            raise ValueError("PrimaryKey rule needs key column(s) in dqattribute1")
        df = self.source_df()
        duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1)
        duplicate_keys = duplicates.count()
        bad = df.join(duplicates.select(*keys), on=keys, how="inner") if duplicate_keys else None
        return float(duplicate_keys), None, bad

    def passed(self, source_value, target_value):
        return source_value == 0

    def describe(self, source_value, target_value):
        return f"{int(source_value)} duplicate key value(s) on {', '.join(self.rule.columns)}"


@register("NullCheck", "Null")
class NullCheck(DQCheck):
    """Fails when any of the listed columns is null."""

    def evaluate(self):
        columns = self.rule.columns
        if not columns:
            raise ValueError("NullCheck rule needs column(s) in dqattribute1")
        condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
        bad = self.source_df().filter(condition)
        null_rows = bad.count()
        return float(null_rows), None, (bad if null_rows else None)

    def passed(self, source_value, target_value):
        return source_value == 0

    def describe(self, source_value, target_value):
        return f"{int(source_value)} row(s) with nulls in {', '.join(self.rule.columns)}"


@register("RecordCount", "RowCount")
class RecordCountCheck(DQCheck):
    """Fails when the source and target layers hold a different number of rows."""

    def evaluate(self):
        return float(self.source_df().count()), float(self.target_df().count()), None

    def passed(self, source_value, target_value):
        return source_value == target_value

    def describe(self, source_value, target_value):
        return f"{int(source_value)} rows in source vs {int(target_value)} in target"


@register("SumCheck", "Sum")
class SumCheck(DQCheck):
    """Fails when a numeric column's total differs between layers (attribute2 = target column, optional)."""

    TOLERANCE = 0.01

    def evaluate(self):
        source_column = self.rule.attribute1
        if not source_column:
            raise ValueError("SumCheck rule needs a numeric column in dqattribute1")
        target_column = self.rule.attribute2 or source_column
        source_total = self.source_df().agg(F.sum(F.col(source_column).cast("double"))).first()[0]
        target_total = self.target_df().agg(F.sum(F.col(target_column).cast("double"))).first()[0]
        return source_total, target_total, None

    def passed(self, source_value, target_value):
        if source_value is None or target_value is None:
            return source_value is None and target_value is None
        return abs(source_value - target_value) <= self.TOLERANCE

# COMMAND ----------

# MAGIC %md ### Engine

# COMMAND ----------

class DQEngine:
    """Runs a list of rules and stores results and failed records as Delta tables."""

    def __init__(self, spark: SparkSession, results_table: str = "dq.dqresults",
                 bad_records_schema: str = "dq_badrecords"):
        self.spark = spark
        self.results_table = results_table
        self.bad_records_schema = bad_records_schema

    def run(self, rules: List[DQRule], save: bool = True) -> DataFrame:
        run_id = str(uuid.uuid4())
        results = []
        for rule in rules:
            check_class = CHECK_REGISTRY.get(normalize(rule.rule_name))
            if check_class is None:
                results.append(DQResult(
                    run_id, rule.rule_id, rule.rule_name, rule.object_name, rule.source_layer,
                    rule.target_layer, None, None, 0, "Error",
                    f"Unknown rule type '{rule.rule_name}'", datetime.now(),
                ))
                continue
            result, bad_records = check_class(self.spark, rule, run_id).run()
            results.append(result)
            if save and bad_records is not None:
                self._save_bad_records(bad_records, rule, run_id)
        results_df = self.spark.createDataFrame(
            [tuple(getattr(r, f.name) for f in RESULT_SCHEMA.fields) for r in results],
            schema=RESULT_SCHEMA,
        )
        if save:
            self._ensure_schema(self.results_table.split(".")[0])
            results_df.write.format("delta").mode("append").saveAsTable(self.results_table)
        return results_df

    def _ensure_schema(self, schema: str):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def _save_bad_records(self, bad_records: DataFrame, rule: DQRule, run_id: str):
        self._ensure_schema(self.bad_records_schema)
        table = f"{self.bad_records_schema}.{rule.object_name}_{normalize(rule.rule_name)}".lower()
        (bad_records
            .withColumn("dq_run_id", F.lit(run_id))
            .withColumn("dq_rule_id", F.lit(rule.rule_id))
            .withColumn("dq_captured_at", F.current_timestamp())
            .write.format("delta").mode("append").option("mergeSchema", "true")
            .saveAsTable(table))

# COMMAND ----------

# MAGIC %md ### Loading rules

# COMMAND ----------

def rules_from_dataframe(rules_df: DataFrame) -> List[DQRule]:
    """Converts rows from dqr.Vw_Rules into DQRule objects."""
    rules = []
    for row in rules_df.collect():
        data = row.asDict()
        rules.append(DQRule(
            rule_id=int(data["dqruleid"]),
            rule_name=data["dqrulename"],
            object_name=data["dqobjectname"].lower(),
            source_layer=data["sourcelayer"],
            target_layer=data.get("targetlayer"),
            attribute1=data.get("dqattribute1"),
            attribute2=data.get("dqattribute2"),
            target_object=data.get("targetobjectname"),
        ))
    return rules


def load_rules_from_sql(spark: SparkSession, jdbc_url: str, user: str, password: str,
                        view: str = "dqr.Vw_Rules") -> List[DQRule]:
    """Reads active rules from Azure SQL. Note: Spark JDBC expects the option name 'user', not 'username'."""
    rules_df = (spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", view)
                .option("user", user)
                .option("password", password)
                .load())
    return rules_from_dataframe(rules_df)

# COMMAND ----------

# MAGIC %md ### Sample rules (mirror of dq_metadata.sql, for running without Azure SQL)

# COMMAND ----------

_PRIMARY_KEYS = {
    "costcenter": "CostCenterNumber", "currency": "CurrencyId", "custtable": "CustomerId",
    "fiscalperiod": "FiscalPeriodName", "parties": "PartyId", "partyaddress": "PartyNumber",
    "promotable": "PromotionId", "purchaseorder": "PoNumber,LineItem", "purchcategory": "CategoryId",
    "purchcontracts": "ContractId", "purchitem": "ItemId",
    "salesorderline": "SalesOrderNumber,SalesOrderLine", "vendtable": "VendId",
}
_NULL_CHECKS = {"costcenter": "CostCenterNumber", "custtable": "CustomerId",
                "vendtable": "VendId", "purchitem": "ItemId"}
_BRONZE_TO_SILVER = {"costcenter": "dimcostcenter", "currency": "dimcurrency", "parties": "dimparty",
                     "purchcategory": "dimpurchasecategory", "purchitem": "dimpurchitem",
                     "vendtable": "dimvendor"}

SAMPLE_RULES: List[DQRule] = (
    [DQRule(i + 1, "PrimaryKey", obj, "bronze", attribute1=cols)
     for i, (obj, cols) in enumerate(_PRIMARY_KEYS.items())]
    + [DQRule(101 + i, "NullCheck", obj, "bronze", attribute1=col)
       for i, (obj, col) in enumerate(_NULL_CHECKS.items())]
    + [DQRule(201 + i, "RecordCount", obj, "bronze", "silver", target_object=dim)
       for i, (obj, dim) in enumerate(_BRONZE_TO_SILVER.items())]
)
