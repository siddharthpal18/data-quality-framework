"""Runs the data quality engine on a laptop, no Azure or Databricks needed.

Creates small bronze and silver tables that mimic the Dynamics 365 data, with a few
problems planted on purpose, then runs every check type and prints the results.

Usage (from the repo root):
    pip install -r requirements-dev.txt
    python demo/run_local_demo.py
"""
from pathlib import Path
import shutil
import tempfile

from pyspark.sql import SparkSession

ENGINE = Path(__file__).resolve().parents[1] / "notebooks" / "dataquality" / "dq_engine.py"
exec(compile(ENGINE.read_text(), str(ENGINE), "exec"))  # MAGIC lines are comments, so it runs as plain Python

warehouse = tempfile.mkdtemp(prefix="dq-demo-")
spark = (SparkSession.builder.master("local[1]").appName("dq-demo")
         .config("spark.sql.warehouse.dir", warehouse)
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

for db in ("bronze", "silver"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def save(rows, columns, name):
    spark.createDataFrame(rows, columns).write.mode("overwrite").saveAsTable(name)


# --- Bronze: raw-but-typed data, with planted problems -------------------------------
save([("CC100", "Finance"), ("CC200", "Operations"), ("CC200", "Operations"),   # duplicate key
      ("CC300", "IT"), (None, "Unknown")],                                       # null key
     ["CostCenterNumber", "Name"], "bronze.costcenter")
save([("CAD", "Canadian Dollar"), ("USD", "US Dollar"), ("INR", "Indian Rupee")],
     ["CurrencyId", "Name"], "bronze.currency")
save([("V001", "Acme Supplies"), ("V002", "Northwind"), ("V003", "Contoso")],
     ["VendId", "VendorName"], "bronze.vendtable")
save([("PO1", 1, 250.0), ("PO1", 2, 100.0), ("PO2", 1, 75.5)],                   # composite key, clean
     ["PoNumber", "LineItem", "Amount"], "bronze.purchaseorder")
save([("SO1", 1, 3.0), ("SO1", 2, 5.0), ("SO2", 1, 2.0)],
     ["SalesOrderNumber", "SalesOrderLine", "Qty"], "bronze.salesorderline")

# --- Silver: modeled tables ------------------------------------------------------------
save([(1, "CC100"), (2, "CC200"), (3, "CC300")], ["CostCenterKey", "CostCenterNumber"], "silver.dimcostcenter")
save([(1, "CAD"), (2, "USD"), (3, "INR")], ["CurrencyKey", "CurrencyId"], "silver.dimcurrency")
save([(1, "V001"), (2, "V002")], ["VendorKey", "VendId"], "silver.dimvendor")              # one vendor lost
save([("SO1", 1, 3.0), ("SO1", 2, 4.0), ("SO2", 1, 2.0)],                                   # quantity changed
     ["SalesOrderNumber", "SalesOrderLine", "Qty"], "silver.factsalesorderline")

# --- Rules (the same shape as rows in dqr.Vw_Rules) -----------------------------------
rules = [
    DQRule(1, "PrimaryKey", "costcenter", "bronze", attribute1="CostCenterNumber"),
    DQRule(2, "PrimaryKey", "currency", "bronze", attribute1="CurrencyId"),
    DQRule(3, "PrimaryKey", "purchaseorder", "bronze", attribute1="PoNumber,LineItem"),
    DQRule(101, "NullCheck", "costcenter", "bronze", attribute1="CostCenterNumber"),
    DQRule(102, "NullCheck", "vendtable", "bronze", attribute1="VendId"),
    DQRule(201, "RecordCount", "currency", "bronze", "silver", target_object="dimcurrency"),
    DQRule(202, "RecordCount", "vendtable", "bronze", "silver", target_object="dimvendor"),
    DQRule(301, "SumCheck", "salesorderline", "bronze", "silver", "Qty", "Qty", "factsalesorderline"),
]

results = DQEngine(spark).run(rules, save=False).orderBy("rule_id")

print("\nData quality results")
print("-" * 92)
print(f"{'Rule':>5}  {'Check':<12} {'Table':<16} {'Status':<7} {'Failed':>6}  Message")
print("-" * 92)
for r in results.collect():
    print(f"{r.rule_id:>5}  {r.rule_name:<12} {r.object_name:<16} {r.status:<7} {r.failed_records:>6}  {r.message}")
print("-" * 92)
summary = {row["status"]: row["count"] for row in results.groupBy("status").count().collect()}
print("Summary:", ", ".join(f"{k}={v}" for k, v in sorted(summary.items())))

# Show the actual bad rows for the duplicate-key failure, like dq_badrecords would hold.
_, bad_rows = PrimaryKeyCheck(spark, rules[0], "demo").run()
print("\nDuplicate rows found in bronze.costcenter:")
bad_rows.show(truncate=False)

spark.stop()
shutil.rmtree(warehouse, ignore_errors=True)
