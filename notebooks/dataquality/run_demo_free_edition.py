# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Demo (Databricks Free Edition)
# MAGIC
# MAGIC Runs the engine on Databricks without Azure SQL or ADLS. Creates small demo tables in separate
# MAGIC `demo_bronze` and `demo_silver` schemas (so real bronze and silver tables are never touched),
# MAGIC with problems planted on purpose, runs every check type, and saves results to
# MAGIC `dq.dqresults` and failed rows to the `dq_badrecords` schema.
# MAGIC
# MAGIC Keep this notebook in the same folder as `dq_engine`.

# COMMAND ----------

# MAGIC %run ./dq_engine

# COMMAND ----------

# MAGIC %md ### 1. Create demo tables (in demo_bronze and demo_silver, separate from real data)

# COMMAND ----------

for schema in ("demo_bronze", "demo_silver"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def save(rows, columns, name):
    spark.createDataFrame(rows, columns).write.format("delta").mode("overwrite").saveAsTable(name)


# Bronze, with planted problems
save([("CC100", "Finance"), ("CC200", "Operations"), ("CC200", "Operations"),   # duplicate key
      ("CC300", "IT"), (None, "Unknown")],                                       # null key
     ["CostCenterNumber", "Name"], "demo_bronze.costcenter")
save([("CAD", "Canadian Dollar"), ("USD", "US Dollar"), ("INR", "Indian Rupee")],
     ["CurrencyId", "Name"], "demo_bronze.currency")
save([("V001", "Acme Supplies"), ("V002", "Northwind"), ("V003", "Contoso")],
     ["VendId", "VendorName"], "demo_bronze.vendtable")
save([("PO1", 1, 250.0), ("PO1", 2, 100.0), ("PO2", 1, 75.5)],
     ["PoNumber", "LineItem", "Amount"], "demo_bronze.purchaseorder")
save([("SO1", 1, 3.0), ("SO1", 2, 5.0), ("SO2", 1, 2.0)],
     ["SalesOrderNumber", "SalesOrderLine", "Qty"], "demo_bronze.salesorderline")

# Silver
save([(1, "CC100"), (2, "CC200"), (3, "CC300")], ["CostCenterKey", "CostCenterNumber"], "demo_silver.dimcostcenter")
save([(1, "CAD"), (2, "USD"), (3, "INR")], ["CurrencyKey", "CurrencyId"], "demo_silver.dimcurrency")
save([(1, "V001"), (2, "V002")], ["VendorKey", "VendId"], "demo_silver.dimvendor")            # one vendor lost
save([("SO1", 1, 3.0), ("SO1", 2, 4.0), ("SO2", 1, 2.0)],                                      # quantity changed
     ["SalesOrderNumber", "SalesOrderLine", "Qty"], "demo_silver.factsalesorderline")

print("Demo tables created")

# COMMAND ----------

# MAGIC %md ### 2. Define rules (same shape as rows in dqr.Vw_Rules)

# COMMAND ----------

rules = [
    DQRule(1, "PrimaryKey", "costcenter", "demo_bronze", attribute1="CostCenterNumber"),
    DQRule(2, "PrimaryKey", "currency", "demo_bronze", attribute1="CurrencyId"),
    DQRule(3, "PrimaryKey", "purchaseorder", "demo_bronze", attribute1="PoNumber,LineItem"),
    DQRule(101, "NullCheck", "costcenter", "demo_bronze", attribute1="CostCenterNumber"),
    DQRule(102, "NullCheck", "vendtable", "demo_bronze", attribute1="VendId"),
    DQRule(201, "RecordCount", "currency", "demo_bronze", "demo_silver", target_object="dimcurrency"),
    DQRule(202, "RecordCount", "vendtable", "demo_bronze", "demo_silver", target_object="dimvendor"),
    DQRule(301, "SumCheck", "salesorderline", "demo_bronze", "demo_silver", "Qty", "Qty", "factsalesorderline"),
]

# COMMAND ----------

# MAGIC %md ### 3. Run the engine and save results

# COMMAND ----------

results = DQEngine(spark).run(rules, save=True)
display(results.select("rule_id", "rule_name", "object_name", "status", "failed_records", "message").orderBy("rule_id"))

# COMMAND ----------

display(results.groupBy("status").count())

# COMMAND ----------

# MAGIC %md ### 4. Look at the saved results and failed rows

# COMMAND ----------

display(spark.table("dq.dqresults").orderBy(F.desc("run_datetime"), "rule_id"))

# COMMAND ----------

display(spark.table("dq_badrecords.costcenter_primarykey"))
