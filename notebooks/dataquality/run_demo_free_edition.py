# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Demo (Databricks Free Edition)
# MAGIC
# MAGIC Runs the engine on Databricks without Azure SQL or ADLS. Creates small bronze and silver
# MAGIC Delta tables with problems planted on purpose, runs every check type, and saves results to
# MAGIC `dq.dqresults` and failed rows to the `dq_badrecords` schema.
# MAGIC
# MAGIC Keep this notebook in the same folder as `dq_engine`.

# COMMAND ----------

# MAGIC %run ./dq_engine

# COMMAND ----------

# MAGIC %md ### 1. Create demo bronze and silver tables

# COMMAND ----------

for schema in ("bronze", "silver"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def save(rows, columns, name):
    spark.createDataFrame(rows, columns).write.format("delta").mode("overwrite").saveAsTable(name)


# Bronze, with planted problems
save([("CC100", "Finance"), ("CC200", "Operations"), ("CC200", "Operations"),   # duplicate key
      ("CC300", "IT"), (None, "Unknown")],                                       # null key
     ["CostCenterNumber", "Name"], "bronze.costcenter")
save([("CAD", "Canadian Dollar"), ("USD", "US Dollar"), ("INR", "Indian Rupee")],
     ["CurrencyId", "Name"], "bronze.currency")
save([("V001", "Acme Supplies"), ("V002", "Northwind"), ("V003", "Contoso")],
     ["VendId", "VendorName"], "bronze.vendtable")
save([("PO1", 1, 250.0), ("PO1", 2, 100.0), ("PO2", 1, 75.5)],
     ["PoNumber", "LineItem", "Amount"], "bronze.purchaseorder")
save([("SO1", 1, 3.0), ("SO1", 2, 5.0), ("SO2", 1, 2.0)],
     ["SalesOrderNumber", "SalesOrderLine", "Qty"], "bronze.salesorderline")

# Silver
save([(1, "CC100"), (2, "CC200"), (3, "CC300")], ["CostCenterKey", "CostCenterNumber"], "silver.dimcostcenter")
save([(1, "CAD"), (2, "USD"), (3, "INR")], ["CurrencyKey", "CurrencyId"], "silver.dimcurrency")
save([(1, "V001"), (2, "V002")], ["VendorKey", "VendId"], "silver.dimvendor")            # one vendor lost
save([("SO1", 1, 3.0), ("SO1", 2, 4.0), ("SO2", 1, 2.0)],                                 # quantity changed
     ["SalesOrderNumber", "SalesOrderLine", "Qty"], "silver.factsalesorderline")

print("Demo tables created")

# COMMAND ----------

# MAGIC %md ### 2. Define rules (same shape as rows in dqr.Vw_Rules)

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md ### 3. Run the engine and save results

# COMMAND ----------

results = DQEngine(spark).run(rules, save=True)
display(results.orderBy("rule_id"))

# COMMAND ----------

display(results.groupBy("status").count())

# COMMAND ----------

# MAGIC %md ### 4. Look at the saved results and failed rows

# COMMAND ----------

display(spark.table("dq.dqresults").orderBy(F.desc("run_datetime"), "rule_id"))

# COMMAND ----------

display(spark.table("dq_badrecords.costcenter_primarykey"))
