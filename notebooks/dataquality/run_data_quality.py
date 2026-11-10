# Databricks notebook source
# MAGIC %md
# MAGIC # Run Data Quality Checks
# MAGIC
# MAGIC Loads the active rules from Azure SQL (`dqr.Vw_Rules`), runs them with the class-based engine,
# MAGIC and writes results to `dq.dqresults` and failed rows to the `dq_badrecords` schema.
# MAGIC Set `USE_SAMPLE_RULES = True` to run without Azure SQL.

# COMMAND ----------

# MAGIC %run ./dq_engine

# COMMAND ----------

USE_SAMPLE_RULES = False

# Replace the placeholders with your own values. Credentials come from a Databricks secret scope.
SQL_SERVER = "<sql-server-name>"
SQL_DATABASE = "<sql-database-name>"
SECRET_SCOPE = "<secret-scope>"

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER}.database.windows.net:1433;"
    f"database={SQL_DATABASE};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)

# COMMAND ----------

if USE_SAMPLE_RULES:
    rules = SAMPLE_RULES
else:
    rules = load_rules_from_sql(
        spark,
        jdbc_url=JDBC_URL,
        user=dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-user"),
        password=dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-password"),
    )

print(f"Loaded {len(rules)} rules")

# COMMAND ----------

results = DQEngine(spark).run(rules, save=True)
display(results.orderBy("rule_id"))

# COMMAND ----------

# MAGIC %md ### Summary

# COMMAND ----------

display(results.groupBy("status").count())

# COMMAND ----------

# Fail the job when any rule fails or errors, so a scheduled pipeline stops before bad data moves on.
problems = results.filter("status != 'Pass'").count()
if problems:
    raise Exception(f"{problems} data quality rule(s) did not pass. See dq.dqresults for details.")
