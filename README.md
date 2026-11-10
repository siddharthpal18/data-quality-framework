# Data Quality Framework for a Modern Data Warehouse

A medallion lakehouse on **Azure Databricks** that ingests 13 Dynamics 365 tables, plus a **class-based, metadata-driven data quality engine** that checks the data before anyone uses it.

## The problem

Dynamics 365 exports its tables as headerless CSV files in the Common Data Model (CDM) format. Before that data can feed reports, it needs a proper schema, clean modeling, and automatic checks that catch duplicates, missing values, and rows lost between layers.

## Architecture

```
Dynamics 365 (CDM export)
        │  headerless CSV + .cdm.json
        ▼
ADLS Gen2 ──► Raw ──► Bronze (Delta) ──► Silver (Delta, 7 dimensions)
                          │                   │
                          └──── DQ Engine ◄───┘
                                   ▲
                     rules from Azure SQL (dqr.Vw_Rules)
                                   │
                     results ► dq.dqresults
                     failed rows ► dq_badrecords.*
```

**Pipeline highlights**

- A custom reader parses each entity's `.cdm.json` definition into a Spark schema and enforces it on the headerless CSVs.
- Raw → Bronze → Silver medallion layers stored as Delta tables.
- 7 silver dimensions, including a date dimension joined to the fiscal calendar, with hash-based surrogate keys and UTC-to-local timestamp conversion.
- ADLS Gen2 access through a service principal (OAuth), with credentials in Databricks secret scopes.

## How the data quality engine works

The engine lives in `notebooks/dataquality/dq_engine.py`.

| Check | What it catches |
|---|---|
| `PrimaryKey` | Duplicate keys, including composite keys |
| `NullCheck` | Empty values in required columns |
| `RecordCount` | Rows lost or added between bronze and silver |
| `SumCheck` | Totals that don't match between layers |

**Design**

- Every check is a subclass of an abstract `DQCheck` base class and implements two methods: `evaluate()` (measure) and `passed()` (decide).
- The shared `run()` method in the base class handles loading tables, building the result, and catching errors, so one broken rule never stops the whole run.
- A `@register` decorator adds each check to a registry. Rule names are normalized, so `PrimaryKeyCheck`, `primary_key`, and `Primary Key` all resolve to the same class.
- **Adding a new check type means writing one new class.** The engine itself doesn't change.
- **Adding a rule means inserting one row** in the Azure SQL metadata tables. No code change.

Each run gets a `run_id`. Results go to `dq.dqresults`, and failed rows are saved to `dq_badrecords.<table>_<check>` for investigation.

## Project structure

```
data-quality-framework/
├── notebooks/
│   └── dataquality/
│       ├── dq_engine.py           # base class, checks, registry, engine
│       ├── run_data_quality.py    # loads rules from Azure SQL and runs the engine
│       └── run_demo_free_edition.py  # demo for Databricks Free Edition
├── sql/
│   └── dq_metadata.sql            # rule tables, view, rules for 13 tables
├── demo/
│   └── run_local_demo.py          # runs every check on a laptop
├── tests/
│   └── test_dq_engine.py          # 10 pytest tests
├── requirements-dev.txt
└── README.md
```

## Run it locally (no cloud account needed)

```bash
pip install -r requirements-dev.txt
python demo/run_local_demo.py
```

The demo builds small bronze and silver tables with problems planted on purpose, runs all four check types, and prints:

```
 Rule  Check        Table            Status  Failed  Message
    1  PrimaryKey   costcenter       Fail         2  1 duplicate key value(s) on CostCenterNumber
    2  PrimaryKey   currency         Pass         0  0 duplicate key value(s) on CurrencyId
    3  PrimaryKey   purchaseorder    Pass         0  0 duplicate key value(s) on PoNumber, LineItem
  101  NullCheck    costcenter       Fail         1  1 row(s) with nulls in CostCenterNumber
  102  NullCheck    vendtable        Pass         0  0 row(s) with nulls in VendId
  201  RecordCount  currency         Pass         0  3 rows in source vs 3 in target
  202  RecordCount  vendtable        Fail         0  3 rows in source vs 2 in target
  301  SumCheck     salesorderline   Fail         0  source=10.0, target=9.0
Summary: Fail=4, Pass=4
```

Requires Python 3.9+ and Java 11+ (for Spark).

## Run it on Databricks Free Edition (no Azure needed)

1. Sign up for Databricks Free Edition.
2. In your workspace, create a folder and import `dq_engine.py` and `run_demo_free_edition.py` into it.
3. Open `run_demo_free_edition.py` and click **Run all**.

It creates demo Delta tables, runs every check, and saves results to `dq.dqresults` and failed rows to `dq_badrecords`.

![Data quality results on Databricks] <img width="1332" height="520" alt="image" src="https://github.com/user-attachments/assets/6f242541-ed2d-413d-af26-5c0180db9fce" />


## Run it on Azure Databricks

1. Run `sql/dq_metadata.sql` in your Azure SQL Database to create the rule tables and the `dqr.Vw_Rules` view.
2. Import the `notebooks/dataquality` folder into your Databricks workspace.
3. In `run_data_quality.py`, set your SQL server, database, and secret scope (keys `sql-user` and `sql-password`).
4. Run `run_data_quality.py`. To try it without Azure SQL, set `USE_SAMPLE_RULES = True`.

## Tests

The tests run on a local Spark session, no Databricks needed:

```bash
pip install -r requirements-dev.txt
pytest tests/
```

They cover every check type, composite keys, rule-name normalization, placeholder handling, and errors for missing tables and unknown rules.

## Tech stack

Python · PySpark · Azure Databricks · Delta Lake · ADLS Gen2 · Azure SQL Database · pytest

## What I'd improve next

- Add more check types, such as freshness, value ranges, and referential integrity.
- Schedule the run as a Databricks Workflow after each silver load.
- Build a small dashboard on `dq.dqresults` to track quality over time.

## Credits

The pipeline started from a Cloud and Data Universe course project. I extended it with the custom CDM reader, the class-based data quality engine, the metadata design for all 13 tables, and the test suite.
