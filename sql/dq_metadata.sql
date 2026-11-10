-- Data quality metadata for Azure SQL Database.
-- Run each GO-separated batch in order. Safe to re-run: existing objects are dropped first.

IF OBJECT_ID('dqr.Vw_Rules', 'V') IS NOT NULL DROP VIEW dqr.Vw_Rules;
IF OBJECT_ID('dqr.dqrules', 'U') IS NOT NULL DROP TABLE dqr.dqrules;
IF OBJECT_ID('dqr.dqobjects', 'U') IS NOT NULL DROP TABLE dqr.dqobjects;
IF OBJECT_ID('dqr.dqchecks', 'U') IS NOT NULL DROP TABLE dqr.dqchecks;
GO

IF SCHEMA_ID('dqr') IS NULL EXEC('CREATE SCHEMA dqr');
GO

-- Check types. dqrulename must match a class registered in dq_engine.py (case-insensitive).
CREATE TABLE dqr.dqchecks (
    dqcheckid        INT          PRIMARY KEY,
    dqrulename       VARCHAR(100) NOT NULL UNIQUE,
    dqruletype       VARCHAR(100) NOT NULL,          -- Single (one table) or Comparison (two layers)
    lastmodifieddate DATETIME2    NOT NULL DEFAULT SYSDATETIME()
);

-- Tables the rules run against.
CREATE TABLE dqr.dqobjects (
    dqobjectid       INT          PRIMARY KEY,
    dqobjectname     VARCHAR(100) NOT NULL UNIQUE,
    active           BIT          NOT NULL DEFAULT 1,
    lastmodifieddate DATETIME2    NOT NULL DEFAULT SYSDATETIME()
);

-- One row per rule. dqattribute1 = column(s), comma-separated for composite keys.
-- dqattribute2 = target column for SumCheck. targetobjectname = silver table for Comparison checks.
CREATE TABLE dqr.dqrules (
    dqruleid         INT          PRIMARY KEY,
    dqcheckid        INT          NOT NULL REFERENCES dqr.dqchecks(dqcheckid),
    dqobjectid       INT          NOT NULL REFERENCES dqr.dqobjects(dqobjectid),
    sourcelayer      VARCHAR(50)  NOT NULL,
    targetlayer      VARCHAR(50)  NULL,
    targetobjectname VARCHAR(100) NULL,
    dqattribute1     VARCHAR(200) NULL,
    dqattribute2     VARCHAR(200) NULL,
    active           BIT          NOT NULL DEFAULT 1,
    lastmodifieddate DATETIME2    NOT NULL DEFAULT SYSDATETIME()
);
GO

INSERT INTO dqr.dqchecks (dqcheckid, dqrulename, dqruletype) VALUES
    (1, 'PrimaryKey',  'Single'),
    (2, 'NullCheck',   'Single'),
    (3, 'RecordCount', 'Comparison'),
    (4, 'SumCheck',    'Comparison');

INSERT INTO dqr.dqobjects (dqobjectid, dqobjectname) VALUES
    (1, 'costcenter'), (2, 'currency'), (3, 'custtable'), (4, 'fiscalperiod'),
    (5, 'parties'), (6, 'partyaddress'), (7, 'promotable'), (8, 'purchaseorder'),
    (9, 'purchcategory'), (10, 'purchcontracts'), (11, 'purchitem'), (12, 'salesorderline'),
    (13, 'vendtable');

-- Primary key checks on all 13 bronze tables.
INSERT INTO dqr.dqrules (dqruleid, dqcheckid, dqobjectid, sourcelayer, dqattribute1) VALUES
    (1, 1, 1, 'bronze', 'CostCenterNumber'),
    (2, 1, 2, 'bronze', 'CurrencyId'),
    (3, 1, 3, 'bronze', 'CustomerId'),
    (4, 1, 4, 'bronze', 'FiscalPeriodName'),
    (5, 1, 5, 'bronze', 'PartyId'),
    (6, 1, 6, 'bronze', 'PartyNumber'),
    (7, 1, 7, 'bronze', 'PromotionId'),
    (8, 1, 8, 'bronze', 'PoNumber,LineItem'),
    (9, 1, 9, 'bronze', 'CategoryId'),
    (10, 1, 10, 'bronze', 'ContractId'),
    (11, 1, 11, 'bronze', 'ItemId'),
    (12, 1, 12, 'bronze', 'SalesOrderNumber,SalesOrderLine'),
    (13, 1, 13, 'bronze', 'VendId');

-- Null checks on key columns.
INSERT INTO dqr.dqrules (dqruleid, dqcheckid, dqobjectid, sourcelayer, dqattribute1) VALUES
    (101, 2, 1, 'bronze', 'CostCenterNumber'),
    (102, 2, 3, 'bronze', 'CustomerId'),
    (103, 2, 13, 'bronze', 'VendId'),
    (104, 2, 11, 'bronze', 'ItemId');

-- Row counts between bronze tables and the silver dimensions built from them.
INSERT INTO dqr.dqrules (dqruleid, dqcheckid, dqobjectid, sourcelayer, targetlayer, targetobjectname) VALUES
    (201, 3, 1, 'bronze', 'silver', 'dimcostcenter'),
    (202, 3, 2, 'bronze', 'silver', 'dimcurrency'),
    (203, 3, 5, 'bronze', 'silver', 'dimparty'),
    (204, 3, 9, 'bronze', 'silver', 'dimpurchasecategory'),
    (205, 3, 11, 'bronze', 'silver', 'dimpurchitem'),
    (206, 3, 13, 'bronze', 'silver', 'dimvendor');

-- SumCheck example for when the fact tables exist (amount column names are assumptions; adjust):
-- INSERT INTO dqr.dqrules (dqruleid, dqcheckid, dqobjectid, sourcelayer, targetlayer, targetobjectname, dqattribute1, dqattribute2)
-- VALUES (301, 4, 12, 'bronze', 'silver', 'factsalesorderline', 'Qty', 'Qty');
GO

-- Active rules only, in the shape dq_engine.rules_from_dataframe() expects.
CREATE VIEW dqr.Vw_Rules AS
SELECT R.dqruleid, C.dqrulename, C.dqruletype, O.dqobjectname,
       R.sourcelayer, R.targetlayer, R.targetobjectname, R.dqattribute1, R.dqattribute2
FROM dqr.dqrules R
JOIN dqr.dqchecks  C ON R.dqcheckid  = C.dqcheckid
JOIN dqr.dqobjects O ON R.dqobjectid = O.dqobjectid
WHERE R.active = 1 AND O.active = 1;
GO

SELECT * FROM dqr.Vw_Rules ORDER BY dqruleid;
