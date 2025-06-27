USE SCHEMA TEST_DB.PUBLIC;

-- OUTPUT AUDIT TABLE
CREATE OR REPLACE TABLE test_db.public.inventory_metrics_collection (
  database_name                        STRING,
  schema_name                        STRING,
  table_name                      STRING,            -- name of the audited table
  row_count                       NUMBER,            -- latest row count
  last_dml_ts                     TIMESTAMP,
  last_query_ts                   TIMESTAMP,     -- timestamp of last query on the table
  clone_lineage                   ARRAY,           -- JSON array or object describing clone hierarchy
  cluster_by_keys                 STRING,           -- JSON array of clustering key expressions
  average_overlaps                FLOAT,             -- avg. micro-partition overlap percentage
  average_depth                   FLOAT,             -- avg. clustering depth
  clustering_errors               ARRAY,            -- count of any clustering errors
  partition_depth_histogram       OBJECT,           -- JSON histogram of partition depths
  total_partition_count           NUMBER,            -- total number of micro-partitions
  total_constant_partition_count  NUMBER,            -- number of constant (unchanged) partitions
  mv_definitions                  ARRAY,           -- JSON array of materialized view definitions
  audit_ts                        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP  -- when this record was inserted
);

-- 1) DDL: create a dummy customer table
CREATE OR REPLACE TABLE test_db.public.customer (
  customer_id     INTEGER,
  first_name      STRING,
  last_name       STRING,
  email           STRING,
  signup_date     TIMESTAMP_NTZ,
  status          STRING
);

-- 2) DML: initial load of customers
INSERT INTO test_db.public.customer
  (customer_id, first_name, last_name, email, signup_date, status) VALUES
  (1001, 'Alice',    'Anderson',  'alice.anderson@example.com',  TO_TIMESTAMP_NTZ('2025-01-15 10:00:00'), 'ACTIVE'),
  (1002, 'Bob',      'Bennett',   'bob.bennett@example.com',     TO_TIMESTAMP_NTZ('2025-02-20 14:30:00'), 'ACTIVE'),
  (1003, 'Carol',    'Carter',    'carol.carter@example.com',    TO_TIMESTAMP_NTZ('2025-03-05 09:45:00'), 'ACTIVE'),
  (1004, 'Dave',     'Dawson',    'dave.dawson@example.com',     TO_TIMESTAMP_NTZ('2025-03-18 16:20:00'), 'INACTIVE'),
  (1005, 'Eve',      'Edwards',   'eve.edwards@example.com',     TO_TIMESTAMP_NTZ('2025-04-01 11:10:00'), 'ACTIVE');