import snowflake.connector
from snowflake.snowpark.functions import upper
from snowflake.connector import DictCursor
import json

connection_params = {
    "user" : ''
    , "password" : ''
    , "account" : ""
    , "warehouse" : ""
    , "role" : ""
}

with snowflake.connector.connect(**connection_params) as conn:
    with conn.cursor(DictCursor) as cur:
        db_name = 'test_db'
        sch_name = 'public'
        table_name = 'customer1'
        # 1) row count
        cur.execute("SELECT COUNT(*) AS row_count FROM test_db.public.customer1;")
        for row in cur:
            print("row_count")
            row_count = row['ROW_COUNT']
            print(row_count)

        # 2) last DML timestamp on this table
        cur.execute(f"""
            SELECT 
                start_time 
                , query_type
            FROM snowflake.account_usage.query_history
            WHERE query_text ILIKE '%%customer1%%'
            AND query_type IN ('INSERT','UPDATE','DELETE','MERGE')
            AND database_name ilike 'test_db'
            AND schema_name ilike 'public'
            order by start_time desc
            limit 1;
        """)
        for row in cur:
            print("last_dml_ts")
            last_dml_ts = row['START_TIME']
            dml_type = row['QUERY_TYPE']
            print(last_dml_ts)

        # 3) last time this table was queried
        cur.execute(f"""
            SELECT 
                MAX(start_time) query_start_ts
            FROM snowflake.account_usage.query_history
            WHERE DATABASE_NAME ILIKE 'TEST_DB'
            AND SCHEMA_NAME ILIKE 'PUBLIC'
            AND query_text ILIKE '%%customer1%%'
            AND query_text NOT ILIKE '%snowflake.account_usage.query_history%';
        """)
        for row in cur:
            print("last_query_ts")
            last_query_ts = row['QUERY_START_TS']
            print(last_query_ts)

        # 4) clone lineage
        cur.execute(f"""
                    select
                        ARRAY_AGG(CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.',TABLE_NAME)) WITHIN GROUP (ORDER BY TABLE_CREATED DESC) AS CLONE_LINEAGE
                    from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
                    where clone_group_id IN (
                        select 
                            id
                        from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
                        where table_name ILIKE 'CUSTOMER1'
                        AND TABLE_CATALOG ILIKE 'TEST_DB'
                    );
                   """)
        for row in cur:
            print("clustering")
            clone_lineage = row['CLONE_LINEAGE']

        # 6) clustering information
        cur.execute(f"""
                    with cte as (
                        SELECT parse_json(SYSTEM$CLUSTERING_INFORMATION('test_db.public.customer1', '(signup_date)')) as cluster_stats
                    )
                    select
                        cluster_stats:cluster_by_keys::string as cluster_by_keys
                        , cluster_stats:average_overlaps::float as average_overlaps
                        , cluster_stats:average_depth::float as average_depth
                        , cluster_stats:clustering_errors::array as clustering_errors
                        , cluster_stats:partition_depth_histogram::object as partition_depth_histogram
                        , cluster_stats:total_partition_count::int as total_partition_count
                        , cluster_stats:total_constant_partition_count::int as total_constant_partition_count
                    from cte;
                """)
        for row in cur:
            print("clustering")
            cluster_by_keys = row['CLUSTER_BY_KEYS']
            average_overlaps = row['AVERAGE_OVERLAPS']
            average_depth = row['AVERAGE_DEPTH']
            clustering_errors = row['CLUSTERING_ERRORS']
            partition_depth_histogram = row['PARTITION_DEPTH_HISTOGRAM']
            total_partition_count = row['TOTAL_PARTITION_COUNT']
            total_constant_partition_count = row['TOTAL_CONSTANT_PARTITION_COUNT']
        print(cluster_by_keys)

        # 7) materialized views that reference this table
        cur.execute(f"""
            SHOW MATERIALIZED VIEWS;
        """)
        print("mviews")
        mv_list = []
        for mview in cur:
            if (mview['source_database_name'] == 'TEST_DB') & (mview['source_schema_name'] == 'PUBLIC') & (mview['source_table_name'] == 'CUSTOMER1'):
                mv_definition = mview['text']
                mv_list.append(mv_definition)
        print(mv_list)
        mv_list = json.dumps(mv_list, separators=(',',':')).replace("'", "''")
        
        print("mv_list")
        print(mv_list)
        print(type(mv_list))
        
        target_db = 'test_db'
        target_schema = 'public'
        target_table = 'inventory_metrics_collection'
        insert_string = f"""
                    INSERT INTO {target_db}.{target_schema}.{target_table} (
                        database_name,
                        schema_name,
                        table_name,
                        row_count,
                        dml_type,
                        last_dml_ts,
                        last_query_ts,
                        clone_lineage,
                        cluster_by_keys,
                        average_overlaps,
                        average_depth,
                        clustering_errors,
                        partition_depth_histogram,
                        total_partition_count,
                        total_constant_partition_count,
                        mv_definitions
                        )
                    SELECT 
                        '{db_name}' as database_name,
                        '{sch_name}' as schema_name,
                        '{table_name}' as table_name,
                        {row_count} AS row_count,
                        '{dml_type}' AS dml_type,
                        TRY_TO_TIMESTAMP('{last_dml_ts}') as last_dml_ts,
                        TRY_TO_TIMESTAMP('{last_query_ts}') as last_query_ts,
                        parse_json($${clone_lineage}$$) as clone_lineage,
                        '{cluster_by_keys}' as cluster_by_keys,
                        {average_overlaps} as average_overlaps,
                        {average_depth} as average_depth,
                        parse_json('{clustering_errors}') as clustering_errors,
                        parse_json('{partition_depth_histogram}') as partition_depth_histogram,
                        {total_partition_count} as total_partition_count,
                        {total_constant_partition_count} as total_constant_partition_count,
                        parse_json($${mv_list}$$) as mv_definitions;
                    """
        print(insert_string)
        cur.execute(insert_string)