import duckdb
from psycopg2.extras import execute_values
from loguru import logger
from src.config import settings
from src.utils.db import get_db_connection, init_db_schema
from src.utils.minio_client import get_minio_client


def setup_duckdb_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Configures DuckDB to connect to local MinIO (S3 compatible object storage)."""
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{settings.minio_endpoint}';")
    conn.execute(f"SET s3_access_key_id='{settings.minio_access_key}';")
    conn.execute(f"SET s3_secret_access_key='{settings.minio_secret_key}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")


def check_files_exist(prefix: str) -> bool:
    """Checks if there are any objects in the Silver bucket matching the prefix."""
    client = get_minio_client()
    try:
        objects = client.list_objects(settings.bucket_silver, prefix=prefix, recursive=True)
        return any(objects)
    except Exception as e:
        logger.error(f"Error checking objects in bucket '{settings.bucket_silver}' with prefix '{prefix}': {e}")
        return False


def aggregate_heart_rate(duck_conn) -> None:
    """Aggregates Heart Rate silver data and writes to Postgres daily summary table."""
    prefix = "records_type=heart_rate"
    if not check_files_exist(prefix):
        logger.warning(f"No silver Parquet files found for heart rate ('{prefix}'). Skipping HR aggregation.")
        return

    logger.info("Aggregating daily heart rate statistics...")
    query = f"""
        SELECT 
            CAST(start_date AS DATE) as date,
            AVG(value) as avg_heart_rate,
            MIN(value) as min_heart_rate,
            MAX(value) as max_heart_rate,
            COUNT(value)::INTEGER as records_count
        FROM read_parquet('s3://{settings.bucket_silver}/{prefix}/*.parquet')
        GROUP BY 1
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning("No heart rate data yielded from query.")
        return

    # Load into Postgres
    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_heart_rate_summary 
                (date, avg_heart_rate, min_heart_rate, max_heart_rate, records_count)
                VALUES %s
                ON CONFLICT (date) DO UPDATE SET
                    avg_heart_rate = EXCLUDED.avg_heart_rate,
                    min_heart_rate = EXCLUDED.min_heart_rate,
                    max_heart_rate = EXCLUDED.max_heart_rate,
                    records_count = EXCLUDED.records_count,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (r["date"], r["avg_heart_rate"], r["min_heart_rate"], r["max_heart_rate"], int(r["records_count"]))
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_heart_rate_summary'.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert heart rate summaries: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_activity(duck_conn) -> None:
    """Aggregates Step Count, active/basal energy and distance into daily activity summary."""
    prefixes = [
        "records_type=step_count",
        "records_type=active_energy",
        "records_type=basal_energy",
        "records_type=distance"
    ]
    
    active_prefixes = [p for p in prefixes if check_files_exist(p)]
    if not active_prefixes:
        logger.warning("No silver Parquet files found for any activity metrics. Skipping Activity aggregation.")
        return

    logger.info("Aggregating daily activity metrics...")

    cte_defs = []
    
    if "records_type=step_count" in active_prefixes:
        cte_defs.append(f"""
            steps AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as step_count
                FROM read_parquet('s3://{settings.bucket_silver}/records_type=step_count/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("steps AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as step_count WHERE 1=0)")

    if "records_type=active_energy" in active_prefixes:
        cte_defs.append(f"""
            active_energy AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as active_energy_kcal
                FROM read_parquet('s3://{settings.bucket_silver}/records_type=active_energy/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("active_energy AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as active_energy_kcal WHERE 1=0)")

    if "records_type=basal_energy" in active_prefixes:
        cte_defs.append(f"""
            basal_energy AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as basal_energy_kcal
                FROM read_parquet('s3://{settings.bucket_silver}/records_type=basal_energy/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("basal_energy AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as basal_energy_kcal WHERE 1=0)")

    if "records_type=distance" in active_prefixes:
        cte_defs.append(f"""
            distance AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as distance_km
                FROM read_parquet('s3://{settings.bucket_silver}/records_type=distance/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("distance AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as distance_km WHERE 1=0)")

    cte_block = ",\n".join(cte_defs)

    query = f"""
        WITH {cte_block}
        SELECT 
            coalesce(s.date, ae.date, be.date, d.date) as date,
            coalesce(s.step_count, 0.0) as step_count,
            coalesce(ae.active_energy_kcal, 0.0) as active_energy_kcal,
            coalesce(be.basal_energy_kcal, 0.0) as basal_energy_kcal,
            coalesce(d.distance_km, 0.0) as distance_km
        FROM steps s
        FULL OUTER JOIN active_energy ae ON s.date = ae.date
        FULL OUTER JOIN basal_energy be ON coalesce(s.date, ae.date) = be.date
        FULL OUTER JOIN distance d ON coalesce(s.date, ae.date, be.date) = d.date
        WHERE coalesce(s.date, ae.date, be.date, d.date) IS NOT NULL
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning("No activity data yielded from query.")
        return

    # Load into Postgres
    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_activity_summary 
                (date, step_count, active_energy_kcal, basal_energy_kcal, distance_km)
                VALUES %s
                ON CONFLICT (date) DO UPDATE SET
                    step_count = EXCLUDED.step_count,
                    active_energy_kcal = EXCLUDED.active_energy_kcal,
                    basal_energy_kcal = EXCLUDED.basal_energy_kcal,
                    distance_km = EXCLUDED.distance_km,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (r["date"], r["step_count"], r["active_energy_kcal"], r["basal_energy_kcal"], r["distance_km"])
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_activity_summary'.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert activity summaries: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_workouts(duck_conn) -> None:
    """Aggregates Workout data and writes to Postgres workouts summary table."""
    prefix = "workouts"
    if not check_files_exist(prefix):
        logger.warning(f"No silver Parquet files found for workouts ('{prefix}'). Skipping workout aggregation.")
        return

    logger.info("Aggregating workouts...")
    query = f"""
        SELECT 
            workout_type,
            start_date,
            first(end_date) as end_date,
            first(duration) as duration_minutes,
            first(total_energy_burned) as total_energy_kcal,
            first(total_distance) as total_distance_km
        FROM read_parquet('s3://{settings.bucket_silver}/{prefix}/*.parquet')
        GROUP BY workout_type, start_date
        ORDER BY start_date
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning("No workout data yielded from query.")
        return

    # Load into Postgres
    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.workouts_summary 
                (workout_type, start_date, end_date, duration_minutes, total_energy_kcal, total_distance_km)
                VALUES %s
                ON CONFLICT (workout_type, start_date) DO UPDATE SET
                    end_date = EXCLUDED.end_date,
                    duration_minutes = EXCLUDED.duration_minutes,
                    total_energy_kcal = EXCLUDED.total_energy_kcal,
                    total_distance_km = EXCLUDED.total_distance_km,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (
                    r["workout_type"],
                    r["start_date"],
                    r["end_date"],
                    r["duration_minutes"],
                    r["total_energy_kcal"],
                    r["total_distance_km"]
                )
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'workouts_summary'.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert workout summaries: {e}")
        raise e
    finally:
        pg_conn.close()


def process_gold() -> None:
    """Orchestrates Gold schema creation, aggregates data in DuckDB, and loads Postgres."""
    init_db_schema()

    duck_conn = duckdb.connect()
    setup_duckdb_s3(duck_conn)

    aggregate_heart_rate(duck_conn)
    aggregate_activity(duck_conn)
    aggregate_workouts(duck_conn)

    logger.info("Gold layer loading complete.")


if __name__ == "__main__":
    process_gold()
