import duckdb
from psycopg2.extras import execute_values
from loguru import logger
from src.config import settings
from src.utils.db import get_db_connection, init_db_schema
from src.utils.minio_client import get_minio_client


def setup_duckdb_s3(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{settings.minio_endpoint}';")
    conn.execute(f"SET s3_access_key_id='{settings.minio_access_key}';")
    conn.execute(f"SET s3_secret_access_key='{settings.minio_secret_key}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")


def check_files_exist(user_id: str, prefix: str) -> bool:
    client = get_minio_client()
    try:
        objects = client.list_objects(settings.bucket_silver, prefix=f"{user_id}/{prefix}", recursive=True)
        return any(objects)
    except Exception as e:
        logger.error(f"Error checking objects in bucket '{settings.bucket_silver}' with prefix '{user_id}/{prefix}': {e}")
        return False


def aggregate_heart_rate(duck_conn, user_id: str) -> None:
    prefix = "records_type=heart_rate"
    if not check_files_exist(user_id, prefix):
        logger.warning(f"No silver Parquet files found for heart rate ('{user_id}/{prefix}'). Skipping HR aggregation.")
        return

    logger.info(f"Aggregating daily heart rate statistics for user {user_id}...")
    query = f"""
        SELECT 
            CAST(start_date AS DATE) as date,
            AVG(value) as avg_heart_rate,
            MIN(value) as min_heart_rate,
            MAX(value) as max_heart_rate,
            COUNT(value)::INTEGER as records_count
        FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/{prefix}/**/*.parquet')
        GROUP BY 1
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning(f"No heart rate data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_heart_rate_summary 
                (user_id, date, avg_heart_rate, min_heart_rate, max_heart_rate, records_count)
                VALUES %s
                ON CONFLICT (user_id, date) DO UPDATE SET
                    avg_heart_rate = EXCLUDED.avg_heart_rate,
                    min_heart_rate = EXCLUDED.min_heart_rate,
                    max_heart_rate = EXCLUDED.max_heart_rate,
                    records_count = EXCLUDED.records_count,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (user_id, r["date"], r["avg_heart_rate"], r["min_heart_rate"], r["max_heart_rate"], int(r["records_count"]))
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_heart_rate_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert heart rate summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_activity(duck_conn, user_id: str) -> None:
    prefixes = [
        "records_type=step_count",
        "records_type=active_energy",
        "records_type=basal_energy",
        "records_type=distance"
    ]

    active_prefixes = [p for p in prefixes if check_files_exist(user_id, p)]
    if not active_prefixes:
        logger.warning(f"No silver Parquet files found for any activity metrics for user {user_id}. Skipping Activity aggregation.")
        return

    logger.info(f"Aggregating daily activity metrics for user {user_id}...")

    cte_defs = []

    if "records_type=step_count" in active_prefixes:
        cte_defs.append(f"""
            steps_raw AS (
                SELECT 
                    start_date, 
                    end_date, 
                    value,
                    CASE 
                        WHEN source_name LIKE '%Watch%' THEN 1
                        WHEN source_name = 'Zepp' THEN 2
                        ELSE 3
                    END as priority
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=step_count/**/*.parquet')
            ),
            steps AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as step_count
                FROM steps_raw p1
                WHERE NOT EXISTS (
                    SELECT 1 FROM steps_raw p2
                    WHERE p2.priority < p1.priority
                      AND p2.start_date < p1.end_date
                      AND p1.start_date < p2.end_date
                )
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("steps AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as step_count WHERE 1=0)")

    if "records_type=active_energy" in active_prefixes:
        cte_defs.append(f"""
            active_energy_raw AS (
                SELECT 
                    start_date, 
                    end_date, 
                    value,
                    CASE 
                        WHEN source_name LIKE '%Watch%' THEN 1
                        WHEN source_name = 'Zepp' THEN 2
                        ELSE 3
                    END as priority
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=active_energy/**/*.parquet')
            ),
            active_energy AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as active_energy_kcal
                FROM active_energy_raw p1
                WHERE NOT EXISTS (
                    SELECT 1 FROM active_energy_raw p2
                    WHERE p2.priority < p1.priority
                      AND p2.start_date < p1.end_date
                      AND p1.start_date < p2.end_date
                )
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("active_energy AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as active_energy_kcal WHERE 1=0)")

    if "records_type=basal_energy" in active_prefixes:
        cte_defs.append(f"""
            basal_energy_raw AS (
                SELECT 
                    start_date, 
                    end_date, 
                    value,
                    CASE 
                        WHEN source_name LIKE '%Watch%' THEN 1
                        WHEN source_name = 'Zepp' THEN 2
                        ELSE 3
                    END as priority
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=basal_energy/**/*.parquet')
            ),
            basal_energy AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as basal_energy_kcal
                FROM basal_energy_raw p1
                WHERE NOT EXISTS (
                    SELECT 1 FROM basal_energy_raw p2
                    WHERE p2.priority < p1.priority
                      AND p2.start_date < p1.end_date
                      AND p1.start_date < p2.end_date
                )
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("basal_energy AS (SELECT CAST(NULL AS DATE) as date, CAST(0.0 AS DOUBLE PRECISION) as basal_energy_kcal WHERE 1=0)")

    if "records_type=distance" in active_prefixes:
        cte_defs.append(f"""
            distance_raw AS (
                SELECT 
                    start_date, 
                    end_date, 
                    value,
                    CASE 
                        WHEN source_name LIKE '%Watch%' THEN 1
                        WHEN source_name = 'Zepp' THEN 2
                        ELSE 3
                    END as priority
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=distance/**/*.parquet')
            ),
            distance AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as distance_km
                FROM distance_raw p1
                WHERE NOT EXISTS (
                    SELECT 1 FROM distance_raw p2
                    WHERE p2.priority < p1.priority
                      AND p2.start_date < p1.end_date
                      AND p1.start_date < p2.end_date
                )
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
        logger.warning(f"No activity data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_activity_summary 
                (user_id, date, step_count, active_energy_kcal, basal_energy_kcal, distance_km)
                VALUES %s
                ON CONFLICT (user_id, date) DO UPDATE SET
                    step_count = EXCLUDED.step_count,
                    active_energy_kcal = EXCLUDED.active_energy_kcal,
                    basal_energy_kcal = EXCLUDED.basal_energy_kcal,
                    distance_km = EXCLUDED.distance_km,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (user_id, r["date"], r["step_count"], r["active_energy_kcal"], r["basal_energy_kcal"], r["distance_km"])
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_activity_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert activity summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_workouts(duck_conn, user_id: str) -> None:
    prefix = "workouts"
    if not check_files_exist(user_id, prefix):
        logger.warning(f"No silver Parquet files found for workouts ('{user_id}/{prefix}'). Skipping workout aggregation.")
        return

    logger.info(f"Aggregating workouts for user {user_id}...")
    query = f"""
        SELECT 
            workout_type,
            start_date,
            first(end_date) as end_date,
            first(duration) as duration_minutes,
            first(total_energy_burned) as total_energy_kcal,
            first(total_distance) as total_distance_km
        FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/{prefix}/**/*.parquet')
        GROUP BY workout_type, start_date
        ORDER BY start_date
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning(f"No workout data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.workouts_summary 
                (user_id, workout_type, start_date, end_date, duration_minutes, total_energy_kcal, total_distance_km)
                VALUES %s
                ON CONFLICT (user_id, workout_type, start_date) DO UPDATE SET
                    end_date = EXCLUDED.end_date,
                    duration_minutes = EXCLUDED.duration_minutes,
                    total_energy_kcal = EXCLUDED.total_energy_kcal,
                    total_distance_km = EXCLUDED.total_distance_km,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (
                    user_id,
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
            logger.info(f"Upserted {len(rows)} records into Postgres table 'workouts_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert workout summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_sleep(duck_conn, user_id: str) -> None:
    prefix = "records_type=sleep"
    if not check_files_exist(user_id, prefix):
        logger.warning(f"No silver Parquet files found for sleep ('{user_id}/{prefix}'). Skipping sleep aggregation.")
        return

    logger.info(f"Aggregating daily sleep duration by stage for user {user_id}...")

    query = f"""
        SELECT 
            CAST(start_date AS DATE) as date,
            SUM(CASE WHEN value = 1.0 THEN (epoch(end_date) - epoch(start_date)) / 60.0 ELSE 0.0 END) as asleep_min,
            SUM(CASE WHEN value = 2.0 THEN (epoch(end_date) - epoch(start_date)) / 60.0 ELSE 0.0 END) as deep_min,
            SUM(CASE WHEN value = 3.0 THEN (epoch(end_date) - epoch(start_date)) / 60.0 ELSE 0.0 END) as light_min,
            SUM(CASE WHEN value = 4.0 THEN (epoch(end_date) - epoch(start_date)) / 60.0 ELSE 0.0 END) as rem_min,
            SUM(CASE WHEN value = 5.0 THEN (epoch(end_date) - epoch(start_date)) / 60.0 ELSE 0.0 END) as awake_min
        FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/{prefix}/**/*.parquet')
        GROUP BY 1
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning(f"No sleep data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_sleep_summary 
                (user_id, date, total_sleep_minutes, asleep_minutes, deep_sleep_minutes, light_sleep_minutes, rem_sleep_minutes, awake_minutes)
                VALUES %s
                ON CONFLICT (user_id, date) DO UPDATE SET
                    total_sleep_minutes = EXCLUDED.total_sleep_minutes,
                    asleep_minutes = EXCLUDED.asleep_minutes,
                    deep_sleep_minutes = EXCLUDED.deep_sleep_minutes,
                    light_sleep_minutes = EXCLUDED.light_sleep_minutes,
                    rem_sleep_minutes = EXCLUDED.rem_sleep_minutes,
                    awake_minutes = EXCLUDED.awake_minutes,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = []
            for r in res:
                asleep = r["asleep_min"] or 0.0
                deep = r["deep_min"] or 0.0
                light = r["light_min"] or 0.0
                rem = r["rem_min"] or 0.0
                awake = r["awake_min"] or 0.0
                total = asleep + deep + light + rem
                rows.append((user_id, r["date"], total, asleep, deep, light, rem, awake))

            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_sleep_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert sleep summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_cardiovascular(duck_conn, user_id: str) -> None:
    prefixes = [
        "records_type=resting_heart_rate",
        "records_type=hrv",
        "records_type=vo2_max",
        "records_type=blood_oxygen"
    ]
    active_prefixes = [p for p in prefixes if check_files_exist(user_id, p)]
    if not active_prefixes:
        logger.warning(f"No cardiovascular metrics parquets found for user {user_id}. Skipping cardio aggregation.")
        return

    logger.info(f"Aggregating daily cardiovascular indicators for user {user_id}...")

    cte_defs = []
    if "records_type=resting_heart_rate" in active_prefixes:
        cte_defs.append(f"""
            resting AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as resting_hr
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=resting_heart_rate/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("resting AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as resting_hr WHERE 1=0)")

    if "records_type=hrv" in active_prefixes:
        cte_defs.append(f"""
            hrv AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as hrv_val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=hrv/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("hrv AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as hrv_val WHERE 1=0)")

    if "records_type=vo2_max" in active_prefixes:
        cte_defs.append(f"""
            vo2 AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as vo2_max_val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=vo2_max/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("vo2 AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as vo2_max_val WHERE 1=0)")

    if "records_type=blood_oxygen" in active_prefixes:
        cte_defs.append(f"""
            ox AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as ox_avg, MIN(value) as ox_min
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=blood_oxygen/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("ox AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as ox_avg, CAST(NULL AS DOUBLE PRECISION) as ox_min WHERE 1=0)")

    cte_block = ",\n".join(cte_defs)

    query = f"""
        WITH {cte_block}
        SELECT 
            coalesce(r.date, h.date, v.date, o.date) as date,
            r.resting_hr,
            h.hrv_val as hrv_sdnn,
            v.vo2_max_val as vo2_max,
            o.ox_avg as blood_oxygen_avg,
            o.ox_min as blood_oxygen_min
        FROM resting r
        FULL OUTER JOIN hrv h ON r.date = h.date
        FULL OUTER JOIN vo2 v ON coalesce(r.date, h.date) = v.date
        FULL OUTER JOIN ox o ON coalesce(r.date, h.date, v.date) = o.date
        WHERE coalesce(r.date, h.date, v.date, o.date) IS NOT NULL
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning(f"No cardiovascular data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_cardiovascular_summary 
                (user_id, date, avg_resting_heart_rate, avg_hrv_sdnn, avg_vo2_max, avg_blood_oxygen, min_blood_oxygen)
                VALUES %s
                ON CONFLICT (user_id, date) DO UPDATE SET
                    avg_resting_heart_rate = EXCLUDED.avg_resting_heart_rate,
                    avg_hrv_sdnn = EXCLUDED.avg_hrv_sdnn,
                    avg_vo2_max = EXCLUDED.avg_vo2_max,
                    avg_blood_oxygen = EXCLUDED.avg_blood_oxygen,
                    min_blood_oxygen = EXCLUDED.min_blood_oxygen,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (user_id, r["date"], r["resting_hr"], r["hrv_sdnn"], r["vo2_max"], r["blood_oxygen_avg"], r["blood_oxygen_min"])
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_cardiovascular_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert cardiovascular summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_respiratory(duck_conn, user_id: str) -> None:
    prefixes = [
        "records_type=respiratory_rate",
        "records_type=breathing_disturbances"
    ]
    active_prefixes = [p for p in prefixes if check_files_exist(user_id, p)]
    if not active_prefixes:
        logger.warning(f"No respiratory metrics parquets found for user {user_id}. Skipping respiratory aggregation.")
        return

    logger.info(f"Aggregating daily respiratory status for user {user_id}...")

    cte_defs = []
    if "records_type=respiratory_rate" in active_prefixes:
        cte_defs.append(f"""
            resp AS (
                SELECT 
                    CAST(start_date AS DATE) as date, 
                    AVG(value) as avg_resp,
                    MIN(value) as min_resp,
                    MAX(value) as max_resp
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=respiratory_rate/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("resp AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as avg_resp, CAST(NULL AS DOUBLE PRECISION) as min_resp, CAST(NULL AS DOUBLE PRECISION) as max_resp WHERE 1=0)")

    if "records_type=breathing_disturbances" in active_prefixes:
        cte_defs.append(f"""
            disturb AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as avg_dist
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=breathing_disturbances/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("disturb AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as avg_dist WHERE 1=0)")

    cte_block = ",\n".join(cte_defs)

    query = f"""
        WITH {cte_block}
        SELECT 
            coalesce(r.date, d.date) as date,
            r.avg_resp as avg_respiratory_rate,
            r.min_resp as min_respiratory_rate,
            r.max_resp as max_respiratory_rate,
            d.avg_dist as avg_breathing_disturbances
        FROM resp r
        FULL OUTER JOIN disturb d ON r.date = d.date
        WHERE coalesce(r.date, d.date) IS NOT NULL
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning(f"No respiratory data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_respiratory_summary 
                (user_id, date, avg_respiratory_rate, min_respiratory_rate, max_respiratory_rate, avg_breathing_disturbances)
                VALUES %s
                ON CONFLICT (user_id, date) DO UPDATE SET
                    avg_respiratory_rate = EXCLUDED.avg_respiratory_rate,
                    min_respiratory_rate = EXCLUDED.min_respiratory_rate,
                    max_respiratory_rate = EXCLUDED.max_respiratory_rate,
                    avg_breathing_disturbances = EXCLUDED.avg_breathing_disturbances,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (user_id, r["date"], r["avg_respiratory_rate"], r["min_respiratory_rate"], r["max_respiratory_rate"], r["avg_breathing_disturbances"])
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_respiratory_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert respiratory summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def aggregate_metrics(duck_conn, user_id: str) -> None:
    prefixes = [
        "records_type=body_mass",
        "records_type=flights_climbed",
        "records_type=time_in_daylight",
        "records_type=walking_speed",
        "records_type=wrist_temperature",
        "records_type=exercise_time",
        "records_type=stand_hour",
        "records_type=physical_effort"
    ]
    active_prefixes = [p for p in prefixes if check_files_exist(user_id, p)]
    if not active_prefixes:
        logger.warning(f"No physical, stand, or environmental metrics parquets found for user {user_id}. Skipping metrics aggregation.")
        return

    logger.info(f"Aggregating daily physical and environmental metrics for user {user_id}...")

    cte_defs = []

    if "records_type=body_mass" in active_prefixes:
        cte_defs.append(f"""
            bm AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=body_mass/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("bm AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    if "records_type=flights_climbed" in active_prefixes:
        cte_defs.append(f"""
            fc_raw AS (
                SELECT 
                    start_date, 
                    end_date, 
                    value,
                    CASE 
                        WHEN source_name LIKE '%Watch%' THEN 1
                        WHEN source_name = 'Zepp' THEN 2
                        ELSE 3
                    END as priority
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=flights_climbed/**/*.parquet')
            ),
            fc AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as val
                FROM fc_raw p1
                WHERE NOT EXISTS (
                    SELECT 1 FROM fc_raw p2
                    WHERE p2.priority < p1.priority
                      AND p2.start_date < p1.end_date
                      AND p1.start_date < p2.end_date
                )
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("fc AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    if "records_type=time_in_daylight" in active_prefixes:
        cte_defs.append(f"""
            dl AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=time_in_daylight/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("dl AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    if "records_type=walking_speed" in active_prefixes:
        cte_defs.append(f"""
            ws AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=walking_speed/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("ws AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    if "records_type=wrist_temperature" in active_prefixes:
        cte_defs.append(f"""
            wt AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=wrist_temperature/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("wt AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    if "records_type=exercise_time" in active_prefixes:
        cte_defs.append(f"""
            et AS (
                SELECT CAST(start_date AS DATE) as date, SUM(value) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=exercise_time/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("et AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    if "records_type=stand_hour" in active_prefixes:
        cte_defs.append(f"""
            sh AS (
                SELECT CAST(start_date AS DATE) as date, COUNT(DISTINCT start_date) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=stand_hour/**/*.parquet')
                WHERE value = 0.0
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("sh AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS INTEGER) as val WHERE 1=0)")

    if "records_type=physical_effort" in active_prefixes:
        cte_defs.append(f"""
            pe AS (
                SELECT CAST(start_date AS DATE) as date, AVG(value) as val
                FROM read_parquet('s3://{settings.bucket_silver}/{user_id}/records_type=physical_effort/**/*.parquet')
                GROUP BY 1
            )
        """)
    else:
        cte_defs.append("pe AS (SELECT CAST(NULL AS DATE) as date, CAST(NULL AS DOUBLE PRECISION) as val WHERE 1=0)")

    cte_block = ",\n".join(cte_defs)

    query = f"""
        WITH {cte_block}
        SELECT 
            coalesce(bm.date, fc.date, dl.date, ws.date, wt.date, et.date, sh.date, pe.date) as date,
            bm.val as body_mass_kg,
            fc.val as flights_climbed,
            dl.val as time_in_daylight_minutes,
            ws.val as avg_walking_speed_kmh,
            wt.val as avg_wrist_temperature_c,
            et.val as exercise_time_minutes,
            coalesce(sh.val, 0)::INTEGER as stand_hours,
            pe.val as avg_physical_effort
        FROM bm
        FULL OUTER JOIN fc ON bm.date = fc.date
        FULL OUTER JOIN dl ON coalesce(bm.date, fc.date) = dl.date
        FULL OUTER JOIN ws ON coalesce(bm.date, fc.date, dl.date) = ws.date
        FULL OUTER JOIN wt ON coalesce(bm.date, fc.date, dl.date, ws.date) = wt.date
        FULL OUTER JOIN et ON coalesce(bm.date, fc.date, dl.date, ws.date, wt.date) = et.date
        FULL OUTER JOIN sh ON coalesce(bm.date, fc.date, dl.date, ws.date, wt.date, et.date) = sh.date
        FULL OUTER JOIN pe ON coalesce(bm.date, fc.date, dl.date, ws.date, wt.date, et.date, sh.date) = pe.date
        WHERE coalesce(bm.date, fc.date, dl.date, ws.date, wt.date, et.date, sh.date, pe.date) IS NOT NULL
        ORDER BY 1
    """
    res = duck_conn.execute(query).arrow().read_all().to_pylist()

    if not res:
        logger.warning(f"No metrics data yielded from query for user {user_id}.")
        return

    pg_conn = get_db_connection()
    try:
        with pg_conn.cursor() as cur:
            insert_query = f"""
                INSERT INTO {settings.db_schema}.daily_metrics_summary 
                (user_id, date, body_mass_kg, flights_climbed, time_in_daylight_minutes, avg_walking_speed_kmh, avg_wrist_temperature_c, exercise_time_minutes, stand_hours, avg_physical_effort)
                VALUES %s
                ON CONFLICT (user_id, date) DO UPDATE SET
                    body_mass_kg = EXCLUDED.body_mass_kg,
                    flights_climbed = EXCLUDED.flights_climbed,
                    time_in_daylight_minutes = EXCLUDED.time_in_daylight_minutes,
                    avg_walking_speed_kmh = EXCLUDED.avg_walking_speed_kmh,
                    avg_wrist_temperature_c = EXCLUDED.avg_wrist_temperature_c,
                    exercise_time_minutes = EXCLUDED.exercise_time_minutes,
                    stand_hours = EXCLUDED.stand_hours,
                    avg_physical_effort = EXCLUDED.avg_physical_effort,
                    updated_at = CURRENT_TIMESTAMP
            """
            rows = [
                (
                    user_id,
                    r["date"],
                    r["body_mass_kg"],
                    r["flights_climbed"],
                    r["time_in_daylight_minutes"],
                    r["avg_walking_speed_kmh"],
                    r["avg_wrist_temperature_c"],
                    r["exercise_time_minutes"],
                    r["stand_hours"],
                    r["avg_physical_effort"]
                )
                for r in res
            ]
            execute_values(cur, insert_query, rows)
            pg_conn.commit()
            logger.info(f"Upserted {len(rows)} records into Postgres table 'daily_metrics_summary' for user {user_id}.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to upsert physical metrics summaries for user {user_id}: {e}")
        raise e
    finally:
        pg_conn.close()


def process_gold(user_id: str) -> None:
    init_db_schema()

    duck_conn = duckdb.connect()
    setup_duckdb_s3(duck_conn)

    aggregate_heart_rate(duck_conn, user_id)
    aggregate_activity(duck_conn, user_id)
    aggregate_workouts(duck_conn, user_id)
    aggregate_sleep(duck_conn, user_id)
    aggregate_cardiovascular(duck_conn, user_id)
    aggregate_respiratory(duck_conn, user_id)
    aggregate_metrics(duck_conn, user_id)

    logger.info(f"Gold layer loading complete for user {user_id}.")
