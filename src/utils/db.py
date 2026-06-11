import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
from src.config import settings


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise e


def init_db_schema() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.db_schema};")
            logger.info(f"Schema '{settings.db_schema}' ensured.")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.users (
                    username VARCHAR(50) PRIMARY KEY,
                    password_hash VARCHAR(256) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info("Users table schema ensured.")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_heart_rate_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    date DATE,
                    avg_heart_rate DOUBLE PRECISION,
                    min_heart_rate DOUBLE PRECISION,
                    max_heart_rate DOUBLE PRECISION,
                    records_count INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, date)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_activity_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    date DATE,
                    step_count DOUBLE PRECISION,
                    active_energy_kcal DOUBLE PRECISION,
                    basal_energy_kcal DOUBLE PRECISION,
                    distance_km DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, date)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.workouts_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    workout_type VARCHAR(100),
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    duration_minutes DOUBLE PRECISION,
                    total_energy_kcal DOUBLE PRECISION,
                    total_distance_km DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, workout_type, start_date)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_sleep_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    date DATE,
                    total_sleep_minutes DOUBLE PRECISION,
                    asleep_minutes DOUBLE PRECISION,
                    deep_sleep_minutes DOUBLE PRECISION,
                    light_sleep_minutes DOUBLE PRECISION,
                    rem_sleep_minutes DOUBLE PRECISION,
                    awake_minutes DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, date)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_cardiovascular_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    date DATE,
                    avg_resting_heart_rate DOUBLE PRECISION,
                    avg_hrv_sdnn DOUBLE PRECISION,
                    avg_vo2_max DOUBLE PRECISION,
                    avg_blood_oxygen DOUBLE PRECISION,
                    min_blood_oxygen DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, date)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_respiratory_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    date DATE,
                    avg_respiratory_rate DOUBLE PRECISION,
                    min_respiratory_rate DOUBLE PRECISION,
                    max_respiratory_rate DOUBLE PRECISION,
                    avg_breathing_disturbances DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, date)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_metrics_summary (
                    user_id VARCHAR(50) REFERENCES {settings.db_schema}.users(username) ON DELETE CASCADE,
                    date DATE,
                    body_mass_kg DOUBLE PRECISION,
                    flights_climbed DOUBLE PRECISION,
                    time_in_daylight_minutes DOUBLE PRECISION,
                    avg_walking_speed_kmh DOUBLE PRECISION,
                    avg_wrist_temperature_c DOUBLE PRECISION,
                    exercise_time_minutes DOUBLE PRECISION,
                    stand_hours INTEGER,
                    avg_physical_effort DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, date)
                );
            """)

            conn.commit()
            logger.info("Postgres database schema initialized successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error initializing database schema: {e}")
        raise e
    finally:
        conn.close()
