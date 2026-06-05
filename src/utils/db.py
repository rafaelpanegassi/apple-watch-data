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
            # Create schema
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.db_schema};")
            logger.info(f"Schema '{settings.db_schema}' ensured.")

            # Create Daily Heart Rate Summary table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_heart_rate_summary (
                    date DATE PRIMARY KEY,
                    avg_heart_rate DOUBLE PRECISION,
                    min_heart_rate DOUBLE PRECISION,
                    max_heart_rate DOUBLE PRECISION,
                    records_count INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create Daily Activity Summary table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.daily_activity_summary (
                    date DATE PRIMARY KEY,
                    step_count DOUBLE PRECISION,
                    active_energy_kcal DOUBLE PRECISION,
                    basal_energy_kcal DOUBLE PRECISION,
                    distance_km DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create Workouts Summary table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {settings.db_schema}.workouts_summary (
                    workout_type VARCHAR(100),
                    start_date TIMESTAMP,
                    end_date TIMESTAMP,
                    duration_minutes DOUBLE PRECISION,
                    total_energy_kcal DOUBLE PRECISION,
                    total_distance_km DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (workout_type, start_date)
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
