







import sys
import duckdb
from pathlib import Path


project_root = str(Path.cwd().resolve())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.config import settings
from src.utils.minio_client import get_minio_client


conn = duckdb.connect()


conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")


conn.execute(f"SET s3_endpoint='{settings.minio_endpoint}';")
conn.execute(f"SET s3_access_key_id='{settings.minio_access_key}';")
conn.execute(f"SET s3_secret_access_key='{settings.minio_secret_key}';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")

print("✅ DuckDB configurado com sucesso para consultar o MinIO!")






client = get_minio_client()
objects = client.list_objects(settings.bucket_silver, recursive=False)

print("📂 Partições estruturadas prontas para consulta na Silver:")
for obj in objects:
    if obj.is_dir:
        dir_name = obj.object_name.strip("/")
        print(f"  - {dir_name}")








query_hr = f"""
    SELECT creation_date, value, unit, source_name 
    FROM read_parquet('s3://{settings.bucket_silver}/records_type=heart_rate/**/*.parquet') 
    LIMIT 10
"""
conn.execute(query_hr).df()





query_oxygen = f"""
    SELECT 
        CAST(start_date AS DATE) as data,
        ROUND(AVG(value) * 100, 2) as avg_oxygen_percentage,
        COUNT(*) as leituras
    FROM read_parquet('s3://{settings.bucket_silver}/records_type=blood_oxygen/**/*.parquet')
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 10
"""
conn.execute(query_oxygen).df()





query_resp = f"""
    SELECT 
        MIN(value) as min_respiratory_rate,
        MAX(value) as max_respiratory_rate,
        ROUND(AVG(value), 1) as avg_respiratory_rate
    FROM read_parquet('s3://{settings.bucket_silver}/records_type=respiratory_rate/**/*.parquet')
"""
conn.execute(query_resp).df()





query_workouts = f"""
    SELECT 
        workout_type, 
        COUNT(*) as total_treinos,
        ROUND(AVG(duration), 1) as duracao_media_min,
        ROUND(SUM(total_energy_burned), 0) as calorias_totais_burned_kcal
    FROM read_parquet('s3://{settings.bucket_silver}/workouts/**/*.parquet')
    GROUP BY 1
    ORDER BY 2 DESC
"""
conn.execute(query_workouts).df()






query_custom = f"""
    SELECT 
        type,
        COUNT(*) as total_records
    FROM read_parquet('s3://{settings.bucket_silver}/records_type=*/**/*.parquet')
    GROUP BY 1
    ORDER BY 2 DESC
"""
conn.execute(query_custom).df()
