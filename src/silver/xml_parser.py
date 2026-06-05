import os
import shutil
import zipfile
import uuid
from datetime import datetime
from pathlib import Path
import lxml.etree as etree
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from loguru import logger

from src.config import settings
from src.utils.minio_client import download_file, get_minio_client, ensure_bucket

# Record type mapping to keep datasets clean
TYPE_MAPPING = {
    # Core activity & cardiovascular
    "HKQuantityTypeIdentifierHeartRate": "heart_rate",
    "HKQuantityTypeIdentifierStepCount": "step_count",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "distance",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "active_energy",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "basal_energy",
    "HKCategoryTypeIdentifierSleepAnalysis": "sleep",
    
    # New metrics from user request
    "HKQuantityTypeIdentifierRespiratoryRate": "respiratory_rate",
    "HKQuantityTypeIdentifierOxygenSaturation": "blood_oxygen",
    "HKQuantityTypeIdentifierRestingHeartRate": "resting_heart_rate",
    "HKQuantityTypeIdentifierAppleStandTime": "stand_time",
    "HKCategoryTypeIdentifierAppleStandHour": "stand_hour",
    "HKQuantityTypeIdentifierPhysicalEffort": "physical_effort",
    "HKQuantityTypeIdentifierAppleExerciseTime": "exercise_time",
    
    # Extra rich metrics available in user data
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "hrv",
    "HKQuantityTypeIdentifierBodyMass": "body_mass",
    "HKQuantityTypeIdentifierFlightsClimbed": "flights_climbed",
    "HKQuantityTypeIdentifierTimeInDaylight": "time_in_daylight",
    "HKQuantityTypeIdentifierWalkingSpeed": "walking_speed",
    "HKQuantityTypeIdentifierVO2Max": "vo2_max",
    "HKQuantityTypeIdentifierAppleSleepingWristTemperature": "wrist_temperature",
    "HKQuantityTypeIdentifierAppleSleepingBreathingDisturbances": "breathing_disturbances"
}

# Date parser helper
def parse_date(date_str: str) -> datetime:
    """Parses date string like '2026-06-04 10:00:00 -0300' into a datetime object."""
    try:
        parts = date_str.split(" ")
        if len(parts) >= 2:
            dt_part = f"{parts[0]} {parts[1]}"
            return datetime.strptime(dt_part, "%Y-%m-%d %H:%M:%S")
        return datetime.fromisoformat(date_str)
    except Exception:
        return datetime.now()


def safe_extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extracts zip archive while preventing Zip Slip (directory traversal)."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    resolved_extract_dir = extract_dir.resolve()

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            # Resolve target path and verify boundary
            target_path = Path(resolved_extract_dir / member.filename).resolve()
            if not str(target_path).startswith(str(resolved_extract_dir) + os.sep) and str(target_path) != str(resolved_extract_dir):
                raise ValueError(f"Security Exception: Path traversal attempt in ZIP: {member.filename}")
            
            # Extract member
            zip_ref.extract(member, resolved_extract_dir)


def write_partitioned_batch(records: list, schema: pa.Schema, root_path: Path) -> None:
    """Writes a list of dicts to a partitioned Parquet dataset locally."""
    root_path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_to_dataset(table, root_path=str(root_path), partition_cols=["date"])


def process_silver() -> None:
    """Processes Bronze ZIP into Silver Parquet files using constant memory."""
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = (project_root / "data").resolve()
    tmp_dir = data_dir / "tmp_processing"
    current_time = datetime.utcnow()
    
    # Clean up previous temp files if they exist
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # 1. Download from MinIO Bronze
    local_zip = tmp_dir / "apple_health_raw.zip"
    logger.info("Downloading raw export ZIP from MinIO Bronze...")
    download_file(settings.bucket_bronze, "apple_health_raw.zip", str(local_zip))

    # 2. Extract ZIP safely
    extract_dir = tmp_dir / "extracted"
    logger.info("Extracting ZIP safely...")
    safe_extract_zip(local_zip, extract_dir)

    # Find export.xml
    xml_path = None
    for p in extract_dir.glob("**/export.xml"):
        xml_path = p
        break

    if not xml_path:
        raise FileNotFoundError("Could not find 'export.xml' in the extracted zip.")
    
    logger.info(f"Parsing XML file: {xml_path}")

    # Initialize PyArrow Schemas
    record_schema = pa.schema([
        ("source_name", pa.string()),
        ("source_version", pa.string()),
        ("device", pa.string()),
        ("unit", pa.string()),
        ("creation_date", pa.timestamp("s")),
        ("start_date", pa.timestamp("s")),
        ("end_date", pa.timestamp("s")),
        ("value", pa.float64()),
        ("type", pa.string()),
        ("ingested_at", pa.timestamp("s")),
        ("file_source", pa.string()),
        ("date", pa.string())
    ])

    workout_schema = pa.schema([
        ("workout_type", pa.string()),
        ("duration", pa.float64()),
        ("duration_unit", pa.string()),
        ("total_distance", pa.float64()),
        ("total_distance_unit", pa.string()),
        ("total_energy_burned", pa.float64()),
        ("total_energy_burned_unit", pa.string()),
        ("source_name", pa.string()),
        ("source_version", pa.string()),
        ("start_date", pa.timestamp("s")),
        ("end_date", pa.timestamp("s")),
        ("ingested_at", pa.timestamp("s")),
        ("file_source", pa.string()),
        ("date", pa.string())
    ])

    # Buffers to prevent writing too many tiny files
    buffers = {k: [] for k in TYPE_MAPPING.values()}
    workout_buffer = []
    
    # Storage maps for output local files to be uploaded
    local_files_to_upload = []
    
    # 3. Stream Parse XML securely (hardened XML Parser settings to prevent XXE)
    context = etree.iterparse(
        str(xml_path),
        events=("end",),
        tag=("Record", "Workout"),
        resolve_entities=False,  # Disable entity expansion
        no_network=True,         # Disable network requests
        load_dtd=False           # Disable external DTD loading
    )

    record_count = 0
    workout_count = 0
    batch_size = 20000

    logger.info("Streaming through Apple Watch export XML...")
    for event, elem in context:
        if elem.tag == "Record":
            raw_type = elem.get("type")
            mapped_type = TYPE_MAPPING.get(raw_type)
            
            if mapped_type:
                # Cast value to float
                raw_val = elem.get("value", "0")
                try:
                    if mapped_type == "sleep":
                        if "Deep" in raw_val:
                            val = 2.0
                        elif "Core" in raw_val or "Light" in raw_val:
                            val = 3.0
                        elif "REM" in raw_val:
                            val = 4.0
                        elif "Awake" in raw_val:
                            val = 5.0
                        elif "Asleep" in raw_val:
                            val = 1.0
                        else:
                            val = 0.0 # InBed / Default
                    else:
                        val = float(raw_val)
                except ValueError:
                    val = 0.0

                start_dt = parse_date(elem.get("startDate"))
                record_row = {
                    "source_name": elem.get("sourceName", ""),
                    "source_version": elem.get("sourceVersion", ""),
                    "device": elem.get("device", ""),
                    "unit": elem.get("unit", ""),
                    "creation_date": parse_date(elem.get("creationDate")),
                    "start_date": start_dt,
                    "end_date": parse_date(elem.get("endDate")),
                    "value": val,
                    "type": mapped_type,
                    "ingested_at": current_time,
                    "file_source": "apple_health_raw.zip",
                    "date": start_dt.strftime("%Y-%m-%d")
                }
                buffers[mapped_type].append(record_row)
                record_count += 1
                
                # Flush batch to local Parquet if buffer limit reached
                if len(buffers[mapped_type]) >= batch_size:
                    local_root = tmp_dir / "parquet" / f"records_type={mapped_type}"
                    write_partitioned_batch(buffers[mapped_type], record_schema, local_root)
                    buffers[mapped_type] = []

        elif elem.tag == "Workout":
            start_dt = parse_date(elem.get("startDate"))
            
            # Base values from attributes (if present)
            duration = float(elem.get("duration", 0) or 0)
            duration_unit = elem.get("durationUnit", "min")
            total_distance = float(elem.get("totalDistance", 0) or 0)
            total_distance_unit = elem.get("totalDistanceUnit", "km")
            total_energy_burned = float(elem.get("totalEnergyBurned", 0) or 0)
            total_energy_burned_unit = elem.get("totalEnergyBurnedUnit", "kcal")
            
            # Extract statistics from child WorkoutStatistics elements if present
            for stats in elem.findall("WorkoutStatistics"):
                stats_type = stats.get("type")
                stats_sum = stats.get("sum")
                if stats_sum is not None:
                    try:
                        val = float(stats_sum)
                    except ValueError:
                        val = 0.0
                    
                    if stats_type == "HKQuantityTypeIdentifierActiveEnergyBurned":
                        total_energy_burned = val
                        total_energy_burned_unit = stats.get("unit", "kcal")
                    elif stats_type and "Distance" in stats_type:
                        total_distance = val
                        total_distance_unit = stats.get("unit", "km")
            
            workout_row = {
                "workout_type": elem.get("workoutActivityType", ""),
                "duration": duration,
                "duration_unit": duration_unit,
                "total_distance": total_distance,
                "total_distance_unit": total_distance_unit,
                "total_energy_burned": total_energy_burned,
                "total_energy_burned_unit": total_energy_burned_unit,
                "source_name": elem.get("sourceName", ""),
                "source_version": elem.get("sourceVersion", ""),
                "start_date": start_dt,
                "end_date": parse_date(elem.get("endDate")),
                "ingested_at": current_time,
                "file_source": "apple_health_raw.zip",
                "date": start_dt.strftime("%Y-%m-%d")
            }
            workout_buffer.append(workout_row)
            workout_count += 1
            
            if len(workout_buffer) >= batch_size:
                local_root = tmp_dir / "parquet" / "workouts"
                write_partitioned_batch(workout_buffer, workout_schema, local_root)
                workout_buffer = []

        # Memory clean up: free processed XML elements
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    # Flush remaining buffers
    for mapped_type, buffer_rows in buffers.items():
        if buffer_rows:
            local_root = tmp_dir / "parquet" / f"records_type={mapped_type}"
            write_partitioned_batch(buffer_rows, record_schema, local_root)

    if workout_buffer:
        local_root = tmp_dir / "parquet" / "workouts"
        write_partitioned_batch(workout_buffer, workout_schema, local_root)

    logger.info(f"Parsing finished. Extracted {record_count} records and {workout_count} workouts.")

    # 4. Merge, Deduplicate and Upload to MinIO Silver Bucket
    ensure_bucket(settings.bucket_silver)
    minio_client = get_minio_client()

    logger.info("Merging and deduplicating partitions with existing MinIO data...")
    parquet_base = tmp_dir / "parquet"
    
    if parquet_base.exists():
        for type_dir in parquet_base.iterdir():
            if not type_dir.is_dir():
                continue
            
            mapped_type = type_dir.name
            
            for date_dir in type_dir.iterdir():
                if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                    continue
                
                date_str = date_dir.name.split("=")[1]
                
                # Check for existing data in MinIO for this partition
                minio_prefix = f"{mapped_type}/date={date_str}/"
                try:
                    objects = list(minio_client.list_objects(settings.bucket_silver, prefix=minio_prefix, recursive=True))
                except Exception as e:
                    logger.warning(f"Failed to list objects for {minio_prefix}: {e}")
                    objects = []

                # Read new data
                new_df = pq.read_table(str(date_dir)).to_pandas()
                if "date" not in new_df.columns:
                    new_df["date"] = date_str
                else:
                    new_df["date"] = new_df["date"].astype(str)

                if objects:
                    # Download existing files and merge (flat folder to avoid partition type inference merge bugs)
                    down_dir = tmp_dir / "download" / mapped_type
                    down_dir.mkdir(parents=True, exist_ok=True)
                    
                    existing_dfs = []
                    for i, obj in enumerate(objects):
                        local_down_path = down_dir / f"{date_str}_existing_{i}.parquet"
                        try:
                            minio_client.fget_object(settings.bucket_silver, obj.object_name, str(local_down_path))
                            df_part = pq.read_table(str(local_down_path)).to_pandas()
                            if "date" not in df_part.columns:
                                df_part["date"] = date_str
                            else:
                                df_part["date"] = df_part["date"].astype(str)
                            existing_dfs.append(df_part)
                        except Exception as e:
                            logger.error(f"Failed to process existing object {obj.object_name}: {e}")

                    if existing_dfs:
                        existing_df = pd.concat(existing_dfs, ignore_index=True)
                        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        merged_df = new_df
                else:
                    merged_df = new_df

                # Sort by ingested_at ascending to keep the earliest record during drop_duplicates
                if "ingested_at" in merged_df.columns:
                    merged_df = merged_df.sort_values("ingested_at", ascending=True)

                # Deduplicate
                if mapped_type == "workouts":
                    subset = ["workout_type", "source_name", "start_date", "end_date"]
                    schema = workout_schema
                else:
                    subset = ["type", "source_name", "start_date", "end_date", "value"]
                    schema = record_schema

                # Drop duplicates keeping the first one (oldest ingested_at)
                merged_df = merged_df.drop_duplicates(subset=subset, keep="first")

                # Write consolidated file locally
                final_partition_dir = tmp_dir / "final" / mapped_type / f"date={date_str}"
                final_partition_dir.mkdir(parents=True, exist_ok=True)
                final_local_path = final_partition_dir / "part_0.parquet"
                
                try:
                    # Ensure datetime columns have correct pandas datetime representation matching timestamp("s")
                    for col in ["creation_date", "start_date", "end_date", "ingested_at"]:
                        if col in merged_df.columns:
                            merged_df[col] = pd.to_datetime(merged_df[col]).dt.round('s')
                    
                    # Ensure string columns are standard strings (not category)
                    str_cols = ["source_name", "source_version", "device", "unit", "type", "file_source", "date"] if mapped_type != "workouts" else ["workout_type", "source_name", "source_version", "file_source", "date"]
                    for col in str_cols:
                        if col in merged_df.columns:
                            merged_df[col] = merged_df[col].astype(str)
                    
                    # Convert to Arrow Table using the schema
                    table = pa.Table.from_pandas(merged_df, schema=schema, preserve_index=False)
                    pq.write_table(table, str(final_local_path))
                except Exception as e:
                    logger.error(f"Failed to write merged table for {mapped_type}/{date_str}: {e}")
                    pq.write_table(pa.Table.from_pandas(merged_df, preserve_index=False), str(final_local_path))

                # Delete existing files in MinIO partition
                for obj in objects:
                    try:
                        minio_client.remove_object(settings.bucket_silver, obj.object_name)
                    except Exception as e:
                        logger.error(f"Failed to delete {obj.object_name}: {e}")

                # Upload to MinIO
                target_object_name = f"{mapped_type}/date={date_str}/part_0.parquet"
                try:
                    minio_client.fput_object(settings.bucket_silver, target_object_name, str(final_local_path))
                except Exception as e:
                    logger.error(f"Failed to upload to {target_object_name}: {e}")
                    raise e

    logger.info(f"Silver processing complete! Uploaded tables to MinIO bucket '{settings.bucket_silver}' with date partitioning and metadata columns.")

    # Clean up local temporary files
    shutil.rmtree(tmp_dir)
    logger.info("Cleaned up processing directory.")


if __name__ == "__main__":
    process_silver()
