import os
import shutil
import zipfile
import uuid
from datetime import datetime
from pathlib import Path
import lxml.etree as etree
import pyarrow as pa
import pyarrow.parquet as pq
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


def write_parquet_batch(records: list, schema: pa.Schema, output_path: Path) -> None:
    """Writes a list of dicts to a Parquet file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, str(output_path))


def process_silver() -> None:
    """Processes Bronze ZIP into Silver Parquet files using constant memory."""
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = (project_root / "data").resolve()
    tmp_dir = data_dir / "tmp_processing"
    
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
        ("type", pa.string())
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
        ("end_date", pa.timestamp("s"))
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
                        if "Asleep" in raw_val:
                            val = 1.0
                        elif "Deep" in raw_val:
                            val = 2.0
                        elif "Light" in raw_val:
                            val = 3.0
                        elif "REM" in raw_val:
                            val = 4.0
                        elif "Awake" in raw_val:
                            val = 5.0
                        else:
                            val = 0.0 # InBed / Default
                    else:
                        val = float(raw_val)
                except ValueError:
                    val = 0.0

                record_row = {
                    "source_name": elem.get("sourceName", ""),
                    "source_version": elem.get("sourceVersion", ""),
                    "device": elem.get("device", ""),
                    "unit": elem.get("unit", ""),
                    "creation_date": parse_date(elem.get("creationDate")),
                    "start_date": parse_date(elem.get("startDate")),
                    "end_date": parse_date(elem.get("endDate")),
                    "value": val,
                    "type": mapped_type
                }
                buffers[mapped_type].append(record_row)
                record_count += 1
                
                # Flush batch to local Parquet if buffer limit reached
                if len(buffers[mapped_type]) >= batch_size:
                    batch_id = str(uuid.uuid4())[:8]
                    local_path = tmp_dir / "parquet" / f"records_type={mapped_type}" / f"part_{batch_id}.parquet"
                    write_parquet_batch(buffers[mapped_type], record_schema, local_path)
                    local_files_to_upload.append(local_path)
                    buffers[mapped_type] = []

        elif elem.tag == "Workout":
            workout_row = {
                "workout_type": elem.get("workoutActivityType", ""),
                "duration": float(elem.get("duration", 0)),
                "duration_unit": elem.get("durationUnit", "min"),
                "total_distance": float(elem.get("totalDistance", 0)),
                "total_distance_unit": elem.get("totalDistanceUnit", "km"),
                "total_energy_burned": float(elem.get("totalEnergyBurned", 0)),
                "total_energy_burned_unit": elem.get("totalEnergyBurnedUnit", "kcal"),
                "source_name": elem.get("sourceName", ""),
                "source_version": elem.get("sourceVersion", ""),
                "start_date": parse_date(elem.get("startDate")),
                "end_date": parse_date(elem.get("endDate"))
            }
            workout_buffer.append(workout_row)
            workout_count += 1
            
            if len(workout_buffer) >= batch_size:
                batch_id = str(uuid.uuid4())[:8]
                local_path = tmp_dir / "parquet" / "workouts" / f"part_{batch_id}.parquet"
                write_parquet_batch(workout_buffer, workout_schema, local_path)
                local_files_to_upload.append(local_path)
                workout_buffer = []

        # Memory clean up: free processed XML elements
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    # Flush remaining buffers
    for mapped_type, buffer_rows in buffers.items():
        if buffer_rows:
            batch_id = str(uuid.uuid4())[:8]
            local_path = tmp_dir / "parquet" / f"records_type={mapped_type}" / f"part_{batch_id}.parquet"
            write_parquet_batch(buffer_rows, record_schema, local_path)
            local_files_to_upload.append(local_path)

    if workout_buffer:
        batch_id = str(uuid.uuid4())[:8]
        local_path = tmp_dir / "parquet" / "workouts" / f"part_{batch_id}.parquet"
        write_parquet_batch(workout_buffer, workout_schema, local_path)
        local_files_to_upload.append(local_path)

    logger.info(f"Parsing finished. Extracted {record_count} records and {workout_count} workouts.")

    # 4. Upload Silver Parquets to MinIO Silver Bucket
    ensure_bucket(settings.bucket_silver)
    minio_client = get_minio_client()

    logger.info("Clearing previous Silver Parquet files from MinIO...")
    try:
        prefixes = [f"records_type={t}/" for t in TYPE_MAPPING.values()] + ["workouts/"]
        for pfx in prefixes:
            objects = minio_client.list_objects(settings.bucket_silver, prefix=pfx, recursive=True)
            for obj in objects:
                minio_client.remove_object(settings.bucket_silver, obj.object_name)
    except Exception as e:
        logger.warning(f"Failed to clear old objects: {e}")

    logger.info("Uploading Parquet files to MinIO Silver...")
    parquet_base = tmp_dir / "parquet"
    for local_file in local_files_to_upload:
        if local_file.exists():
            rel_path = local_file.relative_to(parquet_base)
            object_name = str(rel_path)
            object_name = object_name.replace(os.sep, "/")
            minio_client.fput_object(settings.bucket_silver, object_name, str(local_file))

    logger.info(f"Silver processing complete! Uploaded tables to MinIO bucket '{settings.bucket_silver}'.")

    # Clean up local temporary files
    shutil.rmtree(tmp_dir)
    logger.info("Cleaned up processing directory.")


if __name__ == "__main__":
    process_silver()
