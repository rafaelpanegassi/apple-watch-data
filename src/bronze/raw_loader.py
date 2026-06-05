import os
import zipfile
import random
from pathlib import Path
from datetime import datetime, timedelta
from loguru import logger
from src.config import settings
from src.utils.minio_client import upload_file, ensure_bucket


def generate_mock_xml_string() -> str:
    """Generates a 60-day historical Apple Watch XML string programmatically."""
    logger.debug("Building mock XML dataset spanning 60 days...")
    xml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<HealthData locale="en_US">',
        '  <ExportDate value="2026-06-04 12:00:00 -0300"/>',
        '  <Me dateOfBirth="1990-01-01" biologicalSex="HKBiologicalSexMale" bloodType="HKBloodTypeNotSet" wheelChairUse="HKWheelchairUseNo"/>'
    ]

    base_date = datetime.strptime("2026-06-04", "%Y-%m-%d")
    random.seed(42)  # Deterministic mock generation

    for i in range(60, -1, -1):
        current_date = base_date - timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        
        # 1. Heart Rate readings (12 records per day)
        for hour in range(8, 20):
            hr_val = random.randint(62, 98)
            time_str = f"{date_str} {hour:02d}:00:00 -0300"
            xml_lines.append(
                f'  <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Rafael\'s Apple Watch" '
                f'sourceVersion="10.4" unit="count/min" creationDate="{time_str}" startDate="{time_str}" '
                f'endDate="{time_str}" value="{hr_val}"/>'
            )
            
        # 2. Step Count (2 entries per day)
        steps_1 = random.randint(3000, 6000)
        steps_2 = random.randint(2000, 5000)
        time_1_start = f"{date_str} 10:00:00 -0300"
        time_1_end = f"{date_str} 10:30:00 -0300"
        time_2_start = f"{date_str} 16:00:00 -0300"
        time_2_end = f"{date_str} 16:30:00 -0300"
        
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="Rafael\'s iPhone" '
            f'sourceVersion="17.4" unit="count" creationDate="{time_1_end}" startDate="{time_1_start}" '
            f'endDate="{time_1_end}" value="{steps_1}"/>'
        )
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="Rafael\'s iPhone" '
            f'sourceVersion="17.4" unit="count" creationDate="{time_2_end}" startDate="{time_2_start}" '
            f'endDate="{time_2_end}" value="{steps_2}"/>'
        )
        
        # 3. Distance (matching steps, approx 0.00075 km per step)
        dist_1 = round(steps_1 * 0.00075, 2)
        dist_2 = round(steps_2 * 0.00075, 2)
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierDistanceWalkingRunning" sourceName="Rafael\'s iPhone" '
            f'sourceVersion="17.4" unit="km" creationDate="{time_1_end}" startDate="{time_1_start}" '
            f'endDate="{time_1_end}" value="{dist_1}"/>'
        )
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierDistanceWalkingRunning" sourceName="Rafael\'s iPhone" '
            f'sourceVersion="17.4" unit="km" creationDate="{time_2_end}" startDate="{time_2_start}" '
            f'endDate="{time_2_end}" value="{dist_2}"/>'
        )
        
        # 4. Energy Burned
        # Active energy (approx 0.04 kcal per step)
        act_eng_1 = round(steps_1 * 0.04, 1)
        act_eng_2 = round(steps_2 * 0.04, 1)
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierActiveEnergyBurned" sourceName="Rafael\'s Apple Watch" '
            f'sourceVersion="10.4" unit="kcal" creationDate="{time_1_end}" startDate="{time_1_start}" '
            f'endDate="{time_1_end}" value="{act_eng_1}"/>'
        )
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierActiveEnergyBurned" sourceName="Rafael\'s Apple Watch" '
            f'sourceVersion="10.4" unit="kcal" creationDate="{time_2_end}" startDate="{time_2_start}" '
            f'endDate="{time_2_end}" value="{act_eng_2}"/>'
        )
        
        # Basal energy (approx 1600 kcal per day)
        basal_val = random.randint(1550, 1750)
        xml_lines.append(
            f'  <Record type="HKQuantityTypeIdentifierBasalEnergyBurned" sourceName="Rafael\'s Apple Watch" '
            f'sourceVersion="10.4" unit="kcal" creationDate="{date_str} 23:59:59 -0300" '
            f'startDate="{date_str} 00:00:00 -0300" endDate="{date_str} 23:59:59 -0300" value="{basal_val}"/>'
        )
        
        # 5. Workouts
        # Running every 3 days
        if i % 3 == 0:
            duration = random.randint(25, 50)
            distance = round(duration * 0.15 + random.uniform(-0.5, 0.5), 1)
            calories = random.randint(duration * 8, duration * 12)
            w_start = f"{date_str} 07:00:00 -0300"
            # Calculate end date offset
            w_end = (current_date + timedelta(minutes=duration)).strftime("%Y-%m-%d %H:%M:%S -0300")
            xml_lines.append(
                f'  <Workout workoutActivityType="HKWorkoutActivityTypeRunning" duration="{duration}" '
                f'durationUnit="min" totalDistance="{distance}" totalDistanceUnit="km" '
                f'totalEnergyBurned="{calories}" totalEnergyBurnedUnit="kcal" sourceName="Rafael\'s Apple Watch" '
                f'sourceVersion="10.4" startDate="{w_start}" endDate="{w_end}"/>'
            )
        
        # Walking every 2 days
        if i % 2 == 0:
            duration = random.randint(30, 60)
            distance = round(duration * 0.07 + random.uniform(-0.2, 0.2), 1)
            calories = random.randint(duration * 3, duration * 5)
            w_start = f"{date_str} 18:00:00 -0300"
            w_end = (current_date + timedelta(hours=18, minutes=duration)).strftime("%Y-%m-%d %H:%M:%S -0300")
            xml_lines.append(
                f'  <Workout workoutActivityType="HKWorkoutActivityTypeWalking" duration="{duration}" '
                f'durationUnit="min" totalDistance="{distance}" totalDistanceUnit="km" '
                f'totalEnergyBurned="{calories}" totalEnergyBurnedUnit="kcal" sourceName="Rafael\'s Apple Watch" '
                f'sourceVersion="10.4" startDate="{w_start}" endDate="{w_end}"/>'
            )
            
    xml_lines.append('</HealthData>')
    return '\n'.join(xml_lines)


def generate_mock_zip(output_path: Path) -> None:
    """Generates a mock ZIP export structure containing 60-day historical health XML."""
    logger.info("Generating mock Apple Watch export data (60-day history)...")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Temporary directory for zip content
    temp_xml_path = output_path.parent / "export.xml"
    
    xml_content = generate_mock_xml_string()
    with open(temp_xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(temp_xml_path, arcname="apple_health_export/export.xml")

    # Clean up local unzipped xml
    if temp_xml_path.exists():
        temp_xml_path.unlink()
    logger.info(f"Mock export ZIP created successfully at '{output_path}'.")


def load_bronze() -> None:
    """Finds ZIP in data/ and uploads it to MinIO bronze bucket."""
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = (project_root / "data").resolve()
    
    data_dir.mkdir(parents=True, exist_ok=True)

    # Search for ZIPs in data/
    zip_files = list(data_dir.glob("*.zip"))
    
    if not zip_files:
        logger.warning("No user data zip found in 'data/' directory.")
        target_zip = data_dir / "export.zip"
        # Overwrite previous mock if it's there or create a new one
        generate_mock_zip(target_zip)
    else:
        target_zip = zip_files[0]
        # Strictly verify directory boundary to prevent traversal bypass
        resolved_zip_path = target_zip.resolve()
        if not str(resolved_zip_path).startswith(str(data_dir) + os.sep):
            raise ValueError(f"Security Warning: Attempted path traversal via target zip: {target_zip}")
        logger.info(f"Found active user ZIP export: {target_zip.name}")

    ensure_bucket(settings.bucket_bronze)
    
    # Upload raw file
    object_name = "apple_health_raw.zip"
    upload_file(settings.bucket_bronze, object_name, str(target_zip))
    logger.info(f"Bronze ingestion complete: {settings.bucket_bronze}/{object_name}")


if __name__ == "__main__":
    load_bronze()
