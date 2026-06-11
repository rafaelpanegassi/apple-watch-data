# Data Dictionary: Apple Watch Medallion Pipeline

This document describes the structure, columns, and data types present in the **Silver** (MinIO Parquet) and **Gold** (PostgreSQL) layers of our pipeline.

---

## 🥈 Silver Layer (MinIO Parquet)

Data in the Silver layer is stored in MinIO buckets in Parquet format, partitioned by metric type.

### 1. Health Records Table (`records_type=*`)
Applies to all record partitions (e.g., `heart_rate`, `step_count`, `distance`, `active_energy`, `basal_energy`, `sleep`, `respiratory_rate`, `blood_oxygen`, `resting_heart_rate`, `stand_time`, `stand_hour`, `physical_effort`, `exercise_time`, `hrv`, `body_mass`, `flights_climbed`, `time_in_daylight`, `walking_speed`, `vo2_max`, `wrist_temperature`, `breathing_disturbances`).

| Column | Type | Description |
| :--- | :--- | :--- |
| `source_name` | String | Name of the source device (e.g., "Rafael's iPhone", "Rafael's Apple Watch"). |
| `source_version` | String | Operating system version of the source device (e.g., "17.4", "10.4"). |
| `device` | String | Detailed hardware and software device information. |
| `unit` | String | Unit of measurement associated with the value (e.g., `count/min`, `count`, `km`, `kcal`, `hr`). |
| `creation_date` | Timestamp | Date and time the record was created locally on the device. |
| `start_date` | Timestamp | Starting date and time of the measurement interval. |
| `end_date` | Timestamp | Ending date and time of the measurement interval. |
| `value` | Float64 | Measured numeric value. |
| `type` | String | Simplified partition identifier (e.g., `heart_rate`, `step_count`, `sleep`). |

*Note on the `sleep` partition: The `value` field is mapped as: 0.0 (InBed), 1.0 (Asleep/Core), 2.0 (Deep), 3.0 (Light), 4.0 (REM), 5.0 (Awake).*

---

### 2. Workouts Table (`workouts`)
Contains the list of workout sessions recorded by the watch.

| Column | Type | Description |
| :--- | :--- | :--- |
| `workout_type` | String | Sport modality of the workout (e.g., `HKWorkoutActivityTypeRunning`, `HKWorkoutActivityTypeWalking`). |
| `duration` | Float64 | Total duration of the workout in the corresponding unit. |
| `duration_unit` | String | Duration unit (typically `min`). |
| `total_distance` | Float64 | Total distance covered during the workout. |
| `total_distance_unit` | String | Distance unit of measurement (e.g., `km`, `m`). |
| `total_energy_burned` | Float64 | Total active calories burned during the session. |
| `total_energy_burned_unit`| String | Energy unit (typically `kcal`). |
| `source_name` | String | Device that recorded the workout (e.g., "Apple Watch"). |
| `source_version` | String | Software version that recorded the workout. |
| `start_date` | Timestamp | Start date and time of the workout. |
| `end_date` | Timestamp | End date and time of the workout. |

---

## 🥇 Gold Layer (PostgreSQL)

The final aggregated summaries that feed the dashboards are structured in relational tables inside the database.

### 1. Table `gold.daily_heart_rate_summary`
Stores the daily heart rate consolidation.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date` | Date | Date of the measurements (Primary Key). |
| `avg_heart_rate` | Float64 | Calculated average heart rate for the day (bpm). |
| `min_heart_rate` | Float64 | Minimum heart rate recorded in the day (bpm). |
| `max_heart_rate` | Float64 | Maximum heart rate recorded in the day (bpm). |
| `records_count` | Integer | Total number of heart rate samples collected in the day. |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |

---

### 2. Table `gold.daily_activity_summary`
Consolidates daily step counts, active energy, basal energy, and walking distances.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date` | Date | Date of the measurements (Primary Key). |
| `step_count` | Float64 | Accumulated step count for the day. |
| `active_energy_kcal` | Float64 | Total active calories burned in the day (kcal). |
| `basal_energy_kcal` | Float64 | Daily basal metabolic energy consumption estimate (kcal). |
| `distance_km` | Float64 | Total walking or running distance covered in the day (km). |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |

---

### 3. Table `gold.workouts_summary`
Structured list of workout sessions.

| Column | Type | Description |
| :--- | :--- | :--- |
| `workout_type` | String | Type of workout exercise (Compound Primary Key). |
| `start_date` | Timestamp | Start date and time of the exercise (Compound Primary Key). |
| `end_date` | Timestamp | End date and time of the exercise. |
| `duration_minutes` | Float64 | Net workout duration in minutes. |
| `total_energy_kcal` | Float64 | Active energy burned during the workout (kcal). |
| `total_distance_km` | Float64 | Distance covered during the workout (km). |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |

---

### 4. Table `gold.daily_sleep_summary`
Consolidates daily sleep stages duration in minutes.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date` | Date | Date of the measurements (Primary Key). |
| `total_sleep_minutes` | Float64 | Sum of Deep, Light, REM, and Asleep minutes. |
| `asleep_minutes` | Float64 | Unspecified general asleep time (fallback). |
| `deep_sleep_minutes` | Float64 | Duration in deep sleep stage. |
| `light_sleep_minutes` | Float64 | Duration in light sleep stage (watchOS Core). |
| `rem_sleep_minutes` | Float64 | Duration in REM sleep stage. |
| `awake_minutes` | Float64 | Duration of wakefulness during the sleep cycle. |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |

---

### 5. Table `gold.daily_cardiovascular_summary`
Consolidates daily cardiovascular indicators.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date` | Date | Date of the measurements (Primary Key). |
| `avg_resting_heart_rate` | Float64 | Daily average resting heart rate (bpm). |
| `avg_hrv_sdnn` | Float64 | Daily average Heart Rate Variability (SDNN, ms). |
| `avg_vo2_max` | Float64 | Daily average VO2 Max capacity score. |
| `avg_blood_oxygen` | Float64 | Daily average blood oxygen saturation level (%). |
| `min_blood_oxygen` | Float64 | Daily minimum blood oxygen saturation level (%). |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |

---

### 6. Table `gold.daily_respiratory_summary`
Consolidates daily breathing metrics.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date` | Date | Date of the measurements (Primary Key). |
| `avg_respiratory_rate` | Float64 | Daily average breaths per minute (br/min). |
| `min_respiratory_rate` | Float64 | Daily minimum breaths per minute. |
| `max_respiratory_rate` | Float64 | Daily maximum breaths per minute. |
| `avg_breathing_disturbances` | Float64 | Daily average sleep breathing disturbances score. |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |

---

### 7. Table `gold.daily_metrics_summary`
Tracks various physical measurements, daylight exposure, and other metrics.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date` | Date | Date of the measurements (Primary Key). |
| `body_mass_kg` | Float64 | Average body mass measurement (kg). |
| `flights_climbed` | Float64 | Total flights of stairs climbed. |
| `time_in_daylight_minutes` | Float64 | Total time exposed to daylight (minutes). |
| `avg_walking_speed_kmh` | Float64 | Average walking speed (km/h). |
| `avg_wrist_temperature_c` | Float64 | Average sleeping wrist temperature deviation (°C). |
| `exercise_time_minutes` | Float64 | Total exercise active minutes. |
| `stand_hours` | Integer | Total count of stand hours met during the day. |
| `avg_physical_effort` | Float64 | Average calculated physical effort score. |
| `updated_at` | Timestamp | Timestamp of the last data load/update. |
