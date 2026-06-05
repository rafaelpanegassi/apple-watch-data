# Dicionário de Dados: Pipeline Apple Watch Medallion

Este documento descreve de forma sucinta e direta a estrutura de colunas e dados presentes nas camadas **Silver** (MinIO Parquet) e **Gold** (Postgres) do nosso pipeline.

---

## 🥈 Camada Silver (MinIO Parquet)

Os dados na camada Silver são armazenados em buckets MinIO formatados em Parquet, particionados por tipo de métrica.

### 1. Tabela de Registros de Saúde (`records_type=*`)
Aplica-se a todas as partições de registros (ex: `heart_rate`, `step_count`, `distance`, `active_energy`, `basal_energy`, `sleep`, `respiratory_rate`, `blood_oxygen`, `resting_heart_rate`, `stand_time`, `stand_hour`, `physical_effort`, `exercise_time`, `hrv`, `body_mass` etc.).

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `source_name` | String | Nome do dispositivo de origem do dado (ex: "iPhone de Rafael", "Apple Watch de Rafael"). |
| `source_version` | String | Versão do sistema operacional do dispositivo de origem (ex: "17.4", "10.4"). |
| `device` | String | Informações detalhadas do hardware e software do dispositivo (ex: modelo do relógio, fabricante). |
| `unit` | String | Unidade de medida associada ao valor (ex: `count/min`, `count`, `km`, `kcal`, `hr`). |
| `creation_date` | Timestamp | Data e hora em que o registro foi criado no banco local do dispositivo. |
| `start_date` | Timestamp | Data e hora de início do intervalo de medição do dado. |
| `end_date` | Timestamp | Data e hora de término do intervalo de medição do dado. |
| `value` | Float64 | Valor numérico medido para o registro específico. |
| `type` | String | Identificador simplificado da partição (ex: `heart_rate`, `step_count`, `sleep`). |

*Nota sobre a partição `sleep`: O campo `value` é classificado numericamente da seguinte forma: 0.0 (InBed), 1.0 (Asleep/Core), 2.0 (Deep), 3.0 (Light), 4.0 (REM), 5.0 (Awake).*

---

### 2. Tabela de Treinos (`workouts`)
Contém a lista de sessões de exercícios físicos registrados no relógio.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `workout_type` | String | Modalidade esportiva do treino (ex: `HKWorkoutActivityTypeRunning`, `HKWorkoutActivityTypeWalking`). |
| `duration` | Float64 | Duração total do treino na unidade correspondente. |
| `duration_unit` | String | Unidade da duração (geralmente `min`). |
| `total_distance` | Float64 | Distância total percorrida durante o treino. |
| `total_distance_unit` | String | Unidade de medida da distância (ex: `km`, `m`). |
| `total_energy_burned` | Float64 | Total de calorias ativas queimadas durante a sessão. |
| `total_energy_burned_unit`| String | Unidade de queima calórica (geralmente `kcal`). |
| `source_name` | String | Dispositivo que registrou o treino (ex: "Apple Watch"). |
| `source_version` | String | Versão do software que registrou o treino. |
| `start_date` | Timestamp | Data e hora do início do treino. |
| `end_date` | Timestamp | Data e hora do encerramento do treino. |

---

## 🥇 Camada Gold (PostgreSQL)

Os dados agregados finais que alimentam os dashboards são estruturados e expostos em tabelas relacionais no banco de dados.

### 1. Tabela `gold.daily_heart_rate_summary`
Armazena a consolidação diária dos batimentos cardíacos.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `date` | Date | Data do dia correspondente às medições (Chave Primária). |
| `avg_heart_rate` | Float64 | Frequência cardíaca média calculada para o dia (bpm). |
| `min_heart_rate` | Float64 | Frequência cardíaca mínima registrada no dia (bpm). |
| `max_heart_rate` | Float64 | Frequência cardíaca máxima registrada no dia (bpm). |
| `records_count` | Integer | Quantidade total de batimentos amostrados no dia. |
| `updated_at` | Timestamp | Data e hora do último processamento/carga deste registro. |

---

### 2. Tabela `gold.daily_activity_summary`
Consolida métricas de movimento, passos e gastos calóricos diários.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `date` | Date | Data do dia correspondente (Chave Primária). |
| `step_count` | Float64 | Total de passos acumulados no dia. |
| `active_energy_kcal` | Float64 | Total de calorias ativas gastas no dia (kcal). |
| `basal_energy_kcal` | Float64 | Gasto energético basal estimado do dia (kcal). |
| `distance_km` | Float64 | Distância total caminhada ou corrida acumulada no dia (km). |
| `updated_at` | Timestamp | Data e hora do último processamento/carga deste registro. |

---

### 3. Tabela `gold.workouts_summary`
Histórico estruturado e único de cada treino registrado.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `workout_type` | String | Modalidade do exercício (Chave Primária Composta). |
| `start_date` | Timestamp | Data e hora de início do exercício (Chave Primária Composta). |
| `end_date` | Timestamp | Data e hora de encerramento do exercício. |
| `duration_minutes` | Float64 | Duração total líquida do exercício em minutos. |
| `total_energy_kcal` | Float64 | Total de calorias queimadas ativas no treino (kcal). |
| `total_distance_km` | Float64 | Distância percorrida no treino (km). |
| `updated_at` | Timestamp | Data e hora do último processamento/carga deste registro. |
