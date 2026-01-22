# 🚗 Motor Insurance Data Pipeline

**Metadata-driven PySpark framework for validating and ingesting motor insurance policy data**

## 📋 Overview

This project demonstrates a **dynamic, metadata-driven ETL pipeline** built with PySpark that:
- ✅ Reads configuration from JSON metadata (no hardcoded logic)
- ✅ Applies business validation rules to insurance policies
- ✅ Splits records into validated (OK) and rejected (KO) datasets
- ✅ Adds audit metadata (ingestion timestamps)
- ✅ Fully containerized with Docker
- ✅ Production-ready with Airflow orchestration

---

## 🏗️ Architecture

```
                ┌─────────────────┐
                │  JSON Metadata  │
                │   (Config)      │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────────────────────────────┐
                │       DataflowEngine (PySpark)          │
                │  ┌─────────┐  ┌──────────┐  ┌────────┐  │
                │  │ Sources │─>│Transform │─>│ Sinks  │  │
                │  └─────────┘  └──────────┘  └────────┘  │
                └─────────────────────────────────────────┘
                        │
                        ├───> output/events/motor_policy (OK)
                        └───> output/discards/motor_policy (KO)
```

---

## 📂 Project Structure

```
.
├── dags/
│   └── motor_insurance_dag.py           # Airflow DAG definition
├── data/
│   ├── input/events/motor_policy/      
                        └── sample.jsonl # Input JSON files
│   └── output/
│       ├── events/motor_policy/         # Validated records
│       └── discards/motor_policy/       # Rejected records
├── metadata/
│   └── motor_insurance_config.json      # Pipeline configuration
├── src/
│   ├── engine/
│   │   └── dataflow_engine.py           # Core processing engine
│   └── utils/
│       └── spark_session.py             # Spark session factory
├── docker-compose.yml                   # Multi-container orchestration
├── Dockerfile                           # PySpark standalone image
├── Dockerfile.airflow                   # Airflow image
└── requirements.txt                     # Python dependencies
```

---

## 🚀 Quick Start

### Airflow Orchestration (Production)

```bash
# Start Airflow services
docker-compose up -d

# Access Airflow UI
http://localhost:8080
# Username: admin
# Password: admin

# Trigger the DAG: "motor_policy_ingestion"
```

---

## 🔧 Validation Rules

The engine supports **metadata-driven validation rules**:

|    Rule       |               Example               |         Description          |
|---------------|-------------------------------------|------------------------------|
| `notNull`     | `"validations": ["notNull"]`        | Field cannot be null         |
| `notEmpty`    | `"validations": ["notEmpty"]`       | Field cannot be empty string |

**Example metadata config:**

```json
{
  "field": "driver_age",
  "validations": ["notNull"]
}
```

---

## 📊 Sample Data

### Input (`data/input/events/motor_policy/sample.jsonl`):

```json
{"policy_number":"12345","driver_age":45,"plate_number":""}
{"policy_number":"67890","plate_number":"ABC-123"}
{"policy_number":"54321","driver_age":30,"plate_number":"XYZ-789"}
{"policy_number":"11111","driver_age":20,"plate_number":"DEF-456"}
{"policy_number":"","driver_age":40,"plate_number":""}
```

### Output - Valid Records (OK):

```json
{
  "driver_age": 30,
  "plate_number": "XYZ-789",
  "policy_number": "54321",
  "ingestion_dt": "2025-01-22 10:41:42"
},
{
  "driver_age": 20,
  "plate_number": "DEF-456",
  "policy_number": "11111",
  "ingestion_dt": "2025-01-22 10:41:42"
}
```

### Output - Invalid Records (KO):

```json
{
  "driver_age": 45,
  "plate_number": "",
  "policy_number": "12345",
  "validation_errors": {
    "field": "plate_number",
    "rule": "notEmpty",
    "message": "plate_number is empty"
    },
  "ingestion_dt": "2025-01-22 10:41:42"
},
{
  "driver_age": 40,
  "plate_number": "",
  "policy_number": "",
  "validation_errors": {
    "field": "policy_number",
    "rule": "notEmpty",
    "message": "policy_number is empty"
    },
    {
      "field": "plate_number",
      "rule": "notEmpty",
      "message": "plate_number is empty"
    }
  "ingestion_dt": "2025-01-22 10:41:42"
}
```

---

## 🧪 Adding New Validations

1. **Add rule to `dataflow_engine.py`:**

```python
elif rule_name == 'yourRule':
    invalid_condition = your_logic_here
    error_message = f"{field} failed your rule"
```

2. **Update metadata config:**

```json
{
  "field": "your_field",
  "validations": ["yourRule:param"]
}
```

3. **Run pipeline** - no code changes needed!

---

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (database)
docker-compose down -v

# Clean output directories
rm -rf data/output/*
```

---

## 📝 Key Takeaways

This project demonstrates:

1. **Metadata-driven design** - configuration over code
2. **Data quality enforcement** - validation with audit trails
3. **Separation of concerns** - engine, config, orchestration
4. **Production-ready patterns** - containerization, logging, scheduling

**Perfect for:** Data engineers building scalable, maintainable ETL pipelines.