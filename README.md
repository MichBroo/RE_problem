# Property Data Pipeline

## Overview

This project processes property listing data using a scalable ETL pipeline that:

1. **Extracts** data from JSONL files using Spark's distributed file reading
2. **Transforms** the data using PySpark DataFrames for price parsing, calculations, and filtering
3. **Loads** the processed data into a DuckDB database for analysis

The pipeline leverages Apache Spark for horizontal scalability and includes comprehensive data validation, quality checks, and error handling orchestrated by Apache Airflow.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   JSONL Input   │───▶│  PySpark ETL    │───▶│   DuckDB Out    │
│  (Raw Data)     │    │ (Distributed)   │    │ (Clean Data)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ Apache Airflow  │
                       │ (Orchestration) │
                       └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  Apache Spark   │
                       │ (Compute Engine)│
                       └─────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- At least 2GB RAM and 1 CPU core (4GB+ recommended for larger datasets)
- 5GB free disk space
- Java 11+ (automatically installed in Docker container)

## Quick Start

### 1. Clone and Setup

```bash
# Navigate to project directory
cd /path/to/RE_problem
```

### 2. Run with Docker Compose

#### Airflow Setup (Simplified)

```bash
# Start all services (Airflow + PostgreSQL) - simplified configuration
docker compose up -d --build

# Wait for services to be ready (about 1-2 minutes)
# Check status
docker compose ps

# Access Airflow Web UI
# URL: http://localhost:8080
# Username: admin
# Password: admin
```

#### Standalone Option (Quick Test)

```bash
# Run pipeline without Airflow
docker compose --profile standalone up pipeline-standalone
```

### 3. Monitor Execution

- **Airflow UI**: http://localhost:8080
- **Logs**: Check `logs/` directory
- **Output**: Results saved in `output/properties.duckdb`

## Project Structure

```
RE_problem/
├── src/
│   ├── data_pipeline.py             # Main pipeline logic
│   └── spark_manager.py             # Spark session management
├── dags/
│   └── property_data_pipeline_dag.py # Airflow DAG definition
├── input/
│   └── scraping_data.jsonl          # Input data file
├── output/                          # Generated output files
├── logs/                            # Airflow logs
├── docker-compose.yml               # Docker services configuration
├── Dockerfile                       # Container image definition
├── requirements.txt                 # Python dependencies
├── test_pipeline.py                 # Pipeline tests
└── README.md                        # This file
```

## Data Schema

### Input Format (JSONL)
```json
{
  "id": "0000a4fb",
  "raw_price": "530 000€/mo.",
  "living_area": 84.0,
  "property_type": "apartment",
  "municipality": "Solothurn",
  "scraping_date": "2021-02-17"
}
```

### Output Format (DuckDB)
```json
{
  "id": "0000a4fb",
  "scraping_date": "2021-02-17",
  "property_type": "apartment",
  "municipality": "Solothurn",
  "price": 530000.0,
  "living_area": 84.0,
  "price_per_square_meter": 6309.52
}
```

## Filtering Criteria

The pipeline applies the following filters:
- **Price per square meter**: Between 500 and 15,000
- **Property type**: Only "apartment" or "house"
- **Scraping date**: After March 5, 2020

## Pipeline Components

### 1. Data Processing (`src/data_pipeline.py`)

**Key Functions:**
- `load_jsonl_data()`: Loads JSONL data using Spark
- `parse_price()`: Parses price strings to numeric values
- `transform_data()`: Applies transformations using PySpark DataFrames
- `apply_filters()`: Filters data based on business rules
- `save_to_duckdb()`: Saves processed data to DuckDB
- `run_pipeline()`: Orchestrates the complete ETL process

### 2. Spark Management (`src/spark_manager.py`)

**SparkManager Class:**
- Manages Spark session lifecycle
- Optimizes Spark configuration based on dataset size
- Provides context manager for resource cleanup

### 3. Orchestration (`dags/property_data_pipeline_dag.py`)

**Airflow Tasks:**
1. `check_input_file`: Validates input file exists
2. `run_pipeline`: Executes main ETL process
3. `validate_output`: Performs data quality checks
4. `generate_report`: Creates execution summary
5. `cleanup_temp_files`: Cleans up temporary files

## Usage Examples

### Running Individual Components

```bash
# Test data pipeline directly
docker compose exec airflow python /opt/airflow/src/data_pipeline.py

# Access container shell for debugging
docker compose exec airflow bash
```

### Accessing Results

```bash
# Copy output file to host
docker compose cp airflow:/opt/airflow/output/properties.duckdb ./output/

# Query data directly (requires DuckDB installed locally)
python -c "
import duckdb
conn = duckdb.connect('./output/properties.duckdb')
print('Total records:', conn.execute('SELECT COUNT(*) FROM properties').fetchone()[0])
print('Sample data:', conn.execute('SELECT * FROM properties LIMIT 3').fetchall())
"
```

## Troubleshooting

### Common Issues

1. **Permission Errors**
   ```bash
   # Fix file permissions
   sudo chown -R $USER:$USER logs/ output/
   ```

2. **Memory Issues**
   ```bash
   # Check Docker resources
   docker system df
   docker system prune
   ```

3. **Port Conflicts**
   ```bash
   # Check if port 8080 is in use
   netstat -an | grep :8080
   # Change port in docker-compose.yml if needed
   ```

### Logs and Debugging

```bash
# View Airflow logs
docker compose logs airflow

# View pipeline logs in Airflow UI
# Go to http://localhost:8080 > DAGs > property_data_pipeline > Graph View > Click on task > Logs

# Debug container
docker compose exec airflow bash
```

### Local Development Setup

```bash
# Install dependencies locally (requires Java 11+)
pip install -r requirements.txt

# Set environment variables for Spark
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_LOCAL_IP=127.0.0.1

# Run tests
python test_pipeline.py

# Run pipeline locally
python src/data_pipeline.py
```

### Adding New Features

1. **New Filters**: Modify `apply_filters()` function in `data_pipeline.py`
2. **New Transformations**: Add functions to the transformation pipeline
3. **New Validations**: Update data validation logic
4. **New Tasks**: Add tasks to Airflow DAG in `property_data_pipeline_dag.py`

## Monitoring and Alerting

### Built-in Monitoring
- Airflow task status monitoring via Web UI
- Spark UI for job tracking (when available)
- Data quality validation checks
- Execution time tracking
- Error logging and reporting

### Available Metrics
- Records processed per execution
- Data quality statistics
- Pipeline success/failure rates
- Processing time per stage

## Security Considerations

- Default Airflow credentials (change in production)
- Database stored in container volumes
- No external network access required
- Input data validation and sanitization

## Testing

Run the test suite to validate pipeline functionality:

```bash
# Run tests in container
docker compose exec airflow python test_pipeline.py

# Or locally (requires local setup)
python test_pipeline.py
```

## Example Output

After successful execution, you should see output similar to:

```
Pipeline Statistics:
  total_input_records: 101
  failed_processing: 2
  filtered_out: 15
  final_records: 84

Sample Output:
  {'id': '000640ca', 'scraping_date': '2022-11-24', 'property_type': 'apartment', 'municipality': 'Volketswil', 'price': 1573000.0, 'living_area': 182.0, 'price_per_square_meter': 8643.96}
```

The pipeline successfully processes property data with comprehensive validation, quality checks, and scalability features ready for production use.

---

*This project demonstrates a production-ready data pipeline using modern tools like Apache Spark, Apache Airflow, and DuckDB for efficient property data processing.*