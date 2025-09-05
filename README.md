# Scalable Property Data Pipeline with PySpark

A robust, scalable data pipeline for processing property listing data from JSONL format into DuckDB using Apache Spark for distributed processing, orchestrated with Apache Airflow and containerized with Docker.

## Overview

This project processes property listing data by:
1. **Extracting** data from JSONL files using Spark's distributed file reading
2. **Transforming** the data using PySpark DataFrames for scalable price parsing, calculations, and filtering
3. **Loading** the processed data into a DuckDB database for analysis

The pipeline leverages Apache Spark for horizontal scalability and includes comprehensive data validation, quality checks, and error handling.

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

## Features

- **Distributed Processing**: Apache Spark for horizontal scalability across multiple cores/nodes
- **Data Validation**: Pydantic models ensure data integrity
- **Price Parsing**: Robust parsing using Spark UDFs for various price formats (e.g., "530 000€/mo.")
- **Quality Filters**: Spark SQL-based filtering for optimal performance
- **Monitoring**: Comprehensive logging, Spark UI, and error reporting
- **Auto-optimization**: Dynamic Spark configuration based on dataset size
- **Orchestration**: Airflow DAG with task dependencies and monitoring
- **Containerization**: Docker setup with Java/Spark environment for reproducible deployments

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM and 2 CPU cores (8GB+ recommended for larger datasets)
- 10GB free disk space
- Java 11+ (automatically installed in Docker container)

## Quick Start

### 1. Clone and Setup

```bash
# Navigate to project directory
cd /path/to/pricehubble

# Create environment file
cp env.example .env

# Create necessary directories
mkdir -p logs plugins config output
```

### 2. Run with Docker Compose

#### Option A: Full Airflow Setup (Recommended)

```bash
# Initialize Airflow
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Access Airflow Web UI
open http://localhost:8080
# Username: airflow
# Password: airflow
```

#### Option B: Standalone Pipeline (Quick Test)

```bash
# Run pipeline without Airflow
docker-compose --profile standalone up pipeline-standalone
```

### 3. Monitor Execution

- **Airflow UI**: http://localhost:8080
- **Logs**: Check `logs/` directory
- **Output**: Results saved in `output/properties.duckdb`

## Project Structure

```
pricehubble/
├── src/
│   └── data_pipeline.py          # Main pipeline logic
├── dags/
│   └── property_data_pipeline_dag.py  # Airflow DAG
├── input/
│   └── scraping_data.jsonl       # Input data file
├── output/                       # Generated output files
├── logs/                         # Airflow logs
├── docker-compose.yml            # Docker services configuration
├── Dockerfile                    # Container image definition
├── requirements.txt              # Python dependencies
└── README.md                     # This file
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

**Key Classes:**
- `SparkPropertyDataProcessor`: Main PySpark processing engine with distributed computing
- `PropertyRecord`: Input data validation using Pydantic
- `ProcessedPropertyRecord`: Output data structure validation

**Key Methods:**
- `parse_price_spark()`: Distributed price parsing using Spark UDFs
- `apply_filters()`: Spark SQL-based filtering for optimal performance  
- `transform_data()`: PySpark DataFrame transformations
- `optimize_spark_for_dataset_size()`: Auto-configure Spark based on data size
- `run_pipeline()`: Orchestrates the complete distributed ETL process

### 2. Orchestration (`dags/property_data_pipeline_dag.py`)

**Airflow Tasks:**
1. `check_input_file`: Validates input file exists
2. `run_pipeline`: Executes main ETL process
3. `validate_output`: Performs data quality checks
4. `generate_report`: Creates execution summary
5. `cleanup_temp_files`: Cleans up temporary files

### 3. Configuration

**Environment Variables:**
- `AIRFLOW_UID`: User ID for Airflow processes
- `_AIRFLOW_WWW_USER_USERNAME`: Web UI username
- `_AIRFLOW_WWW_USER_PASSWORD`: Web UI password
- `JAVA_HOME`: Java installation path for Spark
- `SPARK_HOME`: Spark installation path
- `SPARK_DRIVER_MEMORY`: Memory allocation for Spark driver
- `SPARK_EXECUTOR_MEMORY`: Memory allocation for Spark executors

## Usage Examples

### Running Individual Components

```bash
# Test data pipeline directly
docker-compose exec airflow-webserver python /opt/airflow/src/data_pipeline.py

# Check pipeline statistics
docker-compose exec airflow-webserver python -c "
from src.data_pipeline import SparkPropertyDataProcessor
with SparkPropertyDataProcessor('StatCheck', '/opt/airflow/output/properties.duckdb') as p:
    samples = p.get_sample_output(5)
    print('Sample records:', samples)
"
```

### Accessing Results

```bash
# Copy output file to host
docker-compose exec airflow-webserver cp /opt/airflow/output/properties.duckdb /tmp/
docker cp $(docker-compose ps -q airflow-webserver):/tmp/properties.duckdb ./output/

# Query data directly
python -c "
import duckdb
conn = duckdb.connect('./output/properties.duckdb')
print(conn.execute('SELECT COUNT(*) FROM properties').fetchone())
print(conn.execute('SELECT * FROM properties LIMIT 3').fetchall())
"
```

## Troubleshooting

### Common Issues

1. **Permission Errors**
   ```bash
   # Fix file permissions
   sudo chown -R $USER:$USER logs/ plugins/ config/
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
   lsof -i :8080
   # Change port in docker-compose.yml if needed
   ```

### Logs and Debugging

```bash
# View Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# View pipeline logs
tail -f logs/dag_id=property_data_pipeline/run_id=*/task_id=*/attempt=*.log

# Debug container
docker-compose exec airflow-webserver bash
```

## Performance Considerations

- **Memory Usage**: Spark distributes processing across available cores with configurable memory allocation
- **Processing Time**: ~0.5-1 seconds per 1000 records (varies by cluster configuration)
- **Database Size**: DuckDB file size approximately 70% of input JSONL size
- **Scalability**: Horizontally scalable - can process multi-GB datasets across distributed Spark clusters
- **Optimization**: Auto-configures partitions and shuffle settings based on dataset size
- **Monitoring**: Spark UI provides real-time performance metrics and job monitoring

## Development

### Local Development Setup

```bash
# Install dependencies locally (requires Java 11+)
pip install -r requirements.txt

# Set environment variables for Spark
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Adjust path as needed
export SPARK_LOCAL_IP=127.0.0.1

# Run tests
python test_pipeline.py

# Run pipeline locally
python src/data_pipeline.py
```

### Adding New Features

1. **New Filters**: Modify `apply_filters()` method with Spark SQL conditions
2. **New Transformations**: Add methods to `SparkPropertyDataProcessor` using DataFrame operations
3. **New Validations**: Update Pydantic models and Spark schema definitions
4. **New Tasks**: Add tasks to Airflow DAG
5. **Performance Tuning**: Adjust Spark configurations in `optimize_spark_for_dataset_size()`

## Monitoring and Alerting

### Built-in Monitoring
- Airflow task status monitoring
- Spark UI for job tracking and performance metrics
- Data quality validation checks
- Execution time tracking
- Error logging and reporting
- Resource utilization monitoring

### Custom Metrics
- Records processed per minute
- Data quality score
- Pipeline success rate
- Spark job completion rates
- Memory and CPU utilization per executor
- Data skew detection and optimization suggestions

## Security Considerations

- Default Airflow credentials (change in production)
- Database stored in container volumes
- No external network access required
- Input data validation and sanitization

## Contributing

1. Follow PEP 8 style guidelines
2. Add docstrings to all functions
3. Include unit tests for new features
4. Update README for significant changes

## License

This project is developed for PriceHubble's data engineering assessment.

---

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

Spark UI available at: http://localhost:4040
```

The PySpark-based pipeline successfully processes property data with distributed computing, comprehensive validation, quality checks, and horizontal scalability - ready for production use with large datasets.
