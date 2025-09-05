"""
Airflow DAG for Property Data Pipeline.

This DAG orchestrates the property data processing pipeline that:
1. Extracts data from JSONL file
2. Transforms and validates the data
3. Loads processed data into DuckDB
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_pipeline import run_pipeline, get_sample_output, create_spark_session


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'property_data_pipeline',
    default_args=default_args,
    description='Process property listing data from JSONL to DuckDB',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['data-engineering', 'property', 'etl'],
)


def check_input_file(**context):
    """
    Check if input file exists and is readable.
    
    Args:
        context: Airflow context dictionary
        
    Raises:
        FileNotFoundError: If input file doesn't exist
    """
    input_file = "/opt/airflow/input/scraping_data.jsonl"
    
    if not Path(input_file).exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    file_size = Path(input_file).stat().st_size
    context['task_instance'].xcom_push(key='input_file_size', value=file_size)
    
    print(f"Input file check passed. File size: {file_size} bytes")


def run_data_pipeline(**context):
    """
    Run the main data processing pipeline.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        dict: Processing statistics
    """
    input_file = "/opt/airflow/input/scraping_data.jsonl"
    db_path = "/opt/airflow/output/properties.duckdb"
    
    # Ensure output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Create and run pipeline with Spark
    spark = create_spark_session("AirflowPropertyPipeline")
    try:
        stats = run_pipeline(spark, input_file, db_path)
    finally:
        spark.stop()
    
    # Push statistics to XCom for downstream tasks
    context['task_instance'].xcom_push(key='pipeline_stats', value=stats)
    
    print("Pipeline Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    return stats


def validate_output(**context):
    """
    Validate the output data in DuckDB.
    
    Args:
        context: Airflow context dictionary
        
    Raises:
        ValueError: If validation fails
    """
    db_path = "/opt/airflow/output/properties.duckdb"
    
    duckdb_conn = duckdb.connect(db_path)
    
    try:
        # Check if table exists and has data
        result = duckdb_conn.execute("SELECT COUNT(*) FROM properties").fetchone()
        record_count = result[0]
        
        if record_count == 0:
            raise ValueError("No records found in output table")
        
        # Validate data quality
        validation_queries = [
            ("price_range", "SELECT COUNT(*) FROM properties WHERE price <= 0"),
            ("living_area_range", "SELECT COUNT(*) FROM properties WHERE living_area <= 0"),
            ("price_per_sqm_range", "SELECT COUNT(*) FROM properties WHERE price_per_square_meter < 500 OR price_per_square_meter > 15000"),
            ("property_type_valid", "SELECT COUNT(*) FROM properties WHERE property_type NOT IN ('apartment', 'house')"),
            ("date_range", "SELECT COUNT(*) FROM properties WHERE scraping_date <= '2020-03-05'")
        ]
        
        validation_results = {}
        for check_name, query in validation_queries:
            result = duckdb_conn.execute(query).fetchone()
            invalid_count = result[0]
            validation_results[check_name] = invalid_count
            
            if invalid_count > 0:
                print(f"WARNING: {invalid_count} records failed {check_name} validation")
        
        # Get sample records
        samples = get_sample_output(duckdb_conn, 3)
        
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        context['task_instance'].xcom_push(key='sample_records', value=samples)
        context['task_instance'].xcom_push(key='total_records', value=record_count)
    
    finally:
        duckdb_conn.close()
    
    print(f"Validation completed. Total records: {record_count}")
    print("Sample records:")
    for sample in samples:
        print(f"  {sample}")


def generate_summary_report(**context):
    """
    Generate a summary report of the pipeline execution.
    
    Args:
        context: Airflow context dictionary
    """
    # Pull data from XCom
    ti = context['task_instance']
    
    input_file_size = ti.xcom_pull(task_ids='check_input_file', key='input_file_size')
    pipeline_stats = ti.xcom_pull(task_ids='run_pipeline', key='pipeline_stats')
    validation_results = ti.xcom_pull(task_ids='validate_output', key='validation_results')
    total_records = ti.xcom_pull(task_ids='validate_output', key='total_records')
    sample_records = ti.xcom_pull(task_ids='validate_output', key='sample_records')
    
    # Generate report
    report = f"""
    Property Data Pipeline Execution Summary
    ========================================
    
    Execution Date: {context['ds']}
    
    Input File:
    - Size: {input_file_size} bytes
    
    Processing Statistics:
    - Total input records: {pipeline_stats.get('total_input_records', 'N/A')}
    - Failed processing: {pipeline_stats.get('failed_processing', 'N/A')}
    - Filtered out: {pipeline_stats.get('filtered_out', 'N/A')}
    - Final records: {pipeline_stats.get('final_records', 'N/A')}
    
    Data Quality Validation:
    - Total records in output: {total_records}
    - Invalid price records: {validation_results.get('price_range', 'N/A')}
    - Invalid living area records: {validation_results.get('living_area_range', 'N/A')}
    - Price per sqm out of range: {validation_results.get('price_per_sqm_range', 'N/A')}
    - Invalid property types: {validation_results.get('property_type_valid', 'N/A')}
    - Invalid dates: {validation_results.get('date_range', 'N/A')}
    
    Sample Records:
    {chr(10).join([str(record) for record in sample_records])}
    """
    
    print(report)
    
    # Save report to file
    report_path = f"/opt/airflow/output/pipeline_report_{context['ds']}.txt"
    with open(report_path, 'w') as f:
        f.write(report)
    
    print(f"Report saved to: {report_path}")


# Task definitions
check_input_task = PythonOperator(
    task_id='check_input_file',
    python_callable=check_input_file,
    dag=dag,
)

pipeline_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_data_pipeline,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_output',
    python_callable=validate_output,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_summary_report,
    dag=dag,
)

cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='find /tmp -name "*.tmp" -type f -delete || true',
    dag=dag,
)

# Task dependencies
check_input_task >> pipeline_task >> validate_task >> report_task >> cleanup_task
