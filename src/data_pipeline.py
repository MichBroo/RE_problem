"""
Scalable data pipeline for processing property listing data using PySpark.

This module contains the main data processing logic for extracting,
transforming and loading property data from JSONL format into DuckDB
using Apache Spark for distributed processing and scalability.
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path

import duckdb
from pyspark.sql import functions as func

import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _detect_java_home() -> str:
    """
    Detect Java installation path dynamically for any architecture.
    
    Returns:
        str: Path to Java home directory
    """
    import glob
    import platform
    
    # Check environment variable first
    if os.environ.get('JAVA_HOME'):
        java_home = os.environ['JAVA_HOME']
        if os.path.exists(java_home):
            logger.info(f"Using JAVA_HOME from environment: {java_home}")
            return java_home
    
    # Common Java paths to check
    java_search_paths = [
        '/usr/lib/jvm/java-11-openjdk-*',  # Standard OpenJDK
        '/usr/lib/jvm/java-1.11.0-openjdk-*',  # Alternative naming
        '/usr/lib/jvm/default-java',  # Debian/Ubuntu default
        '/usr/lib/jvm/java-11-*',  # Generic Java 11
        '/opt/java/openjdk',  # Docker OpenJDK images
        '/usr/java/default',  # RedHat/CentOS
    ]
    
    arch = platform.machine().lower()
    logger.info(f"Detecting Java for architecture: {arch}")
    
    for search_path in java_search_paths:
        if '*' in search_path:
            matches = glob.glob(search_path)
            if matches:
                # Sort to get consistent results, prefer architecture-specific
                matches.sort()
                for match in matches:
                    if os.path.exists(os.path.join(match, 'bin', 'java')):
                        logger.info(f"Found Java at: {match}")
                        return match
        else:
            if os.path.exists(os.path.join(search_path, 'bin', 'java')):
                logger.info(f"Found Java at: {search_path}")
                return search_path
    
    # Fallback: try to find java executable and derive home
    import shutil
    java_exec = shutil.which('java')
    if java_exec:
        # Follow symlinks and try to find JAVA_HOME
        java_real = os.path.realpath(java_exec)
        possible_home = os.path.dirname(os.path.dirname(java_real))
        if os.path.exists(os.path.join(possible_home, 'lib')):
            logger.info(f"Derived Java home from executable: {possible_home}")
            return possible_home
    
    # Last resort fallback
    fallback = '/usr/lib/jvm/java-11-openjdk-amd64'
    logger.warning(f"Java not found, using fallback: {fallback}")
    return fallback


def _detect_spark_home() -> str:
    """
    Detect PySpark installation path dynamically.
    
    Returns:
        str: Path to PySpark installation
    """
    # Check environment variable first
    if os.environ.get('SPARK_HOME'):
        spark_home = os.environ['SPARK_HOME']
        if os.path.exists(spark_home):
            logger.info(f"Using SPARK_HOME from environment: {spark_home}")
            return spark_home
    
    # Try to find PySpark installation
    try:
        import pyspark
        pyspark_path = os.path.dirname(pyspark.__file__)
        logger.info(f"Found PySpark at: {pyspark_path}")
        return pyspark_path
    except ImportError:
        pass
    
    # Common PySpark paths in containers
    spark_search_paths = [
        '/home/airflow/.local/lib/python3.11/site-packages/pyspark',
        '/home/airflow/.local/lib/python3.*/site-packages/pyspark',
        '/usr/local/lib/python3.*/site-packages/pyspark',
        '/opt/airflow/.local/lib/python3.*/site-packages/pyspark',
    ]
    
    for search_path in spark_search_paths:
        if '*' in search_path:
            import glob
            matches = glob.glob(search_path)
            if matches:
                spark_home = matches[0]
                logger.info(f"Found Spark at: {spark_home}")
                return spark_home
        else:
            if os.path.exists(search_path):
                logger.info(f"Found Spark at: {search_path}")
                return search_path
    
    # Fallback
    fallback = '/home/airflow/.local/lib/python3.11/site-packages/pyspark'
    logger.warning(f"Spark not found, using fallback: {fallback}")
    return fallback


def create_spark_session(app_name: str = "PropertyDataPipeline") -> SparkSession:
    """Create a robust Spark session for any container environment and architecture."""
    
    # Detect Java installation dynamically for multi-architecture support
    java_home = _detect_java_home()
    spark_home = _detect_spark_home()
    
    # Set environment variables
    os.environ['JAVA_HOME'] = java_home
    os.environ['SPARK_HOME'] = spark_home
    os.environ['PYSPARK_PYTHON'] = 'python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
    
    # Log detected configuration
    import platform
    logger.info(f"Multi-arch setup for {platform.machine()} architecture:")
    logger.info(f"  JAVA_HOME: {java_home}")
    logger.info(f"  SPARK_HOME: {spark_home}")
    logger.info(f"  Java exists: {os.path.exists(java_home)}")
    logger.info(f"  Spark exists: {os.path.exists(spark_home)}")
    
    # Initialize findspark with explicit path
    try:
        import findspark
        findspark.init(spark_home=spark_home)
        logger.info(f"findspark initialized with: {spark_home}")
    except ImportError:
        logger.warning("findspark not available")
    except Exception as e:
        logger.warning(f"findspark initialization failed: {e}")
    
    # Create ultra-minimal Spark configuration for containers
    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.master", "local[1]")  # Single thread to avoid complexity
    conf.set("spark.driver.memory", "512m")  # Minimum required by Spark
    conf.set("spark.executor.memory", "512m")
    
    # Disable all UI and external services
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf.set("spark.shuffle.service.enabled", "false")
    conf.set("spark.dynamicAllocation.enabled", "false")
    
    # Container-friendly networking
    conf.set("spark.driver.host", "localhost")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf.set("spark.driver.port", "0")
    conf.set("spark.blockManager.port", "0")
    conf.set("spark.driver.blockManager.port", "0")
    
    # Disable problematic features
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
    
    # Set warehouse directory
    conf.set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    
    # Java options for stability
    conf.set("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    conf.set("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    
    logger.info("Creating Spark session with minimal configuration...")
    
    try:
        # Clear any existing Spark context
        from pyspark import SparkContext
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
            logger.info("Stopped existing Spark context")
        
        # Create new session
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")  # Minimize logging
        
        logger.info(f"✅ Spark session created successfully: {app_name}")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark master: {spark.sparkContext.master}")
        
        return spark
        
    except Exception as e:
        logger.error(f"❌ Failed to create Spark session: {e}")
        logger.error(f"Java Home exists: {os.path.exists(java_home)}")
        logger.error(f"Spark Home exists: {os.path.exists(spark_home)}")
        raise RuntimeError(f"Cannot initialize Spark session: {e}") from e


def create_duckdb_table(duckdb_conn):
    """Create the properties table in DuckDB."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS properties (
        id VARCHAR PRIMARY KEY,
        scraping_date DATE NOT NULL,
        property_type VARCHAR NOT NULL,
        municipality VARCHAR NOT NULL,
        price DOUBLE NOT NULL,
        living_area DOUBLE NOT NULL,
        price_per_square_meter DOUBLE NOT NULL
    )
    """
    
    duckdb_conn.execute(create_table_sql)
    logger.info("Created properties table in DuckDB")
    

def load_jsonl_data(spark, file_path: str):
    """
    Load data from JSONL file using Spark.
    
    Args:
        spark: Spark session
        file_path: Path to JSONL file
        
    Returns:
        Spark DataFrame with raw data
    """
    try:
        # Read JSONL file - let Spark infer schema automatically
        df = spark.read \
            .option("multiline", "false") \
            .json(file_path)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count} records from {file_path}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}")
        raise
    

def parse_price_spark(df):
    """
    Parse raw price using native Spark SQL functions (no UDF).
    
    Args:
        df: Input DataFrame with raw_price column
        
    Returns:
        DataFrame with parsed price column
    """
    # Parse price using native Spark functions
    df_with_price = df.withColumn(
        "price",
        # Remove all non-numeric characters except dots, then convert directly
        func.regexp_replace(
            func.col("raw_price"),
            r'[^\d.]', ''  # Remove everything except digits and dots (including spaces)
        ).cast("double")
    )
    
    # Filter out records where price parsing failed
    df_valid_price = df_with_price.filter(
        func.col("price").isNotNull() & 
        (func.col("price") > 0)
    )
    
    # Log parsing statistics
    total_records = df.count()
    valid_records = df_valid_price.count()
    failed_parsing = total_records - valid_records
    
    logger.info(f"Price parsing: {valid_records}/{total_records} successful, {failed_parsing} failed")
    
    return df_valid_price
    

def transform_data(df):
    """
    Apply all transformations to the data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Parse prices
    df_with_price = parse_price_spark(df)
    
    # Convert scraping_date to proper date format
    df_with_date = df_with_price.withColumn(
        "scraping_date_parsed", 
        func.to_date(func.col("scraping_date"), "yyyy-MM-dd")
    )
    
    # Calculate price per square meter
    df_with_price_per_sqm = df_with_date.withColumn(
        "price_per_square_meter",
        func.col("price") / func.col("living_area")
    )
    
    # Select final columns in correct order
    df_final = df_with_price_per_sqm.select(
        func.col("id"),
        func.col("scraping_date_parsed").alias("scraping_date"),
        func.col("property_type"),
        func.col("municipality"),
        func.col("price"),
        func.col("living_area"),
        func.col("price_per_square_meter")
    )
    
    logger.info("Applied data transformations")
    return df_final
    

def apply_filters(df):
    """
    Apply filtering criteria using Spark SQL.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Filtered DataFrame
    """
    # Define filter conditions
    filters = [
        # Property type filter
        func.col("property_type").isin(["apartment", "house"]),
        
        # Price per square meter filter
        (func.col("price_per_square_meter") >= 500) & 
        (func.col("price_per_square_meter") <= 15000),
        
        # Date filter (after March 5, 2020)
        func.col("scraping_date") > func.lit("2020-03-05"),
        
        # Data quality filters
        func.col("price").isNotNull(),
        func.col("living_area").isNotNull(),
        func.col("living_area") > 0,
        func.col("price") > 0
    ]
    
    # Apply all filters
    df_filtered = df
    for filter_condition in filters:
        df_filtered = df_filtered.filter(filter_condition)
    
    # Log filtering statistics
    original_count = df.count()
    filtered_count = df_filtered.count()
    filtered_out = original_count - filtered_count
    
    logger.info(f"Filtering: {filtered_count}/{original_count} records passed, {filtered_out} filtered out")
    
    return df_filtered
    
    

def save_to_duckdb(df, duckdb_conn):
    """
    Save Spark DataFrame to DuckDB.
    
    Args:
        df: Spark DataFrame to save
        duckdb_conn: DuckDB connection
    """
    # Convert Spark DataFrame to Pandas for DuckDB insertion
    # For very large datasets, this could be done in batches
    pandas_df = df.toPandas()
    
    # Clear existing data and insert new data
    duckdb_conn.execute("TRUNCATE TABLE properties")
    duckdb_conn.register('df_temp', pandas_df)
    duckdb_conn.execute("""
        INSERT INTO properties 
        SELECT * FROM df_temp
    """)
    
    record_count = len(pandas_df)
    logger.info(f"Saved {record_count} records to DuckDB")
    
    

def get_processing_stats(original_df, final_df, failed_processing: int = 0):
    """
    Calculate processing statistics.
    
    Args:
        original_df: Original input DataFrame
        final_df: Final processed DataFrame
        failed_processing: Number of records that failed processing
        
    Returns:
        Dictionary with statistics
    """
    total_input = original_df.count()
    final_records = final_df.count()
    filtered_out = total_input - failed_processing - final_records
    
    return {
        "total_input_records": total_input,
        "failed_processing": failed_processing,
        "filtered_out": filtered_out,
        "final_records": final_records
    }
    

def run_pipeline(spark, input_file: str, db_path: str) -> Dict[str, int]:
    """
    Run the complete data pipeline using Spark.
    
    Args:
        spark: Spark session
        input_file: Path to input JSONL file
        db_path: Path to DuckDB database file
        
    Returns:
        Dictionary with processing statistics
    """
    logger.info("Starting Spark-based data pipeline")
    
    # Create DuckDB connection and table
    duckdb_conn = duckdb.connect(db_path)
    create_duckdb_table(duckdb_conn)
    
    try:
        # Load raw data
        raw_df = load_jsonl_data(spark, input_file)
        
        # Cache the raw DataFrame for multiple operations
        raw_df.cache()
        
        # Transform data
        transformed_df = transform_data(raw_df)
        
        # Apply filters
        filtered_df = apply_filters(transformed_df)
        
        # Cache filtered DataFrame before final operations
        filtered_df.cache()
        
        # Save to DuckDB
        save_to_duckdb(filtered_df, duckdb_conn)
        
        # Calculate statistics
        stats = get_processing_stats(raw_df, filtered_df)
        
        # Unpersist cached DataFrames
        raw_df.unpersist()
        filtered_df.unpersist()
        
        logger.info(f"Pipeline completed: {stats}")
        return stats
        
    finally:
        duckdb_conn.close()
    

def get_sample_output(duckdb_conn, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get sample records from the processed data.
    
    Args:
        duckdb_conn: DuckDB connection
        limit: Number of sample records to return
        
    Returns:
        List of sample records
    """
    result = duckdb_conn.execute(f"""
        SELECT * FROM properties 
        ORDER BY scraping_date DESC 
        LIMIT {limit}
    """).fetchall()
    
    columns = [desc[0] for desc in duckdb_conn.description]
    return [dict(zip(columns, row)) for row in result]


def main():
    """Main function for running the pipeline directly."""
    
    input_file = "input/scraping_data.jsonl"
    db_path = "output/properties.duckdb"
    
    # Ensure output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        stats = run_pipeline(spark, input_file, db_path)
        
        print("\nPipeline Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("\nSample Output:")
        duckdb_conn = duckdb.connect(db_path)
        try:
            samples = get_sample_output(duckdb_conn)
            for sample in samples:
                print(f"  {sample}")
        finally:
            duckdb_conn.close()
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()
