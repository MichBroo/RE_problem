"""
Spark utilities for multi-architecture support and session management.

This module provides utilities for creating and managing Spark sessions
with automatic Java/Spark detection across different architectures.
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Configure logging
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
    """
    Create a robust Spark session for any container environment and architecture.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        SparkSession: Configured Spark session
        
    Raises:
        RuntimeError: If Spark session cannot be created
    """
    # Detect Java and Spark installations dynamically
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
