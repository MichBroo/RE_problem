"""
Spark session manager for property data pipeline.

This module provides a centralized way to manage Spark sessions
with proper configuration and resource management.
"""

import os
import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

logger = logging.getLogger(__name__)


class SparkManager:
    """Manages Spark session initialization and configuration."""
    
    def __init__(self, app_name: str = "PropertyDataPipeline", 
                 master: str = "local[*]",
                 memory: str = "2g",
                 driver_memory: str = "1g"):
        """
        Initialize Spark manager.
        
        Args:
            app_name: Name of the Spark application
            master: Spark master URL
            memory: Executor memory
            driver_memory: Driver memory
        """
        self.app_name = app_name
        self.master = master
        self.memory = memory
        self.driver_memory = driver_memory
        self._spark_session: Optional[SparkSession] = None
    
    def get_spark_session(self) -> SparkSession:
        """
        Get or create Spark session.
        
        Returns:
            SparkSession: Configured Spark session
        """
        if self._spark_session is None:
            self._spark_session = self._create_spark_session()
        
        return self._spark_session
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create a new Spark session with optimized configuration.
        
        Returns:
            SparkSession: Newly created Spark session
        """
        try:
            # Set JAVA_HOME if not set
            if not os.environ.get('JAVA_HOME'):
                java_home = '/usr/lib/jvm/java-11-openjdk-amd64'
                os.environ['JAVA_HOME'] = java_home
                logger.info(f"Set JAVA_HOME to: {java_home}")
            
            # Set SPARK_HOME if not set
            if not os.environ.get('SPARK_HOME'):
                spark_home = '/home/airflow/.local/lib/python3.11/site-packages/pyspark'
                os.environ['SPARK_HOME'] = spark_home
                logger.info(f"Set SPARK_HOME to: {spark_home}")
            
            # Create Spark configuration
            conf = SparkConf()
            conf.set("spark.app.name", self.app_name)
            conf.set("spark.master", self.master)
            conf.set("spark.executor.memory", self.memory)
            conf.set("spark.driver.memory", self.driver_memory)
            
            # Container-specific configurations
            conf.set("spark.driver.host", "localhost")
            conf.set("spark.driver.bindAddress", "0.0.0.0")
            conf.set("spark.ui.enabled", "false")  # Disable Spark UI in container
            conf.set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            
            # Disable external shuffle service and use local mode
            conf.set("spark.shuffle.service.enabled", "false")
            conf.set("spark.dynamicAllocation.enabled", "false")
            
            # Performance optimizations
            conf.set("spark.sql.adaptive.enabled", "true")
            conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            
            # Disable Arrow for compatibility
            conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
            
            # Try to create Spark session with findspark first
            try:
                import findspark
                findspark.init()
            except ImportError:
                pass  # findspark not available, continue without it
            
            # Create Spark session
            spark = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session created: {self.app_name}")
            
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    
    def stop_spark_session(self):
        """Stop the Spark session and clean up resources."""
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
            logger.info("Spark session stopped")
    
    def __enter__(self):
        """Context manager entry."""
        return self.get_spark_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_spark_session()
    


# Global instance for easy access
spark_manager = SparkManager()
