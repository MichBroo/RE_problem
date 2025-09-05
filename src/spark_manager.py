"""
Spark session manager for property data pipeline.

This module provides a centralized way to manage Spark sessions
with proper configuration and resource management.
"""

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
            # Create Spark configuration
            conf = SparkConf()
            conf.set("spark.app.name", self.app_name)
            conf.set("spark.master", self.master)
            conf.set("spark.executor.memory", self.memory)
            conf.set("spark.driver.memory", self.driver_memory)
            
            # Performance optimizations
            conf.set("spark.sql.adaptive.enabled", "true")
            conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            
            # Create Spark session
            spark = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session created: {self.app_name}")
            logger.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
            
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
