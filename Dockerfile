FROM apache/airflow:2.7.3-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies including Java for Spark
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-11-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create necessary directories
RUN mkdir -p /opt/airflow/input /opt/airflow/output /opt/airflow/src

# Copy source code
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/

# Set Python path and Spark configuration
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
ENV SPARK_HOME=/opt/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="$PATH:$SPARK_HOME/bin"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Set working directory
WORKDIR /opt/airflow
