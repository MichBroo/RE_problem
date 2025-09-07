FROM apache/airflow:2.7.3-python3.11

# Switch to root to install system dependencies
USER root

# Install only essential dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Automatically detect JAVA_HOME for any architecture
RUN JAVA_HOME_PATH=$(find /usr/lib/jvm -name "java-11-openjdk-*" -type d | head -1) && \
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> /etc/environment && \
    echo "JAVA_HOME detected: $JAVA_HOME_PATH" && \
    java -version && echo "Java installation verified for $(uname -m) architecture"

# Do not set fixed ENV JAVA_HOME - it will be detected dynamically

# Return to airflow user
USER airflow

# Copy and install Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create necessary directories
RUN mkdir -p /opt/airflow/input /opt/airflow/output /opt/airflow/src

# Copy source code
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/

# Configure Python and Spark environment
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /opt/airflow