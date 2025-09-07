FROM apache/airflow:2.7.3-python3.11

# Switch to root per installare dipendenze di sistema
USER root

# Installa solo le dipendenze essenziali
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Rileva automaticamente JAVA_HOME per qualsiasi architettura
RUN JAVA_HOME_PATH=$(find /usr/lib/jvm -name "java-11-openjdk-*" -type d | head -1) && \
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> /etc/environment && \
    echo "JAVA_HOME detected: $JAVA_HOME_PATH" && \
    java -version && echo "Java installation verified for $(uname -m) architecture"

# Non impostiamo ENV JAVA_HOME fisso - sarà rilevato dinamicamente

# Torna all'utente airflow
USER airflow

# Copia e installa i requirements Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Crea le directory necessarie
RUN mkdir -p /opt/airflow/input /opt/airflow/output /opt/airflow/src

# Copia il codice sorgente
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/

# Configura l'ambiente Python e Spark
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /opt/airflow