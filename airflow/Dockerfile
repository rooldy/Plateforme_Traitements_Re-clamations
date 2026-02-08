FROM apache/airflow:3.1.5

USER root

# Installe Java + procps (requis par Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME pour ARM64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Installe PySpark
RUN pip install --no-cache-dir --timeout=1000 --retries=10 \
    pyspark==3.5.1 \
    py4j==0.10.9.7 \
    apache-airflow-providers-apache-spark

# Vérification
RUN java -version && \
    python -c "from pyspark.sql import SparkSession; print('✅ PySpark + Java OK')"