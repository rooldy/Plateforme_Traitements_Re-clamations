# Dockerfile Airflow + Spark + PostgreSQL JDBC Optimisé
# Pré-installation locale de Spark pour éviter les timeouts

FROM apache/airflow:3.1.5-python3.12

USER root
#RUN chmod +x /docker-entrypoint-initdb.d/*.sh

# ============================================================================
# INSTALLATION JAVA + OUTILS SYSTÈME
# ============================================================================

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ✅ Path Java ARM64 (Mac M1/M2/M3)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# ============================================================================
# VARIABLES SPARK
# ============================================================================

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
#ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# ============================================================================
# COPIE DU TAR DE SPARK LOCAL
# ============================================================================

COPY docker_files/spark-3.5.1-bin-hadoop3.tgz /tmp/

RUN tar -xzf /tmp/spark-3.5.1-bin-hadoop3.tgz \
    && mv spark-3.5.1-bin-hadoop3 ${SPARK_HOME} \
    && rm /tmp/spark-3.5.1-bin-hadoop3.tgz \
    && chown -R airflow:0 ${SPARK_HOME}

# ============================================================================
# UTILISATEUR AIRFLOW
# ============================================================================

USER airflow

# ============================================================================
# PRÉ-TÉLÉCHARGEMENT DRIVER POSTGRESQL (Solution Hybride)
# ============================================================================

RUN mkdir -p /home/airflow/.ivy2

RUN spark-submit \
    --packages org.postgresql:postgresql:42.7.1 \
    --conf spark.jars.ivy=/home/airflow/.ivy2 \
    --version > /dev/null 2>&1 || true

RUN echo "✅ Vérification driver PostgreSQL..." && \
    find /home/airflow/.ivy2 -name "postgresql*.jar" -ls || echo "⚠️  JAR non trouvé"

# ============================================================================
# INSTALLATION DÉPENDANCES PYTHON
# ============================================================================

COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ============================================================================
# CRÉATION DES RÉPERTOIRES
# ============================================================================

RUN mkdir -p /opt/airflow/dags \
             /opt/airflow/logs \
             /opt/airflow/plugins \
             /opt/airflow/spark/jobs \
             /opt/airflow/spark/utils \
             /opt/airflow/data/raw \
             /opt/airflow/data/bronze \
             /opt/airflow/data/silver \
             /opt/airflow/data/gold

# ============================================================================
# HEALTHCHECK
# ============================================================================

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# ============================================================================
# INFORMATIONS BUILD
# ============================================================================

LABEL maintainer="Data Engineering Portfolio" \
      description="Airflow + Spark avec PostgreSQL JDBC optimisé" \
      spark.version="3.5.1" \
      postgres.jdbc.version="42.7.1" \
      architecture="medallion"

WORKDIR /opt/airflow