# ---------------------------------------------------------------------------
# Spark + Iceberg image for Kubeflow Spark Operator
#
# Base  : apache/spark:4.1.1-python3
# Adds  : Iceberg runtime JAR, hadoop-aws, AWS SDK, Python packages
#
# Build:
#   docker build -t your-registry/spark-iceberg:4.1.1 -f image/Dockerfile .
# ---------------------------------------------------------------------------

ARG SPARK_VERSION=4.1.1

FROM apache/spark:${SPARK_VERSION}-python3

# ---------------------------------------------------------------------------
# Build-time arguments
# ---------------------------------------------------------------------------
ARG ICEBERG_VERSION=1.8.0
ARG SPARK_MAJOR=4.1
ARG SCALA_VERSION=2.13

# Hadoop / AWS SDK versions bundled with Spark 4.1.1
ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_VERSION=1.12.648

ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

# ---------------------------------------------------------------------------
# Switch to root to install packages and download JARs
# ---------------------------------------------------------------------------
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Download Iceberg Spark runtime JAR
# Must match Spark major + Scala binary version.
# ---------------------------------------------------------------------------
RUN curl -fsSL \
    "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
    -o "/opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"

# ---------------------------------------------------------------------------
# Download Iceberg AWS integration JAR (Glue catalog, S3FileIO)
# ---------------------------------------------------------------------------
RUN curl -fsSL \
    "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    -o "/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# ---------------------------------------------------------------------------
# Download hadoop-aws for s3a:// filesystem support
# (Spark distribution may already include this; download ensures the version
# matches hadoop-common bundled with Spark 4.1.1)
# ---------------------------------------------------------------------------
RUN curl -fsSL \
    "${MAVEN_CENTRAL}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    -o "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar"

# ---------------------------------------------------------------------------
# Download AWS Java SDK bundle (required by hadoop-aws)
# ---------------------------------------------------------------------------
RUN curl -fsSL \
    "${MAVEN_CENTRAL}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ---------------------------------------------------------------------------
# Install Python dependencies
# ---------------------------------------------------------------------------
COPY image/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# ---------------------------------------------------------------------------
# Copy PySpark scripts into the image
# Scripts can also be loaded from S3 at runtime (set spark_main_file to s3a://)
# ---------------------------------------------------------------------------
COPY pyspark/ /opt/spark/pyspark/

# ---------------------------------------------------------------------------
# Restore non-root Spark user (UID 185 in the upstream image)
# ---------------------------------------------------------------------------
USER spark

# Default working directory expected by Kubeflow Spark Operator
WORKDIR /opt/spark

# Metadata
LABEL maintainer="your-team@example.com" \
      spark.version="${SPARK_VERSION}" \
      iceberg.version="${ICEBERG_VERSION}" \
      description="Spark image with Iceberg support for Kubeflow Spark Operator"
