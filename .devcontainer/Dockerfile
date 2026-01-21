FROM mcr.microsoft.com/devcontainers/python:3.10

# Install system packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    vim \
    git-lfs \
    && rm -rf /var/lib/apt/lists/*


# Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark
RUN wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# PySpark
RUN pip install pyspark
