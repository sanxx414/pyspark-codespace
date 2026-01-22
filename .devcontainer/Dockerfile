FROM mcr.microsoft.com/devcontainers/python:3.10

# -----------------------------
# System tools
# -----------------------------
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    vim \
    git-lfs \
    procps \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Spark
# -----------------------------
ENV SPARK_VERSION=3.5.1
ENV HADOOP_SPARK_VERSION=3

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_SPARK_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_SPARK_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_SPARK_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_SPARK_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# -----------------------------
# Hadoop (HDFS)
# -----------------------------
ENV HADOOP_VERSION=3.3.6

RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# -----------------------------
# Hive
# -----------------------------
ENV HIVE_VERSION=3.1.3

RUN wget https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    mv apache-hive-${HIVE_VERSION}-bin /opt/hive && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

ENV HIVE_HOME=/opt/hive
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV PATH=$PATH:$HIVE_HOME/bin
