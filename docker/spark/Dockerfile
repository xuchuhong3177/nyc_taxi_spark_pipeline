FROM bitnami/spark:3.5.1

USER root

# 安装 Python 和常见依赖
RUN apt-get update && \
    apt-get install -y python3-pip openjdk-17-jdk && \
    pip3 install --no-cache-dir pandas pyarrow psycopg[binary] findspark

ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app