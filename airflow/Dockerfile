FROM redhat/ubi8:latest

# Java 17 (for PySpark) + Python 3.11
RUN dnf install -y java-17-openjdk-headless python3.11 python3.11-pip gcc python3.11-devel git \
    && dnf clean all \
    && alternatives --set python3 /usr/bin/python3.11

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV AIRFLOW_HOME=/opt/airflow

# Constraints file for Airflow compatibility
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip3 install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"

RUN pip3 install --no-cache-dir \
    "dbt-spark[PyHive]" \
    "astronomer-cosmos[openlineage]" \
    apache-airflow-providers-apache-spark

RUN mkdir -p ${AIRFLOW_HOME}
WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080
CMD ["airflow", "standalone"]
