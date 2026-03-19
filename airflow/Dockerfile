FROM redhat/ubi8:latest

# Java 17 (for Spark adapter) + Python 3.11
RUN dnf install -y \
    java-17-openjdk-headless \
    python3.11 python3.11-pip python3.11-devel \
    gcc gcc-c++ \
    libffi-devel openssl-devel cyrus-sasl-devel \
    git \
    && dnf clean all \
    && alternatives --set python3 /usr/bin/python3.11

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV AIRFLOW_HOME=/opt/airflow
ENV PIP_NO_CACHE_DIR=1

# Step 1: Install Airflow with constraints (pins compatible versions)
ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip3 install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"

# Step 2: Install dbt adapters + Cosmos (without constraints — they conflict)
RUN pip3 install \
    dbt-duckdb \
    "dbt-spark[PyHive]" \
    "astronomer-cosmos[openlineage]" \
    "apache-airflow-providers-apache-spark<5" \
    boto3

# Step 3: Ensure Airflow wasn't upgraded by transitive deps
RUN pip3 install "apache-airflow==${AIRFLOW_VERSION}" --force-reinstall --no-deps

RUN mkdir -p ${AIRFLOW_HOME}
WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080
CMD ["airflow", "standalone"]
