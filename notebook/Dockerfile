FROM redhat/ubi8:latest

RUN dnf install -y \
    python3.11 python3.11-pip python3.11-devel \
    gcc gcc-c++ \
    cyrus-sasl-devel \
    && dnf clean all \
    && alternatives --set python3 /usr/bin/python3.11

RUN pip3 install --no-cache-dir \
    "marimo[sql]" \
    duckdb \
    "pyhive[hive]" \
    thrift \
    sasl \
    thrift-sasl \
    altair \
    numpy \
    pandas \
    pyarrow

EXPOSE 2718
CMD ["marimo", "run", "/opt/notebooks/explore.py", "--host", "0.0.0.0", "--port", "2718"]
