# syntax=docker/dockerfile:1
FROM redhat/ubi8:latest

RUN dnf install -y \
    python3.11 python3.11-pip \
    && dnf clean all \
    && alternatives --set python3 /usr/bin/python3.11

# Install uv for fast Python package management
RUN pip3 install uv

# pyhive[hive] pulls in C 'sasl' which is broken on Python 3.11+
# Instead: install pyhive (base) + pure-sasl + thrift-sasl manually
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system \
    pure-sasl \
    thrift \
    && uv pip install --system --no-deps thrift-sasl \
    && uv pip install --system --no-deps pyhive \
    && uv pip install --system \
    "marimo[sql]" \
    duckdb \
    altair \
    numpy \
    pandas \
    pyarrow \
    future \
    python-dateutil \
    "pyiceberg[s3fs,pyarrow]"

EXPOSE 2718
CMD ["marimo", "run", "/opt/notebooks/explore.py", "--host", "0.0.0.0", "--port", "2718"]
