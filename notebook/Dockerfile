FROM redhat/ubi8:latest

RUN dnf install -y \
    python3.11 python3.11-pip \
    && dnf clean all \
    && alternatives --set python3 /usr/bin/python3.11

# pyhive[hive] pulls in C 'sasl' which is broken on Python 3.11+
# Instead: install pyhive (base) + pure-sasl + thrift-sasl manually
RUN pip3 install --no-cache-dir \
    pure-sasl \
    thrift \
    && pip3 install --no-cache-dir --no-deps thrift-sasl \
    && pip3 install --no-cache-dir --no-deps pyhive \
    && pip3 install --no-cache-dir \
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
