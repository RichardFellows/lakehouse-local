FROM redhat/ubi8:latest

# Java 17 + Python 3.11
RUN dnf install -y java-17-openjdk-devel python3.11 python3.11-pip procps hostname tar gzip curl \
    && dnf clean all \
    && alternatives --set python3 /usr/bin/python3.11

# Maven for dependency resolution
ARG MAVEN_VERSION=3.9.14
RUN curl -fsSL https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    | tar -xz -C /opt \
    && ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn

# Spark
ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3
RUN curl -fsSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Pull Iceberg + Nessie + AWS JARs via Maven
COPY pom.xml /tmp/deps/pom.xml
RUN cd /tmp/deps \
    && mvn dependency:copy-dependencies -DoutputDirectory=${SPARK_HOME}/jars -q \
    && rm -rf /tmp/deps ~/.m2

# PySpark
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 7077 8080 10000
ENTRYPOINT ["/entrypoint.sh"]
