# Internal Mirror Configuration

This document lists all external dependencies that need to be swapped to internal mirrors for use behind a corporate firewall. PyPI, Maven Central, and RPM repos are already configured in our base images and will work without changes.

## Container Images

These are pulled directly from public registries and need to be mirrored or proxied.

| Image | Source Registry | Referenced In | Notes |
|-------|----------------|--------------|-------|
| `redhat/ubi8:latest` | `registry.access.redhat.com` | `spark/Dockerfile`, `airflow/Dockerfile`, `airflow/Dockerfile.duckdb` | Base image for all custom containers |
| `localstack/localstack:latest` | Docker Hub | `docker-compose.yml` | S3-compatible storage |
| `ghcr.io/projectnessie/nessie:latest` | GitHub Container Registry | `docker-compose.yml` | Iceberg catalog (Spark mode only) |

### How to swap

Update `docker-compose.yml` and Dockerfiles with your internal registry prefix:

```yaml
# docker-compose.yml
image: your-registry.internal/localstack/localstack:latest
image: your-registry.internal/projectnessie/nessie:latest
```

```dockerfile
# Dockerfiles
FROM your-registry.internal/redhat/ubi8:latest
```

Or configure a registry mirror in Docker Desktop (Settings → Docker Engine):

```json
{
  "registry-mirrors": ["https://your-registry.internal"]
}
```

## Direct URL Downloads

These are fetched via `curl` during Docker build and need to be hosted internally or proxied.

| What | Current URL | File | Line |
|------|-------------|------|------|
| Apache Maven 3.9.9 | `https://dlcdn.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz` | `spark/Dockerfile` | 10 |
| Apache Spark 3.5.4 | `https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz` | `spark/Dockerfile` | 17 |
| Airflow constraints file | `https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt` | `airflow/Dockerfile` | 14 |
| Airflow constraints file | `https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt` | `airflow/Dockerfile.duckdb` | 19 |

### How to swap

**Option A: Host on internal artifact store**

Upload the files to Artifactory/Nexus generic repo and update the Dockerfiles:

```dockerfile
# spark/Dockerfile — replace the ARG defaults or pass as build args
ARG MAVEN_MIRROR=https://artifacts.internal/maven
ARG SPARK_MIRROR=https://artifacts.internal/spark

RUN curl -fsSL ${MAVEN_MIRROR}/apache-maven-3.9.9-bin.tar.gz | tar -xz -C /opt
RUN curl -fsSL ${SPARK_MIRROR}/spark-3.5.4-bin-hadoop3.tgz | tar -xz -C /opt
```

```dockerfile
# airflow/Dockerfile, airflow/Dockerfile.duckdb
ARG CONSTRAINT_URL="https://artifacts.internal/airflow/constraints-2.10.4-3.11.txt"
```

**Option B: Vendor the constraints file**

Download the constraints file once and commit it to the repo:

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt" -OutFile "airflow/constraints.txt"
```

Then update both Airflow Dockerfiles:

```dockerfile
COPY constraints.txt /tmp/constraints.txt
RUN pip3 install "apache-airflow==2.10.4" --constraint /tmp/constraints.txt
```

**Option C: Docker build args (no code changes needed)**

Pass mirror URLs at build time without modifying Dockerfiles:

```powershell
docker compose build `
  --build-arg MAVEN_VERSION=3.9.9 `
  --build-arg CONSTRAINT_URL="https://artifacts.internal/airflow/constraints.txt"
```

*(Requires the Dockerfiles to use ARG for the URLs, which they already do for MAVEN_VERSION, SPARK_VERSION, and CONSTRAINT_URL.)*

## Summary

| Dependency Type | Already Mirrored? | Action Needed |
|----------------|-------------------|---------------|
| RPM packages (dnf) | ✅ Yes (base image) | None |
| PyPI packages (pip) | ✅ Yes (base image) | None |
| Maven JARs (pom.xml) | ✅ Yes (base image) | None |
| Container images | ❌ No | Mirror or prefix with internal registry |
| Apache Maven binary | ❌ No | Host internally or proxy `dlcdn.apache.org` |
| Apache Spark binary | ❌ No | Host internally or proxy `dlcdn.apache.org` |
| Airflow constraints | ❌ No | Host internally or vendor into repo |
