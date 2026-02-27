<!-- Language / 언어 -->
<div align="right">
  <a href="#english">English</a> &nbsp;|&nbsp; <a href="#korean">한국어</a>
</div>

---

<a name="english"></a>

# Image

This directory contains the Dockerfile and supporting files for building the
custom Spark Docker image used by the Kubeflow Spark Operator.

## Contents

| File | Description |
|------|-------------|
| `Dockerfile` | Image based on `apache/spark:4.1.1-python3` with Iceberg and AWS support |
| `requirements.txt` | Python packages installed into the image |

## What's Included

The image extends the official Apache Spark 4.1.1 image with:

- **Iceberg runtime JAR** — enables Iceberg table format support in Spark SQL
- **Iceberg AWS bundle JAR** — Glue catalog and S3FileIO support
- **hadoop-aws JAR** — `s3a://` filesystem support
- **AWS Java SDK bundle** — required by hadoop-aws
- **Python packages** — listed in `requirements.txt`
- **PySpark scripts** — the `pyspark/` directory is baked in at `/opt/spark/pyspark/`

## Build

```bash
# Build from the repository root
docker build \
  -t your-registry/spark-iceberg:4.1.1 \
  -f image/Dockerfile \
  .
```

### Build Arguments

| ARG | Default | Description |
|-----|---------|-------------|
| `ICEBERG_VERSION` | `1.8.0` | Iceberg release to download |
| `SPARK_MAJOR` | `4.1` | Spark major version (used in JAR naming) |
| `SCALA_VERSION` | `2.13` | Scala binary version |
| `HADOOP_VERSION` | `3.4.1` | Hadoop version for hadoop-aws |
| `AWS_SDK_VERSION` | `1.12.648` | AWS Java SDK version |

```bash
docker build \
  --build-arg ICEBERG_VERSION=1.8.0 \
  -t your-registry/spark-iceberg:4.1.1 \
  -f image/Dockerfile \
  .
```

## Push

```bash
docker push your-registry/spark-iceberg:4.1.1
```

## Spark Configuration (Runtime)

S3 and catalog settings are **not** baked into the image. Inject them at runtime
through `spark_conf` in `IcebergMaintenanceOperator`:

```python
spark_conf={
    # AWS Glue catalog
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.warehouse": "s3a://your-bucket/warehouse",

    # S3 credentials via IRSA / Workload Identity
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

    # MinIO / custom endpoint (optional)
    # "spark.hadoop.fs.s3a.endpoint": "http://minio-service:9000",
    # "spark.hadoop.fs.s3a.path.style.access": "true",
}
```

## Kubernetes RBAC

The Spark driver pod runs under the `spark` ServiceAccount.
Apply the following RBAC resources:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-role
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps"]
    verbs: ["create", "get", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-role-binding
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark
roleRef:
  kind: ClusterRole
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
```

## Notes

- The image runs as the non-root `spark` user (UID 185) as in the upstream image.
- JARs are downloaded from Maven Central at build time. For air-gapped environments,
  replace `curl` commands with your internal artifact registry URLs.

---

<a name="korean"></a>

# Image

Kubeflow Spark Operator가 사용하는 커스텀 Spark Docker 이미지를 빌드하기 위한
Dockerfile과 지원 파일이 들어 있는 디렉터리입니다.

## 파일 목록

| 파일 | 설명 |
|------|------|
| `Dockerfile` | `apache/spark:4.1.1-python3` 기반에 Iceberg / AWS 지원 추가 |
| `requirements.txt` | 이미지에 설치할 Python 패키지 목록 |

## 이미지에 포함된 것

공식 Apache Spark 4.1.1 이미지에 다음을 추가합니다.

- **Iceberg runtime JAR** — Spark SQL에서 Iceberg 테이블 포맷 지원
- **Iceberg AWS bundle JAR** — Glue 카탈로그 및 S3FileIO 지원
- **hadoop-aws JAR** — `s3a://` 파일시스템 지원
- **AWS Java SDK bundle** — hadoop-aws 의존성
- **Python 패키지** — `requirements.txt` 목록
- **PySpark 스크립트** — `pyspark/` 디렉터리가 `/opt/spark/pyspark/` 에 포함

## 빌드

```bash
# 레포지토리 루트에서 실행
docker build \
  -t your-registry/spark-iceberg:4.1.1 \
  -f image/Dockerfile \
  .
```

### 빌드 인자 (Build Arguments)

| ARG | 기본값 | 설명 |
|-----|--------|------|
| `ICEBERG_VERSION` | `1.8.0` | 다운로드할 Iceberg 버전 |
| `SPARK_MAJOR` | `4.1` | Spark 메이저 버전 (JAR 파일명에 사용) |
| `SCALA_VERSION` | `2.13` | Scala 바이너리 버전 |
| `HADOOP_VERSION` | `3.4.1` | hadoop-aws 버전 |
| `AWS_SDK_VERSION` | `1.12.648` | AWS Java SDK 버전 |

```bash
docker build \
  --build-arg ICEBERG_VERSION=1.8.0 \
  -t your-registry/spark-iceberg:4.1.1 \
  -f image/Dockerfile \
  .
```

## 푸시

```bash
docker push your-registry/spark-iceberg:4.1.1
```

## Spark 설정 (런타임)

S3 및 카탈로그 설정은 이미지에 포함하지 않습니다.
`IcebergMaintenanceOperator`의 `spark_conf`를 통해 런타임에 주입하세요.

```python
spark_conf={
    # AWS Glue 카탈로그
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.warehouse": "s3a://your-bucket/warehouse",

    # S3 인증 (IRSA / Workload Identity)
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",

    # MinIO / 커스텀 엔드포인트 (선택)
    # "spark.hadoop.fs.s3a.endpoint": "http://minio-service:9000",
    # "spark.hadoop.fs.s3a.path.style.access": "true",
}
```

## Kubernetes RBAC

Spark 드라이버 Pod는 `spark` ServiceAccount로 실행됩니다.
다음 RBAC 리소스를 적용하세요.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-role
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps"]
    verbs: ["create", "get", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-role-binding
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark
roleRef:
  kind: ClusterRole
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
```

## 참고

- 이미지는 업스트림 이미지와 동일하게 non-root `spark` 유저 (UID 185)로 실행됩니다.
- JAR은 빌드 시점에 Maven Central에서 다운로드합니다. 폐쇄망 환경이라면
  `curl` 명령을 내부 아티팩트 레지스트리 URL로 교체하세요.
