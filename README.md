# Hospital Data ETL Pipeline (PySpark + Docker + Google Cloud)

A production-grade ETL pipeline that automates the ingestion, transformation, and cloud-delivery of hospital synthetic
data. This project showcases **PySpark** modularity, **Docker** containerization, and cost-optimized **Google Cloud**
deployment.

## Prerequisites & Local Setup

Replicating a Spark environment on a local machine (especially Windows) requires specific version matching and
environment configurations.

### 1. Python & Java Compatibility

* **Python 3.11**: Avoid 3.12+ to prevent Py4J connectivity issues.
* **Java JDK 17**: Spark requires a specific Java runtime.
    * **Download**: [Microsoft Java](https://learn.microsoft.com/en-us/java/openjdk/download).
    * **Env Var**: Set `JAVA_HOME` to your installation path (e.g., `C:\Program Files\Microsoft\jdk-17...`).

### 2. The Hadoop/Winutils "Missing Bin" Issue

**Issue**: When running Spark on Windows, you will likely see an error:
`java.io.IOException: Could not locate executable null\bin\winutils.exe`.
**Solution**:

1. **Download**: Get the Hadoop binaries (specifically `winutils.exe` and `hadoop.dll`) for your version from a trusted
   repository like [cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.0/bin).
2. **Setup**:
    * Create a folder (e.g., `C:\hadoop`).
    * Create a `bin` folder inside it and place the downloaded files there.
    * **Env Var**: Create a system variable `HADOOP_HOME` pointing to `C:\hadoop`.
    * **Path**: Add `%HADOOP_HOME%\bin` to your System `Path`.

### 3. Kaggle API Authentication

1. Generate a `kaggle.json` from your [Kaggle Account Settings](https://www.kaggle.com/settings).
2. Set the following Environment Variables on your machine:
    * `KAGGLE_USERNAME`: Your Kaggle username.
    * `KAGGLE_KEY`: Your API Key.

---

## Project Architecture

This project follows a **Modular Design** to ensure the code is testable and cloud-ready.

* **`main.py`**: The entry point. Manages the SparkSession and environment orchestration.
* **`src/pipeline.py`**: The "Conductor." Defines the Bronze → Silver → Gold flow.
* **`src/cleanse_data.py`**: Pure Spark transformation logic (Feature Engineering).
* **`src/kaggle_data.py`**: Handles API ingestion and Excel-to-CSV conversion.
* **`src/gcp_utils.py`**: Custom utility to bridge local staging with Google Cloud Storage.
* **`notebooks/`**: Contains the initial Exploratory Data Analysis (EDA) and Spark prototyping.

---

## Containerization (Docker)

To solve "environment drift," the entire pipeline is containerized using a multi-layered Dockerfile.

### Local Testing with Docker Compose:

```bash
# Build the image
docker-compose build

# Run the pipeline (outputs to local /data folder)
docker-compose up

# Access the container shell for debugging
docker run -it --rm hospital_etl_test /bin/bash
```

### Google Cloud Deployment (Free Tier)

This pipeline is optimized for the Google Cloud Free Tier, utilizing Cloud Run Jobs to ensure $0.00 operating costs.1.
Infrastructure SetupGCS Bucket: Create a bucket in us-central1. Set a Lifecycle Rule to delete objects after 7 days to
avoid storage fees.Artifact Registry: Create a Docker repository for your images.IAM: Grant the Storage Object Admin
role to your Cloud Run Service Account.2. Deployment CommandsBash# Tag and Push the image

```bash
docker tag hospital_etl_test us-central1-docker.pkg.dev/[PROJECT_ID]/hospital-repo/etl-pipeline:v1
docker push us-central1-docker.pkg.dev/[PROJECT_ID]/hospital-repo/etl-pipeline:v1
```

### Deploy as a Cloud Run Job

```bash
gcloud run jobs deploy hospital-etl-job \
--image us-central1-docker.pkg.dev/[PROJECT_ID]/hospital-repo/etl-pipeline:v1 \
--region us-central1 \
--set-env-vars ENV_STATE=prod,GCS_BUCKET_NAME=[YOUR_BUCKET_NAME] \
--memory 2Gi
```

### Troubleshooting Summary

* JAVA_HOME ErrorIncorrect Java versionEnsure Java JDK 17 is installed and JAVA_HOME is set.
* winutils.exe ErrorMissing Hadoop binaries on WindowsSet HADOOP_HOME and add /bin to
  Path.
* Py4JNetworkErrorPython 3.12+ incompatibilityDowngrade to Python 3.11.401
* UnauthorizedMissing Kaggle CredentialsPass KAGGLE_USERNAME/KEY via Env Vars.

---