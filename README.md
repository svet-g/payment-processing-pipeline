# International Payment Processing Data Pipeline

A comprehensive data engineering project that processes international payment data using SWIFT messages. This pipeline demonstrates ETL best practices, cloud infrastructure, orchestration, and CI/CD implementation using modern data engineering tools.

## 🏗️ Architecture

```
[Data Generation] → [PySpark Ingestion] → [GCS Storage] → [PySpark Analysis] → [Analytics Output]
                            ↓
                    [Data Quality Checks]
                            ↓
                    [Airflow Orchestration]
                            ↓
                    [Jenkins CI/CD Pipeline]
```

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/payment-processing-pipeline.git
cd payment-processing-pipeline
```

### 2. Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```