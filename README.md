#STEDI-Human-balance-analytics

This project focuses on building a cloud-based data pipeline for the **STEDI Human Balance Analytics** project using AWS services. The pipeline processes raw sensor and customer data, filters records based on customer consent, and creates curated datasets that can be used for downstream analytics and machine learning.


#Project Overview

The STEDI Human Balance Analytics project demonstrates an end-to-end data engineering workflow on AWS. Raw customer, accelerometer, and step trainer data are ingested into Amazon S3, transformed using AWS Glue ETL jobs, cataloged with the AWS Glue Data Catalog, and queried using Amazon Athena.

The pipeline ensures that only customers who have consented to share their data are included in the trusted and curated datasets, maintaining data privacy while preparing high-quality data for analytics.


#Table of Contents

- [Project Overview](#-project-overview)
- [Technologies Used](#-technologies-used)
- [Project Structure](#-project-structure)
- [Data Pipeline](#-data-pipeline)
- [Getting Started](#-getting-started)
- [Pipeline Architecture](#-pipeline-architecture)
- [Future Improvements](#-future-improvements)
- [License](#-license)

#Technologies Used

| Category | Technologies |
|----------|--------------|
| Language | Python, PySpark |
| Cloud Platform | AWS |
| Data Storage | Amazon S3 |
| Data Processing | AWS Glue |
| Metadata Catalog | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Data Format | JSON |
| Version Control | Git & GitHub |

---

# Project Structure

```text
STEDI-Human-balance-analytics/
│
├── glue-jobs/
│   ├── customer_trusted.py
│   ├── accelerometer_trusted.py
│   ├── customer_curated.py
│   └── machine_learning_curated.py
│
├── screenshots/
│   
│
├── architecture/
│   └── pipeline_diagram.png
│
├── README.md
└── LICENSE
```

> *Folder names may vary depending on your repository structure.*

---

#🔄 Data Pipeline

The project consists of four AWS Glue ETL jobs executed in sequence:

### 1. Customer Trusted

- Reads customer data from the landing zone.
- Filters customers who have consented to share their data.
- Stores the filtered data in the **Customer Trusted** zone.

#2. Accelerometer Trusted

- Reads accelerometer sensor data.
- Joins it with **Customer Trusted** data.
- Retains sensor data only for consented customers.

#3. Customer Curated

- Reads **Customer Trusted** and **Accelerometer Trusted** datasets.
- Creates a curated customer dataset containing only customers with accelerometer data.

#4. Machine Learning Curated

- Reads **Customer Curated** and Step Trainer data.
- Produces the final curated dataset used for analytics and machine learning.


#Getting Started

#Clone the Repository

```bash
git clone https://github.com/akhillakkapatri9/STEDI-Human-Balance-Analytics.git
cd STEDI-Human-balance-analytics
```

#Prerequisites

- AWS Account
- Amazon S3
- AWS Glue
- AWS IAM Role with Glue permissions
- AWS Glue Data Catalog
- Amazon Athena
- Python 3.x (optional for local development)

#Setup

1. Upload the provided datasets to the appropriate S3 landing buckets.
2. Create the required AWS Glue Crawlers.
3. Create Glue Data Catalog tables.
4. Configure IAM permissions for AWS Glue.
5. Execute the Glue ETL jobs in the following order:
   - Customer Trusted
   - Accelerometer Trusted
   - Customer Curated
   - Machine Learning Curated
6. Verify the output datasets in Amazon S3.
7. Query the generated tables using Amazon Athena.


#Pipeline Architecture

text
Customer Landing
        │
        ▼
Customer Trusted
        │
        ├──────────────┐
        ▼              │
Accelerometer Landing  │
        │              │
        ▼              │
Accelerometer Trusted  │
        │              │
        └──────┐       │
               ▼       │
        Customer Curated
               │
               │
Step Trainer Landing
               │
               ▼
     Machine Learning Curated

The final curated dataset contains only customers who:
- Have provided consent to share their data.
- Have corresponding accelerometer records.
- Have associated step trainer records.

This dataset is ready for downstream analytics and machine learning workflows.


#Future Improvements

- Automate the pipeline using AWS Step Functions.
- Add data quality validation using AWS Glue Data Quality.
- Integrate Amazon QuickSight dashboards.
- Add CI/CD using GitHub Actions.
- Implement incremental ETL processing.

---

#License

This project is licensed under the MIT License. See the `LICENSE` file for more information.
