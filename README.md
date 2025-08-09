# NYC TLC Data Insights & GenAI Integration

## Overview
This project demonstrates an end-to-end data and AI pipeline using AWS services to process NYC Taxi & Limousine Commission (TLC) trip data, visualize insights, train prediction models, and enable natural language querying through a Generative AI interface powered by Amazon Bedrock.

The workflow covers:
- Data ingestion and storage on S3
- Data cataloging with AWS Glue
- Querying and transformation using Athena
- Visualization with QuickSight
- Model training & deployment in SageMaker
- Conversational analytics with Bedrock (Claude Sonnet 3.5) via Lambda

---

## Architecture

TLC Data → S3 (Raw) → AWS Glue → Athena (Tables/Views) → QuickSight (Dashboard)
                                                       → SageMaker (Models)
                                                       → Lambda → Bedrock (Claude Sonnet 3.5)

---

## Implementation Steps

### 1. Data Acquisition & Storage
- Downloaded **NYC TLC trip records** for July–December 2024.
- Uploaded the raw CSV data to an **S3 bucket** (`genai-taxi-raw`).

### 2. Data Cataloging & Transformation
- Configured an **AWS Glue Crawler** to detect schema and create a raw table in the Athena Data Catalog.
- Created **curated tables** in Athena with necessary filtering, null handling, and type casting.
- Built **Athena views** for aggregated analysis:
  - Trips & revenue by hour
  - Borough-level metrics
  - Tip percentage analysis

### 3. Data Visualization
- Built an **Amazon QuickSight** dashboard (see image below) with:
  - Trips over time
  - Trip distance distribution
  - Trip fares breakdown
  - Pickup vs dropoff borough heatmap
  - Revenue by borough
  - Tip percentage vs fare amount
  - Average tip by distance

<img width="1077" height="1135" alt="Screenshot 2025-08-08 at 3 03 49 PM" src="https://github.com/user-attachments/assets/867d27cd-5915-4c5a-a426-1f12f9dab773" />


### 4. Predictive Modeling
- Trained **two XGBoost prediction models** in SageMaker:
  - **Tip Amount Prediction** regression model
  - **Tip Given Prediction** classification model
- Used curated Athena data for training.
- Deployed the models as SageMaker endpoints for inference.

### Tip Amount Prediction (Regression Model)

| Metric                               | Value        |
|--------------------------------------|--------------|
| Test Samples (n)                     | 73,542       |
| RMSE                                 | 2.5604       |
| MAE                                  | 1.2971       |
| MAPE (%)                             | 79.32%       |
| R²                                   | 0.6541       |
| Baseline RMSE                        | 4.3538       |
| RMSE Improvement vs Baseline (%)     | 41.19%       |

---

### Tip > 0 Classification Model

| Metric       | Value    |
|--------------|----------|
| Test Samples | 73,542   |
| Accuracy     | 95.42%   |
| Precision    | 94.41%   |
| Recall       | 99.93%   |
| F1 Score     | 97.09%   |
| AUC          | 92.99%   |

### 5. Conversational Analytics with Bedrock
- Created an **AWS Lambda function** to:
  - Accept natural language questions
  - Generate SQL queries using **Claude Sonnet 3.5** (Amazon Bedrock)
  - Execute queries in Athena
  - Return results along with a narrative summary
- Example query:
  > "Show total trips and revenue by hour for Manhattan pickups on 1st August 2024."

<img width="1755" height="826" alt="Screenshot 2025-08-08 at 7 58 59 PM" src="https://github.com/user-attachments/assets/235fa403-758f-4935-b22e-7b895c9436f1" />

---

## Technologies Used
- **AWS S3** – Data storage
- **AWS Glue** – Schema detection & ETL
- **Amazon Athena** – Serverless querying
- **Amazon QuickSight** – Visualization
- **Amazon SageMaker** – Model training & deployment
- **Amazon Bedrock** – Generative AI (Claude Sonnet 3.5)
- **AWS Lambda** – Serverless execution
- **Python / Pandas / boto3** – Data processing

---

## Key Highlights
- End-to-end AWS-based pipeline from **raw data ingestion to AI-powered insights**.
- Combines **traditional BI (QuickSight)** with **Generative AI (Bedrock)** for natural language analytics.
- Modular architecture for reusability and scaling.
