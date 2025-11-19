# Automated Crypto ETL Pipeline

This repository contains a **near real-time cryptocurrency ETL pipeline** built using **Python, Apache Airflow, and PostgreSQL**. The pipeline extracts OHLCV (Open, High, Low, Close, Volume) data from the **Binance API**, transforms it (including **Simple Moving Average (SMA) calculations**), and loads it into a **PostgreSQL database** for analysis. You can explore and manage the data using **DBeaver** or any SQL client.

---

## Project Contents

- **dags/**: Contains the Airflow DAG file `crypto_etl_dag.py` which defines the ETL workflow.  
- **scripts/**: Helper Python functions for fetching and transforming data from the Binance API.  
- **Dockerfile**: Builds the custom Airflow runtime image for this project.  
- **requirements.txt**: Python dependencies required for the ETL pipeline.  
- **README.md**: Project documentation and setup instructions.  

---

## Features

- **Automated ETL Workflow**: Runs every minute using **Airflow DAGs**.  
- **Data Extraction**: Fetches OHLCV data from Binance API for BTC, ETH, and other crypto symbols.  
- **Data Transformation**: Calculates SMA and row-wise percentage changes.  
- **Data Loading**: Stores processed data in **PostgreSQL**, with duplicate handling.  
- **Data Exploration**: Connect to the database using **DBeaver** or any SQL client.  
- **Scalable Design**: Easy to extend to multiple symbols or different time intervals.  

---

## Technology Stack

- **Python** – Core ETL logic and API integration.  
- **Apache Airflow** – Scheduling and orchestration of DAGs.  
- **PostgreSQL** – Storing structured cryptocurrency data.  
- **DBeaver** – For querying, exploring, and managing PostgreSQL data.  
- **Binance API** – Source of real-time crypto market data.  

---

## Setup & Installation

1. Clone the repository:


git clone https://github.com/FaryalBhatti/automated_crypto_ETL_Pipeline.git
cd automated_crypto_ETL_Pipeline

2. Set up a Python virtual environment and install dependencies:

bash
python -m venv venv
# Linux/Mac
source venv/bin/activate
# Windows
venv\Scripts\activate
pip install -r requirements.txt

3. Set up PostgreSQL and Airflow (Docker recommended).
4. Configure an Airflow connection for PostgreSQL (postgres_default).
5. Optionally, connect DBeaver to PostgreSQL for data exploration.

---

## Usage
- Place the DAG file in your Airflow DAGs folder.
- Start Airflow webserver and scheduler.
- The DAG will run every minute to fetch, transform, and load crypto data.
- Monitor DAG execution and logs through the Airflow UI.
- Query and explore stored data using DBeaver.

---

## Future Enhancements
- Support dynamic multi-symbol pipelines.
- Add alerts for DAG failures via Slack or email.
- Implement additional analytics metrics like EMA, volatility, etc.
- Move to streaming architecture for sub-minute real-time processing.

Implement additional analytics metrics like EMA, volatility, etc.

Move to streaming architecture for sub-minute real-time processing.
