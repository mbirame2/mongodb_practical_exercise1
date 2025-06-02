# Patient Data Management System

A Python application that processes patient CSV data, stores it in MongoDB, and runs analytical queries.

## Features

- **ETL Pipeline**:
  - Loads patient data from CSV files
  - Transforms data into structured JSON documents
  - Inserts records into MongoDB
- **Query Engine**:
  - Performs analytics on patient data
  - Answers key medical questions

## Prerequisites

- Python 3.8+
- MongoDB 4.4+
- Python packages:
  ```bash
  pip install pymongo pandas

## File Structure

project/
├── data/               # Input CSV files
├── alreadyUpload/      # Processed files
├── scripts/
│   ├── etl.py          # Data loading script
│   └── queries.py      # Analytics queries
└── README.md