# Apache Airflow ETL POC

This repository contains a proof of concept (POC) for an Extract, Transform, Load (ETL) pipeline using Apache Airflow.

## Overview

The ETL pipeline is defined in a Directed Acyclic Graph (DAG) and it performs the following tasks:

1. **Unzips** the data from a tarball file.
2. **Extracts** data from CSV, TSV, and fixed-width files.
3. **Consolidates** the extracted data into a single CSV file.
4. **Transforms** the consolidated data (converts a specific field to uppercase).
