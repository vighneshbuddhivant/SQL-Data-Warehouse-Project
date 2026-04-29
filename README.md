
# 📊 End-to-End Data Warehouse Project (SQL Server)

---

## 📌 Overview

This project demonstrates the design and implementation of a **modern data warehouse** using SQL Server. The goal is to consolidate data from multiple sources (CRM and ERP systems), clean and transform it, and make it ready for analytical reporting.

The project follows industry-standard practices including **ETL processing, Medallion Architecture, and Star Schema modeling**.

---

## 🎯 Objective

* Build a centralized data warehouse for sales data
* Integrate multiple data sources (CRM & ERP)
* Ensure high data quality through cleaning and transformation
* Provide a user-friendly data model for reporting
* Enable analytics using BI tools

---

## 🔄 ETL Process (Extract, Transform, Load)

### 🔹 Extract

Data is collected from source systems without any modification.

* **Type Used:** Full Extraction
* **Method Used:** Pull Extraction
* **Technique Used:** File Parsing (CSV files)

---

### 🔹 Transform

Data is cleaned and processed to make it usable.

* Remove duplicates
* Handle NULL and invalid values
* Standardize formats
* Apply business rules
* Perform data integration

---

### 🔹 Load

Processed data is loaded into the target system.

* **Processing Type:** Batch Processing
* **Load Method:** Full Load (Truncate & Insert)
* **SCD Type Used:** SCD Type 1 (Overwrite)

---

## 🏗️ Data Architecture

This project uses the **Medallion Architecture**, which organizes data into three layers:

<img width="1544" height="912" alt="data_architecture" src="https://github.com/user-attachments/assets/c60345b7-68b0-493e-bed6-d22e92188817" />




### 🥉 Bronze Layer (Raw Data Layer)

Stores raw data exactly as received from source systems. Data is ingested from CSV files into SQL Server without any transformation, ensuring traceability and serving as the source of truth.

---

### 🥈 Silver Layer (Processed Data Layer)

Applies data cleansing, standardization, and transformation. This layer ensures data quality by handling duplicates, missing values, and inconsistencies, making the data ready for further use.

---

### 🥇 Gold Layer (Business Layer)

Contains business-ready data structured using a **Star Schema**. This layer is optimized for reporting and analytics, enabling efficient querying and easy integration with BI tools.

---

## 📋 Project Implementation Steps

1. Requirement Analysis
2. Data Architecture Design
3. Project Initialization
4. Build Bronze Layer
5. Build Silver Layer
6. Build Gold Layer

---

## 📊 Requirement Analysis

* Build a modern data warehouse using SQL Server
* Use CRM and ERP datasets as sources
* Clean and standardize data before analysis
* Integrate multiple data sources
* Provide a business-friendly data model
* Use only latest data (no historization required)
* Maintain proper documentation

---

## ⚙️ Project Initialization

### Naming Conventions

* Snake_case used for all database objects
* Avoided SQL reserved keywords

### Table Naming

* Bronze/Silver: `<source_system>_<entity>`
* Gold: `dim_<entity>`, `fact_<entity>`

### Keys

* Surrogate and primary keys follow: `<table_name>_key`

### Stored Procedures

* Naming format: `load_<layer>`

### Setup

* Created SQL Server database
* Created schemas:

  * bronze
  * silver
  * gold

---

## 🥉 Bronze Layer Implementation

* Created tables for CRM and ERP data
* Loaded data from CSV files into SQL Server
* Used bulk loading approach for efficiency
* Applied truncate before loading to avoid duplication
* Automated loading using stored procedures

---

## 🥈 Silver Layer Implementation

* Performed detailed data exploration
* Applied data cleaning techniques:

  * Removed duplicates
  * Handled NULL values
  * Trimmed unwanted spaces
* Standardized and transformed data
* Applied business rules and validations

### Metadata

* Added system-generated columns to track data load

---

## 🥇 Gold Layer Implementation

* Built a **business-friendly data model**
* Implemented **Star Schema**

### ⭐ Schema Design

* **Dimension Tables**

  * Customers
  * Products

* **Fact Table**

  * Sales

### Features

* Surrogate keys for better performance
* Clean relationships between tables
* Optimized for reporting and analytics

---

## 📈 Final Outcome

* Successfully built an end-to-end data warehouse
* Implemented Medallion Architecture
* Designed Star Schema for analytics
* Ensured high data quality and consistency
* Data is ready for BI tools like:

  * Power BI
  * Tableau

---

## 🧠 Key Learnings

* End-to-end data warehouse development
* ETL pipeline design and implementation
* Data cleaning and transformation techniques
* Data modeling using Star Schema
* SQL Server-based data engineering

---

