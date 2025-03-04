# ETL Pipeline for Online Retail & Telegram Data

## 1. Introduction

This project builds an ETL (Extract, Transform, Load) pipeline to Visualizing Online Retail data from UCI Machine Learning Repository and Telegram messages from multiple channels(3 channels). The goal is to automate data collection, clean and store it in PostgreSQL, and visualize insights using Power BI. The pipeline is designed to enhance decision-making by combining structured Online Retail transactions with unstructured customer interactions from Telegram.

## 2. Data Sources

### Online RetailData (UCI Machine Learning Repository) <br>

Format: CSV <br>

Link: https://archive.ics.uci.edu/dataset/352/online+retail <br>

Fields: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

### Telegram Data

Channels: easybuyethiopia, Ecommerceaddis, jiji_shop_ethiopia <br>
Extracted Fields: Date, Message, User ID <br>

---

## 3. Data Extraction

### Online Retail Data Extraction

Loaded CSV using pandas (read_csv()) <br>
Validated and cleaned before transformation

### Telegram Data Extraction

Step 1: Go to https://my.telegram.org <br>

Step 2: Navigate to API Development Tools <br>

Step 3: Create a new application to get api_id and api_hash <br>

Step 4: Use these credentials in the Telethon client setup <br>

Telethon to Extracted date, message text, and user ID

## 4. Data Cleaning

### Online RetailData

Dropped missing values (dropna())
Removed duplicates (drop_duplicates())
Converted InvoiceDate to datetime, UnitPrice to float, and CustomerID to int

### Telegram Data

Preserved negative User IDs for groups
Standardized date format (datetime)
Removed duplicate messages
Ensured user_id values fit within PostgreSQL BIGINT limits

---

### 5. Development Tools

VS Code - Used for writing and testing Python scripts <br>
Jupyter Notebook - Used for interactive data analysis and debugging <br>
PostgreSQL - Database for storing structured data, Power BI - Visualization and reporting <br>
GitHub - Version control and collaboration

## 6. Data Visualization

If you want to access the dashboard, please use [this link](https://app.powerbi.com/groups/me/reports/f98d9180-dd10-4aaf-95f5-2a1406bb89d9/a9aa4c6c7d7a28da0052?experience=power-bi).
