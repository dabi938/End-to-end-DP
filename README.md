# ETL Pipeline for Online Retail & Telegram Data

## 1. Introduction

This project builds an ETL (Extract, Transform, Load) pipeline to integrate Online Retail data from UCI Machine Learning Repository with Telegram messages from multiple channels(3 channels). The goal is to automate data collection, clean and store it in PostgreSQL, and visualize insights using Power BI. The pipeline is designed to enhance decision-making by combining structured Online Retail transactions with unstructured customer interactions from Telegram.

## 2. Data Sources

# Online RetailData (UCI Machine Learning Repository)

# Format: CSV

# Link: https://archive.ics.uci.edu/dataset/352/online+retail

# Fields: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

# Telegram Data

Channels: easybuyethiopia, Ecommerceaddis, jiji_shop_ethiopia
Extracted Fields: Date, Message, User ID

## 3. Data Extraction

# Online Retail Data Extraction

Loaded CSV using pandas (read_csv())
Validated and cleaned before transformation

# Telegram Data Extraction

# Step 1: Go to https://my.telegram.org

# Step 2: Navigate to API Development Tools

# Step 3: Create a new application to get api_id and api_hash

# Step 4: Use these credentials in the Telethon client setup

# elethon to Extracted date, message text, and user ID

## 4. Data Cleaning

# Online RetailData

Dropped missing values (dropna())
Removed duplicates (drop_duplicates())
Converted InvoiceDate to datetime, UnitPrice to float, and CustomerID to int

# Telegram Data

Preserved negative User IDs for groups
Standardized date format (datetime)
Removed duplicate messages
Ensured user_id values fit within PostgreSQL BIGINT limits

## 5. Database Schema

# Rental_Dataset Table

CREATE TABLE Rental_Dataset (
order_id SERIAL PRIMARY KEY,
InvoiceNo VARCHAR(50),
StockCode VARCHAR(20),
Description TEXT,
Quantity INTEGER,
InvoiceDate TIMESTAMP,
UnitPrice DECIMAL(10,2),
CustomerID BIGINT,
Country VARCHAR(100)
);

# telegram_messages Table

CREATE TABLE telegram_messages (
message_id SERIAL PRIMARY KEY,
date TIMESTAMP,
message TEXT,
user_id BIGINT
);

## 6. Data Loading

# Database Connection

Connected using psycopg2
Created tables with appropriate constraints

# Online RetailData Insertion

INSERT INTO Rental_Dataset (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);

# Telegram Data Insertion

INSERT INTO telegram_messages (date, message, user_id)
VALUES (%s, %s, %s);

# 7. Development Tools

# VS Code - Used for writing and testing Python scripts

# Jupyter Notebook - Used for interactive data analysis and debugging

# PostgreSQL - Database for storing structured data

# Power BI - Visualization and reporting

# GitHub - Version control and collaboration

## 8. Data Visualization

# Power BI Dashboards

# Online RetailSales Trends (monthly and yearly patterns)

# Customer Segmentation (purchase behavior analysis)

# Telegram Message Analysis (sentiment & trending topics)
