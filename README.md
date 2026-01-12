# SQL Data Cleaning – Global Layoffs Dataset

This project demonstrates a practical SQL-based data cleaning workflow using a public Kaggle dataset on global company layoffs.

## Dataset
- Source: Kaggle – Global Layoffs (2022)
- Schema preserved to maintain reproducibility

## What I Did
- Preserved raw data and cleaned using staging tables
- Identified and removed duplicate layoff records using `ROW_NUMBER()`
- Standardized company, industry, country, and date fields
- Handled NULL values logically
- Removed records with no usable layoff information

## Tools & Concepts
- SQL (MySQL-compatible)
- Window functions
- Data standardization
- Date transformation
- Data quality validation

## Outcome
The final dataset is clean, consistent, and ready for exploratory data analysis or reporting.
