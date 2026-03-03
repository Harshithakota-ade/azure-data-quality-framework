Azure Data Quality Framework (Rule-Based Validation Engine)

This project implements a reusable, rule-driven Data Quality (DQ) framework using PySpark and Azure SQL.

The framework validates datasets dynamically based on rule configuration and produces:

Rule validation results

Failed records dataset

Data quality score

Logging and audit trail

Objective

To design a production-ready validation framework that can:

Detect null violations

Validate data types

Check duplicates

Apply threshold rules

Calculate data quality scores

Log validation results

Architecture

Source Data
→ Rule Configuration
→ DQ Engine (PySpark)
→ Passed Records
→ Failed Records
→ SQL Logging Table

Key Features

Rule-driven validation (no hardcoded checks)

Configurable rule types

Reusable engine for any dataset

Quality scoring

Logging framework

Enterprise-style architecture

Rule Types Supported

NOT_NULL

UNIQUE

RANGE_CHECK

LENGTH_CHECK

REGEX_CHECK

THRESHOLD_CHECK
