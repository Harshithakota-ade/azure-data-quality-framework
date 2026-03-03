# Architecture - Azure Data Quality Framework

## Goal
Validate datasets using rule configuration and produce:
- Rule result metrics (PASS/FAIL, failed count, severity)
- Failed records dataset (for quarantine)
- DQ score (simple severity-weighted scoring)
- SQL logging for audit and monitoring

## High-Level Flow
Source Data (ADLS/Delta/Table)
→ Load rules from JSON / Control table
→ PySpark DQ Engine applies rules
→ Output:
   - Passed records (clean zone)
   - Failed records (quarantine zone)
   - Rule results (metrics)
→ Persist:
   - DQ run log to Azure SQL
   - Rule results to Azure SQL
→ Optional alerting (email/Teams) based on score or failures
