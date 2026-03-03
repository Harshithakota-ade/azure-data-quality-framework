/* =========================================================
   Data Quality Logging Tables
   Target: Azure SQL DB / Synapse
   ========================================================= */

IF OBJECT_ID('dbo.dq_run_log', 'U') IS NOT NULL
    DROP TABLE dbo.dq_run_log;
GO

CREATE TABLE dbo.dq_run_log (
    dq_run_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    dataset_name      VARCHAR(200) NOT NULL,
    run_id            VARCHAR(100) NULL,          -- pipeline run id / job run id
    start_time_utc    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    end_time_utc      DATETIME2 NULL,
    dq_score          DECIMAL(5,2) NULL,
    status            VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- RUNNING/SUCCESS/FAILED
    error_message     NVARCHAR(4000) NULL
);
GO

IF OBJECT_ID('dbo.dq_rule_results', 'U') IS NOT NULL
    DROP TABLE dbo.dq_rule_results;
GO

CREATE TABLE dbo.dq_rule_results (
    rule_result_id    BIGINT IDENTITY(1,1) PRIMARY KEY,
    dq_run_id         BIGINT NOT NULL,
    rule_id           VARCHAR(50) NOT NULL,
    rule_type         VARCHAR(50) NOT NULL,
    status            VARCHAR(10) NOT NULL,        -- PASS/FAIL
    failed_count      BIGINT NOT NULL,
    total_count       BIGINT NOT NULL,
    severity          VARCHAR(10) NOT NULL,
    description       NVARCHAR(500) NULL,
    details           NVARCHAR(2000) NULL,
    created_at_utc    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_dq_rule_results_run FOREIGN KEY (dq_run_id) REFERENCES dbo.dq_run_log(dq_run_id)
);
GO

CREATE INDEX IX_dq_rule_results_run
ON dbo.dq_rule_results(dq_run_id, status);
GO
