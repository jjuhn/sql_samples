
CREATE TABLE IF NOT EXISTS raw_agent_events (
    raw_event_id        BIGSERIAL PRIMARY KEY,
    account_id          VARCHAR(20)   NULL,        -- 12-digit string
    job_id              VARCHAR(50)   NULL,        -- GUID
    agent_id            VARCHAR(50)   NULL,        -- GUID
    job_status          VARCHAR(50)   NULL,        -- create, creating, start, starting, etc.
    agent_type          VARCHAR(50)   NULL,        -- analysis, documentation, transfomration, etc.
    event_timestamp     TIMESTAMP     NOT NULL,
    job_event_name      VARCHAR(100)  NULL,        -- job created, analysis_started, etc.
    error               TEXT          NULL,        -- timeout, etc.
    ingested_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(255)  NULL         -- optional: for traceability (e.g. S3 path)
);


CREATE INDEX IF NOT EXISTS ix_raw_agent_events_timestamp ON raw_agent_events(event_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_agent_events_job_id ON raw_agent_events(job_id);


CREATE TABLE IF NOT EXISTS raw_agent_loc (
    raw_loc_id       BIGSERIAL PRIMARY KEY,
    job_id           VARCHAR(50)    NULL,
    agent_id         VARCHAR(50)    NULL,
    agent_type       VARCHAR(50)    NULL,
    metered_amount   BIGINT         NULL,        -- e.g. 394500005304
    event_timestamp  TIMESTAMP      NOT NULL,
    ingested_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    source_file      VARCHAR(255)   NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_agent_loc_job_id ON raw_agent_loc(job_id);


-- Clean agent_events
INSERT INTO agent_events_clean (
    account_id, job_id, agent_id, job_status, agent_type,
    event_timestamp, event_date, job_event_name, error_message,
    date_key, is_job_start, is_job_complete, is_error
)
WITH cleaned AS (
    SELECT
        account_id,
        job_id,
        agent_id,
        -- Standardize job_status
        CASE LOWER(TRIM(job_status))
            WHEN 'create' THEN 'created'
            WHEN 'creating' THEN 'created'
            WHEN 'start' THEN 'started'
            WHEN 'starting' THEN 'started'
            WHEN 'stopping' THEN 'stopped'
            ELSE LOWER(TRIM(job_status))
        END AS job_status,

        -- Fix common typo
        CASE WHEN LOWER(TRIM(agent_type)) = 'transfomration' 
             THEN 'transformation' 
             ELSE LOWER(TRIM(agent_type)) 
        END AS agent_type,

        event_timestamp,
        event_timestamp::date AS event_date,
        LOWER(TRIM(job_event_name)) AS job_event_name_clean,
        error AS error_message,

        -- Flags
        (LOWER(job_event_name) IN ('job_started', 'job start', 'started'))          AS is_job_start,
        (LOWER(job_event_name) IN ('job_finished', 'job complete', 'completed'))   AS is_job_complete,
        (error IS NOT NULL OR LOWER(error) LIKE '%timeout%')                        AS is_error

    FROM raw_agent_events
)
SELECT
    account_id, job_id, agent_id, job_status, agent_type,
    event_timestamp, event_date, job_event_name_clean,
    error_message,
    (EXTRACT(YEAR FROM event_date)::int * 10000 +
     EXTRACT(MONTH FROM event_date)::int * 100 +
     EXTRACT(DAY FROM event_date)::int) AS date_key,
    is_job_start, is_job_complete, is_error
FROM cleaned
ON CONFLICT (event_timestamp, job_id, agent_id, job_event_name_clean) DO NOTHING;


-- Clean agent_loc (latest per job + agent_type)
INSERT INTO agent_loc (job_id, agent_id, agent_type, metered_amount, event_date, date_key)
SELECT 
    job_id,
    agent_id,
    CASE WHEN LOWER(TRIM(agent_type)) = 'transfomration' THEN 'transformation' ELSE LOWER(TRIM(agent_type)) END,
    metered_amount,
    event_timestamp::date,
    (EXTRACT(YEAR FROM event_timestamp::date)::int * 10000 +
     EXTRACT(MONTH FROM event_timestamp::date)::int * 100 +
     EXTRACT(DAY FROM event_timestamp::date)::int) AS date_key
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY job_id, agent_type ORDER BY event_timestamp DESC) AS rn
    FROM raw_agent_loc
    WHERE metered_amount IS NOT NULL
) t
WHERE rn = 1
ON CONFLICT (job_id, agent_type) DO UPDATE 
SET metered_amount = EXCLUDED.metered_amount,
    event_date = EXCLUDED.event_date,
    date_key = EXCLUDED.date_key;
    
    
    
    
