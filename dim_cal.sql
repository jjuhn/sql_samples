-- =============================================================================
-- Sunday–Saturday weeks, Amazon-style week_id (YYYYWW on week start)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_calendar (
    date_key                        INTEGER       PRIMARY KEY,
    full_date                       DATE          NOT NULL,
    year                            INTEGER,
    month                           INTEGER,
    day                             INTEGER,
    day_name                        VARCHAR(20),
    weekday                         INTEGER,          -- 0=Sunday ... 6=Saturday
    is_weekend                      BOOLEAN,

    week_id                         INTEGER,
    week_start_date                 DATE,
    week_end_date                   DATE,

    last_1_week_id                  INTEGER,
    last_1_week_start_date          DATE,
    last_1_week_end_date            DATE,

    last_4_weeks_start_week_id      INTEGER,
    last_4_weeks_start_date         DATE,
    last_4_weeks_end_date           DATE,

    last_month_start_date           DATE,
    last_month_end_date             DATE,

    last_year_week_id               INTEGER,
    last_year_start_date            DATE,
    last_year_end_date              DATE
);

-- Populate (safe to re-run)
INSERT INTO dim_calendar (
    date_key, full_date, year, month, day, day_name, weekday, is_weekend,
    week_id, week_start_date, week_end_date,
    last_1_week_id, last_1_week_start_date, last_1_week_end_date,
    last_4_weeks_start_week_id, last_4_weeks_start_date, last_4_weeks_end_date,
    last_month_start_date, last_month_end_date,
    last_year_week_id, last_year_start_date, last_year_end_date
)
WITH
-- 1. Generate dates
dates AS (
    SELECT d::date AS full_date
    FROM generate_series('2020-01-01'::date, '2035-12-31'::date, '1 day'::interval) gs(d)
),

-- 2. Daily + week boundaries (Sunday start)
base AS (
    SELECT
        full_date,
        EXTRACT(YEAR  FROM full_date)::int          AS year,
        EXTRACT(MONTH FROM full_date)::int          AS month,
        EXTRACT(DAY   FROM full_date)::int          AS day,
        TO_CHAR(full_date, 'Day')                   AS day_name,
        EXTRACT(DOW   FROM full_date)::int          AS weekday,

        full_date - (EXTRACT(DOW FROM full_date) * interval '1 day')::interval
                                                    AS week_start_date,

        (full_date - (EXTRACT(DOW FROM full_date) * interval '1 day')::interval) + interval '6 days'
                                                    AS week_end_date
    FROM dates
),

-- 3. Add week_id
daily AS (
    SELECT *,
           (EXTRACT(YEAR FROM week_start_date)::int * 100 +
            (FLOOR((EXTRACT(DOY FROM week_start_date) - 1) / 7)::int + 1)
           )::int                                   AS week_id
    FROM base
),

-- 4. One row per week + LAGs
weekly AS (
    SELECT DISTINCT ON (week_id)
        week_id,
        week_start_date,
        week_end_date,

        LAG(week_id)           OVER w AS last_1_week_id,
        LAG(week_start_date)   OVER w AS last_1_week_start_date,
        LAG(week_end_date)     OVER w AS last_1_week_end_date,

        LAG(week_id, 4)        OVER w AS last_4_weeks_start_week_id,
        LAG(week_start_date, 4) OVER w AS last_4_weeks_start_date,
        LAG(week_end_date, 1)  OVER w AS last_4_weeks_end_date,

        LAG(week_id, 52)       OVER w AS last_year_week_id,
        LAG(week_start_date, 52) OVER w AS last_year_start_date,
        LAG(week_end_date, 52) OVER w AS last_year_end_date
    FROM daily
    WINDOW w AS (ORDER BY week_start_date)
)

-- 5. Final select + ORDER BY before ON CONFLICT
SELECT
    (d.year * 10000 + d.month * 100 + d.day)::int     AS date_key,
    d.full_date,
    d.year,
    d.month,
    d.day,
    d.day_name,
    d.weekday,
    (d.weekday IN (0,6))                              AS is_weekend,

    d.week_id,
    d.week_start_date,
    d.week_end_date,

    w.last_1_week_id,
    w.last_1_week_start_date,
    w.last_1_week_end_date,

    w.last_4_weeks_start_week_id,
    w.last_4_weeks_start_date,
    w.last_4_weeks_end_date,

    DATE_TRUNC('month', d.full_date - interval '1 month')::date   AS last_month_start_date,
    (DATE_TRUNC('month', d.full_date) - interval '1 day')::date   AS last_month_end_date,

    w.last_year_week_id,
    w.last_year_start_date,
    w.last_year_end_date

FROM daily d
LEFT JOIN weekly w ON d.week_id = w.week_id

ORDER BY d.full_date                  -- Moved here: safe and useful
ON CONFLICT (date_key) DO NOTHING;




