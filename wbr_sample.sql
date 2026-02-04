WITH 
weeks AS (
    SELECT DISTINCT
        week_id,
        week_start_date::date AS week_start,
        week_end_date,
        year,
        last_1_week_start_date,
        last_1_week_end_date,
        last_4_weeks_start_date,
        last_4_weeks_end_date,
        last_year_start_date,
        last_year_end_date
    FROM dim_calendar
    WHERE week_end_date <= CURRENT_DATE
      AND week_id >= 202401   -- adjust start week
    ORDER BY week_id DESC
),

weekly_raw AS (
    SELECT
        dc.week_id,
        COUNT(DISTINCT CASE WHEN e.is_job_start    THEN e.job_id END)          AS jobs_started,
        COUNT(DISTINCT CASE WHEN e.is_job_complete THEN e.job_id END)          AS jobs_completed,
        COUNT(DISTINCT e.account_id)                                           AS active_accounts,
        COALESCE(SUM(CASE WHEN l.agent_type = 'transformation' 
                          THEN l.metered_amount ELSE 0 END), 0)                AS loc_transformed,
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN e.is_job_start THEN e.job_id END) = 0 THEN 0.0
            ELSE ROUND(
                100.0 * COUNT(DISTINCT CASE WHEN e.is_job_complete THEN e.job_id END) /
                COUNT(DISTINCT CASE WHEN e.is_job_start THEN e.job_id END),
                1
            )
        END AS success_rate_pct
    FROM weeks dc
    LEFT JOIN agent_events_clean e ON e.date_key = dc.date_key
    LEFT JOIN agent_loc l ON l.job_id = e.job_id
    GROUP BY dc.week_id
),

with_all_comparisons AS (
    SELECT 
        week_id,
        year,

        -- t1w (current week)
        jobs_started AS t1w_jobs_started,
        jobs_completed AS t1w_jobs_completed,
        active_accounts AS t1w_active_accounts,
        loc_transformed AS t1w_loc_transformed,
        success_rate_pct AS t1w_success_rate_pct,

        -- t1w% (WoW)
        ROUND(100.0 * (jobs_started - LAG(jobs_started) OVER w) / NULLIF(LAG(jobs_started) OVER w, 0), 1) AS t1w_jobs_started_pct,
        ROUND(100.0 * (jobs_completed - LAG(jobs_completed) OVER w) / NULLIF(LAG(jobs_completed) OVER w, 0), 1) AS t1w_jobs_completed_pct,
        ROUND(100.0 * (active_accounts - LAG(active_accounts) OVER w) / NULLIF(LAG(active_accounts) OVER w, 0), 1) AS t1w_active_accounts_pct,
        ROUND(100.0 * (loc_transformed - LAG(loc_transformed) OVER w) / NULLIF(LAG(loc_transformed) OVER w, 0), 1) AS t1w_loc_pct,
        ROUND(success_rate_pct - LAG(success_rate_pct) OVER w, 1) AS t1w_success_delta,

        -- t4w (rolling 4 weeks sum / avg)
        SUM(jobs_started)     OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS t4w_jobs_started,
        SUM(jobs_completed)   OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS t4w_jobs_completed,
        SUM(active_accounts)  OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS t4w_active_accounts,
        SUM(loc_transformed)  OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS t4w_loc_transformed,
        ROUND(AVG(success_rate_pct) OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 1) AS t4w_success_rate_avg_pct,

        -- t4w % (vs previous 4-week period)
        ROUND(100.0 * (
            SUM(jobs_started) OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -
            SUM(jobs_started) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)
        ) / NULLIF(SUM(jobs_started) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING), 0), 1) AS t4w_jobs_started_pct,

        -- similar for other metrics...

        -- ytd (year-to-date cumulative)
        SUM(jobs_started)     OVER (PARTITION BY year ORDER BY week_id) AS ytd_jobs_started,
        SUM(jobs_completed)   OVER (PARTITION BY year ORDER BY week_id) AS ytd_jobs_completed,
        SUM(active_accounts)  OVER (PARTITION BY year ORDER BY week_id) AS ytd_active_accounts,
        SUM(loc_transformed)  OVER (PARTITION BY year ORDER BY week_id) AS ytd_loc_transformed,
        ROUND(AVG(success_rate_pct) OVER (PARTITION BY year ORDER BY week_id), 1) AS ytd_success_rate_avg_pct,

        -- ytd% (vs same period last year)
        -- This requires joining to last year's ytd - more complex - placeholder for now
        NULL::numeric AS ytd_jobs_started_pct   -- can add proper logic if needed

    FROM weekly_raw
    WINDOW w AS (ORDER BY week_id)
)

-- 5. Final long format (UNION per metric)
SELECT 
    week_id,
    week_start,
    'job_started'     AS metric_type,
    t1w_jobs_started  AS t1w,
    t1w_jobs_started_pct AS t1w_pct,
    t4w_jobs_started  AS t4w,
    t4w_jobs_started_pct AS t4w_pct,
    ytd_jobs_started  AS ytd,
    ytd_jobs_started_pct AS ytd_pct
FROM with_all_comparisons

UNION ALL

SELECT 
    week_id,
    week_start,
    'job_completed'   AS metric_type,
    t1w_jobs_completed,
    t1w_jobs_completed_pct,
    t4w_jobs_completed,
    NULL,  -- add t4w_pct if calculated
    ytd_jobs_completed,
    NULL
FROM with_all_comparisons

UNION ALL

SELECT 
    week_id,
    week_start,
    'active_account'  AS metric_type,
    t1w_active_accounts,
    t1w_active_accounts_pct,
    t4w_active_accounts,
    NULL,
    ytd_active_accounts,
    NULL
FROM with_all_comparisons

UNION ALL

SELECT 
    week_id,
    week_start,
    'loc'             AS metric_type,
    t1w_loc_transformed,
    t1w_loc_pct,
    t4w_loc_transformed,
    NULL,
    ytd_loc_transformed,
    NULL
FROM with_all_comparisons

ORDER BY week_id DESC, metric_type;
