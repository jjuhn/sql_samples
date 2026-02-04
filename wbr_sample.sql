WITH 
/* =====================================================
   1. Calendar Spine (Day Level → Supports Weekly Aggregation)
   ===================================================== */
weeks AS (
    SELECT
        date_key,
        week_id,
        CAST(week_start_date AS DATE) AS week_start,
        week_end_date,
        year
    FROM dim_calendar
    WHERE week_end_date < CURRENT_DATE     -- only completed weeks
      AND week_id >= 202401
),

/* =====================================================
   2. Weekly Raw Aggregates
   ===================================================== */
weekly_raw AS (
    SELECT
        w.week_id,
        w.year,

        COUNT(DISTINCT CASE WHEN e.is_job_start THEN e.job_id END) AS jobs_started,
        COUNT(DISTINCT CASE WHEN e.is_job_complete THEN e.job_id END) AS jobs_completed,
        COUNT(DISTINCT e.account_id) AS active_accounts_weekly,

        COALESCE(SUM(
            CASE 
                WHEN l.agent_type = 'transformation' 
                THEN l.metered_amount 
                ELSE 0 
            END
        ),0) AS loc_transformed,

        CASE 
            WHEN COUNT(DISTINCT CASE WHEN e.is_job_start THEN e.job_id END) = 0 
            THEN 0.0
            ELSE ROUND(
                100.0 *
                COUNT(DISTINCT CASE WHEN e.is_job_complete THEN e.job_id END) /
                COUNT(DISTINCT CASE WHEN e.is_job_start THEN e.job_id END),
                1
            )
        END AS success_rate_pct

    FROM weeks w
    LEFT JOIN agent_events_clean e 
        ON e.date_key = w.date_key
    LEFT JOIN agent_loc l 
        ON l.job_id = e.job_id
    GROUP BY 1,2
),

/* =====================================================
   3. Add Time Comparisons
   ===================================================== */
with_comparisons AS (
    SELECT 
        week_id,
        year,

        /* --- T1W values --- */
        jobs_started        AS t1w_jobs_started,
        jobs_completed      AS t1w_jobs_completed,
        active_accounts_weekly AS t1w_active_accounts,
        loc_transformed     AS t1w_loc_transformed,
        success_rate_pct    AS t1w_success_rate_pct,

        /* --- Week over week % --- */
        ROUND(100.0 * (jobs_started - LAG(jobs_started) OVER w)
             / NULLIF(LAG(jobs_started) OVER w,0),1) AS t1w_jobs_started_pct,

        ROUND(100.0 * (jobs_completed - LAG(jobs_completed) OVER w)
             / NULLIF(LAG(jobs_completed) OVER w,0),1) AS t1w_jobs_completed_pct,

        ROUND(100.0 * (active_accounts_weekly - LAG(active_accounts_weekly) OVER w)
             / NULLIF(LAG(active_accounts_weekly) OVER w,0),1) AS t1w_active_accounts_pct,

        ROUND(100.0 * (loc_transformed - LAG(loc_transformed) OVER w)
             / NULLIF(LAG(loc_transformed) OVER w,0),1) AS t1w_loc_pct,

        ROUND(success_rate_pct - LAG(success_rate_pct) OVER w,1) AS t1w_success_delta,

        /* --- Rolling 4 Week --- */
        SUM(jobs_started) OVER (
            ORDER BY week_id 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS t4w_jobs_started,

        SUM(jobs_completed) OVER (
            ORDER BY week_id 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS t4w_jobs_completed,

        SUM(loc_transformed) OVER (
            ORDER BY week_id 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS t4w_loc_transformed,

        ROUND(
            AVG(success_rate_pct) OVER (
                ORDER BY week_id 
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ),1
        ) AS t4w_success_rate_avg_pct,

        /* --- Rolling 4 Week % vs Prior 4 Week --- */
        ROUND(100.0 * (
            SUM(jobs_started) OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -
            SUM(jobs_started) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)
        ) / NULLIF(
            SUM(jobs_started) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING),0
        ),1) AS t4w_jobs_started_pct,

        ROUND(100.0 * (
            SUM(jobs_completed) OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -
            SUM(jobs_completed) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)
        ) / NULLIF(
            SUM(jobs_completed) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING),0
        ),1) AS t4w_jobs_completed_pct,

        ROUND(100.0 * (
            SUM(loc_transformed) OVER (ORDER BY week_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -
            SUM(loc_transformed) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING)
        ) / NULLIF(
            SUM(loc_transformed) OVER (ORDER BY week_id ROWS BETWEEN 7 PRECEDING AND 4 PRECEDING),0
        ),1) AS t4w_loc_pct,

        /* --- YTD --- */
        SUM(jobs_started) OVER (PARTITION BY year ORDER BY week_id) AS ytd_jobs_started,
        SUM(jobs_completed) OVER (PARTITION BY year ORDER BY week_id) AS ytd_jobs_completed,
        SUM(active_accounts_weekly) OVER (PARTITION BY year ORDER BY week_id) AS ytd_active_accounts,
        SUM(loc_transformed) OVER (PARTITION BY year ORDER BY week_id) AS ytd_loc_transformed,

        ROUND(
            AVG(success_rate_pct) OVER (PARTITION BY year ORDER BY week_id),1
        ) AS ytd_success_rate_avg_pct

    FROM weekly_raw
    WINDOW w AS (ORDER BY week_id)
),

/* =====================================================
   4. Convert To Long Reporting Format (Readable Version)
   ===================================================== */
final AS (

SELECT w.week_id, w.week_start, 'job_started' AS metric_type,
       c.t1w_jobs_started AS t1w,
       c.t1w_jobs_started_pct AS t1w_pct,
       c.t4w_jobs_started AS t4w,
       c.t4w_jobs_started_pct AS t4w_pct,
       c.ytd_jobs_started AS ytd,
       NULL::numeric AS ytd_pct
FROM with_comparisons c
JOIN (SELECT DISTINCT week_id, week_start FROM weeks) w
    ON w.week_id = c.week_id

UNION ALL

SELECT w.week_id, w.week_start, 'job_completed',
       c.t1w_jobs_completed,
       c.t1w_jobs_completed_pct,
       c.t4w_jobs_completed,
       c.t4w_jobs_completed_pct,
       c.ytd_jobs_completed,
       NULL::numeric
FROM with_comparisons c
JOIN (SELECT DISTINCT week_id, week_start FROM weeks) w
    ON w.week_id = c.week_id

UNION ALL

SELECT w.week_id, w.week_start, 'active_account',
       c.t1w_active_accounts,
       c.t1w_active_accounts_pct,
       NULL,
       NULL,
       c.ytd_active_accounts,
       NULL::numeric
FROM with_comparisons c
JOIN (SELECT DISTINCT week_id, week_start FROM weeks) w
    ON w.week_id = c.week_id

UNION ALL

SELECT w.week_id, w.week_start, 'loc',
       ROUND(c.t1w_loc_transformed / 1000.0,1),
       c.t1w_loc_pct,
       ROUND(c.t4w_loc_transformed / 1000.0,1),
       c.t4w_loc_pct,
       ROUND(c.ytd_loc_transformed / 1000.0,1),
       NULL::numeric
FROM with_comparisons c
JOIN (SELECT DISTINCT week_id, week_start FROM weeks) w
    ON w.week_id = c.week_id
)

/* =====================================================
   Final Output
   ===================================================== */
SELECT *
FROM final
ORDER BY week_id DESC, metric_type;
