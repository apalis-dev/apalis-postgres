WITH stats AS (
    SELECT
        job_type,
        COUNT(*) FILTER (WHERE status = 'Running') AS running_jobs,
        COUNT(*) FILTER (WHERE status = 'Pending') AS pending_jobs,
        COUNT(*) FILTER (WHERE status = 'Failed') AS failed_jobs,
        COUNT(*) FILTER (WHERE status = 'Done') AS done_jobs,
        COUNT(*) FILTER (WHERE status = 'Killed') AS killed_jobs,
        COUNT(*) FILTER (WHERE status IN ('Pending', 'Queued', 'Running')) AS active_jobs,
        SUM(CASE WHEN status = 'Running' AND run_at < now() - INTERVAL '1 hour' THEN 1 END) AS stale_running_jobs,
        SUM(CASE WHEN run_at >= now() - INTERVAL '1 hour' THEN 1 END) AS jobs_past_hour,
        SUM(CASE WHEN run_at::date = CURRENT_DATE THEN 1 END) AS jobs_today,
        SUM(CASE WHEN run_at::date = CURRENT_DATE AND status = 'Killed' THEN 1 END) AS killed_jobs_today,
        SUM(CASE WHEN run_at >= now() - INTERVAL '7 days' THEN 1 END) AS jobs_past_7_days,
        ROUND(
            AVG(EXTRACT(EPOCH FROM (done_at - run_at)) / 60.0)
                FILTER (WHERE status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL),
            2
        ) AS avg_job_duration_mins,
        MAX(CASE WHEN status = 'Running' THEN EXTRACT(EPOCH FROM now() - run_at) / 60.0 ELSE 0 END) AS longest_running_job_mins,
        COUNT(*) AS total_jobs,
        MAX(run_at) AS most_recent_job
    FROM apalis.jobs
    GROUP BY job_type
),
activity AS (
    SELECT
        job_type,
        jsonb_agg(daily_count ORDER BY run_date) AS activity
    FROM (
        SELECT
            job_type,
            run_at::date AS run_date,
            COUNT(*) AS daily_count
        FROM apalis.jobs
        WHERE run_at >= now() - INTERVAL '7 days'
        GROUP BY job_type, run_at::date
    ) daily
    GROUP BY job_type
),
lockers AS (
    SELECT
        job_type,
        jsonb_agg(DISTINCT lock_by) AS workers
    FROM apalis.jobs
    WHERE lock_by IS NOT NULL
    GROUP BY job_type
),
queue_stats AS (
    SELECT
        s.job_type,
        jsonb_agg(
            jsonb_build_object(
                'title', t.statistic,
                'stat_type', t.type,
                'value', t.value,
                'priority', t.priority
            ) ORDER BY t.priority, t.statistic
        ) FILTER (WHERE t.value IS NOT NULL) AS stats
    FROM stats s
    CROSS JOIN LATERAL (VALUES
        (1, 'Number', 'RUNNING_JOBS', s.running_jobs::text),
        (1, 'Number', 'PENDING_JOBS', s.pending_jobs::text),
        (1, 'Number', 'FAILED_JOBS', s.failed_jobs::text),
        (2, 'Number', 'ACTIVE_JOBS', s.active_jobs::text),
        (2, 'Number', 'STALE_RUNNING_JOBS', s.stale_running_jobs::text),
        (2, 'Percentage', 'KILL_RATE',
            ROUND(100.0 * s.killed_jobs / NULLIF(s.total_jobs, 0), 2)::text),
        (3, 'Number', 'JOBS_PAST_HOUR', s.jobs_past_hour::text),
        (3, 'Number', 'JOBS_TODAY', s.jobs_today::text),
        (3, 'Number', 'KILLED_JOBS_TODAY',
            CASE WHEN s.jobs_today IS NOT NULL THEN COALESCE(s.killed_jobs_today, 0)::text END),
        (3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR',
            CASE WHEN s.jobs_past_hour IS NOT NULL
                THEN ROUND(s.jobs_past_hour / 60.0, 2)::text END),
        (4, 'Number', 'TOTAL_JOBS', s.total_jobs::text),
        (4, 'Number', 'DONE_JOBS', s.done_jobs::text),
        (4, 'Number', 'KILLED_JOBS', s.killed_jobs::text),
        (4, 'Percentage', 'SUCCESS_RATE',
            ROUND(100.0 * s.done_jobs / NULLIF(s.total_jobs, 0), 2)::text),
        (5, 'Decimal', 'AVG_JOB_DURATION_MINS', s.avg_job_duration_mins::text),
        (5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS', ROUND(s.longest_running_job_mins, 2)::text),
        (6, 'Number', 'JOBS_PAST_7_DAYS', s.jobs_past_7_days::text),
        (8, 'Timestamp', 'MOST_RECENT_JOB', s.most_recent_job::text)
    ) AS t(priority, type, statistic, value)
    GROUP BY s.job_type
),
all_job_types AS (
    SELECT worker_type AS job_type
    FROM apalis.workers
    UNION
    SELECT job_type
    FROM stats
)
SELECT
    jt.job_type as "name!",
    COALESCE(qs.stats, '[]'::jsonb) as "stats!",
    COALESCE(lk.workers, '[]'::jsonb) as "workers!",
    COALESCE(a.activity, '[]'::jsonb) as "activity!"
FROM all_job_types jt
LEFT JOIN queue_stats qs ON jt.job_type = qs.job_type
LEFT JOIN lockers lk ON jt.job_type = lk.job_type
LEFT JOIN activity a ON jt.job_type = a.job_type
ORDER BY jt.job_type;
