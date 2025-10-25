WITH queue_stats AS (
    SELECT
        job_type,
        jsonb_agg(
            jsonb_build_object(
                'title', statistic,
                'stat_type', type,
                'value', value,
                'priority', priority
            ) ORDER BY priority, statistic
        ) as stats
    FROM (
        -- Priority 1: Current Status
        SELECT
            job_type,
            1 AS priority,
            'Number' AS type,
            'RUNNING_JOBS' AS statistic,
            SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END)::TEXT AS value
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 1, 'Number', 'PENDING_JOBS',
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 1, 'Number', 'FAILED_JOBS',
            SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        -- Priority 2: Health Metrics
        SELECT
            job_type, 2, 'Number', 'ACTIVE_JOBS',
            SUM(CASE WHEN status IN ('Pending', 'Queued', 'Running') THEN 1 ELSE 0 END)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 2, 'Number', 'STALE_RUNNING_JOBS',
            COUNT(*)::TEXT
        FROM apalis.jobs
        WHERE status = 'Running' AND run_at < now() - INTERVAL '1 hour'
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 2, 'Percentage', 'KILL_RATE',
            ROUND(100.0 * SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        -- Priority 3: Recent Activity
        SELECT
            job_type, 3, 'Number', 'JOBS_PAST_HOUR',
            COUNT(*)::TEXT
        FROM apalis.jobs
        WHERE run_at >= now() - INTERVAL '1 hour'
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 3, 'Number', 'JOBS_TODAY',
            COUNT(*)::TEXT
        FROM apalis.jobs
        WHERE run_at::date = CURRENT_DATE
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 3, 'Number', 'KILLED_JOBS_TODAY',
            SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END)::TEXT
        FROM apalis.jobs
        WHERE run_at::date = CURRENT_DATE
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR',
            ROUND(COUNT(*) / 60.0, 2)::TEXT
        FROM apalis.jobs
        WHERE run_at >= now() - INTERVAL '1 hour'
        GROUP BY job_type
        
        UNION ALL
        
        -- Priority 4: Overall Stats
        SELECT
            job_type, 4, 'Number', 'TOTAL_JOBS',
            COUNT(*)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 4, 'Number', 'DONE_JOBS',
            SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 4, 'Number', 'KILLED_JOBS',
            SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 4, 'Percentage', 'SUCCESS_RATE',
            ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        -- Priority 5: Performance
        SELECT
            job_type, 5, 'Decimal', 'AVG_JOB_DURATION_MINS',
            ROUND(AVG(EXTRACT(EPOCH FROM (done_at - run_at)) / 60.0), 2)::TEXT
        FROM apalis.jobs
        WHERE status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL
        GROUP BY job_type
        
        UNION ALL
        
        SELECT
            job_type, 5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS',
            ROUND(MAX(CASE WHEN status = 'Running' THEN EXTRACT(EPOCH FROM now() - run_at) / 60.0 ELSE 0 END), 2)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
        
        UNION ALL
        
        -- Priority 6: Historical
        SELECT
            job_type, 6, 'Number', 'JOBS_PAST_7_DAYS',
            COUNT(*)::TEXT
        FROM apalis.jobs
        WHERE run_at >= now() - INTERVAL '7 days'
        GROUP BY job_type
        
        UNION ALL
        
        -- Priority 8: Timestamps
        SELECT
            job_type, 8, 'Timestamp', 'MOST_RECENT_JOB',
            MAX(run_at)::TEXT
        FROM apalis.jobs
        GROUP BY job_type
    ) subquery
    GROUP BY job_type
),
all_job_types AS (
    SELECT worker_type AS job_type
    FROM apalis.workers
    UNION
    SELECT DISTINCT job_type
    FROM apalis.jobs
)
SELECT
    jt.job_type as name,
    COALESCE(qs.stats, '[]'::jsonb) as stats,
    COALESCE(
        (
            SELECT jsonb_agg(DISTINCT lock_by)
            FROM apalis.jobs
            WHERE job_type = jt.job_type AND lock_by IS NOT NULL
        ),
        '[]'::jsonb
    ) as workers,
    COALESCE(
        (
            SELECT jsonb_agg(daily_count ORDER BY run_date)
            FROM (
                SELECT
                    COUNT(*) as daily_count,
                    run_at::date AS run_date
                FROM apalis.jobs
                WHERE job_type = jt.job_type
                    AND run_at >= now() - INTERVAL '7 days'
                GROUP BY run_at::date
                ORDER BY run_date
            ) t
        ),
        '[]'::jsonb
    ) as activity
FROM all_job_types jt
LEFT JOIN queue_stats qs ON jt.job_type = qs.job_type
ORDER BY name;
