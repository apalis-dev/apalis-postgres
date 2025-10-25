SELECT
    1 AS priority,
    'Number' AS type,
    'RUNNING_JOBS' AS statistic,
    SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END)::REAL AS value
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    1, 'Number', 'PENDING_JOBS',
    SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    2, 'Number', 'FAILED_JOBS',
    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    2, 'Number', 'ACTIVE_JOBS',
    SUM(CASE WHEN status IN ('Pending', 'Queued', 'Running') THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    2, 'Number', 'STALE_RUNNING_JOBS',
    COUNT(*)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND status = 'Running'
    AND run_at < now() - INTERVAL '1 hour'

UNION ALL

SELECT
    2, 'Percentage', 'KILL_RATE',
    ROUND(100.0 * SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    3, 'Number', 'JOBS_PAST_HOUR',
    COUNT(*)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '1 hour'

UNION ALL

SELECT
    3, 'Number', 'JOBS_TODAY',
    COUNT(*)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at::date = CURRENT_DATE

UNION ALL

SELECT
    3, 'Number', 'KILLED_JOBS_TODAY',
    SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at::date = CURRENT_DATE

UNION ALL

SELECT
    3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR',
    ROUND(COUNT(*) / 60.0, 2)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '1 hour'

UNION ALL

SELECT
    4, 'Number', 'TOTAL_JOBS',
    COUNT(*)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    4, 'Number', 'DONE_JOBS',
    SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    4, 'Number', 'COMPLETED_JOBS',
    SUM(CASE WHEN status IN ('Done', 'Failed', 'Killed') THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    4, 'Number', 'KILLED_JOBS',
    SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    4, 'Percentage', 'SUCCESS_RATE',
    ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    5, 'Decimal', 'AVG_JOB_DURATION_MINS',
    ROUND(AVG(EXTRACT(EPOCH FROM (done_at - run_at)) / 60.0), 2)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND status IN ('Done', 'Failed', 'Killed')
    AND done_at IS NOT NULL

UNION ALL

SELECT
    5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS',
    ROUND(MAX(CASE WHEN status = 'Running' THEN EXTRACT(EPOCH FROM (now() - run_at)) / 60.0 ELSE 0 END), 2)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    5, 'Number', 'QUEUE_BACKLOG',
    SUM(CASE WHEN status = 'Pending' AND run_at <= now() THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    6, 'Number', 'JOBS_PAST_24_HOURS',
    COUNT(*)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '1 day'

UNION ALL

SELECT
    6, 'Number', 'JOBS_PAST_7_DAYS',
    COUNT(*)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '7 days'

UNION ALL

SELECT
    6, 'Number', 'KILLED_JOBS_PAST_7_DAYS',
    SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '7 days'

UNION ALL

SELECT
    6, 'Percentage', 'SUCCESS_RATE_PAST_24H',
    ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '1 day'

UNION ALL

SELECT
    7, 'Decimal', 'AVG_JOBS_PER_HOUR_PAST_24H',
    ROUND(COUNT(*) / 24.0, 2)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '1 day'

UNION ALL

SELECT
    7, 'Decimal', 'AVG_JOBS_PER_DAY_PAST_7D',
    ROUND(COUNT(*) / 7.0, 2)::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND run_at >= now() - INTERVAL '7 days'

UNION ALL

SELECT
    8, 'Timestamp', 'MOST_RECENT_JOB',
    EXTRACT(EPOCH FROM MAX(run_at))::REAL
FROM apalis.jobs
WHERE job_type = $1

UNION ALL

SELECT
    8, 'Timestamp', 'OLDEST_PENDING_JOB',
    EXTRACT(EPOCH FROM MIN(run_at))::REAL
FROM apalis.jobs
WHERE job_type = $1
    AND status = 'Pending'
    AND run_at <= now()

UNION ALL

SELECT
    8, 'Number', 'PEAK_HOUR_JOBS',
    MAX(hourly_count)::REAL
FROM (
    SELECT COUNT(*) as hourly_count
    FROM apalis.jobs
    WHERE job_type = $1
        AND run_at >= now() - INTERVAL '1 day'
    GROUP BY EXTRACT(HOUR FROM run_at)
) subquery

ORDER BY priority, statistic;
