WITH quarterly_employees AS (
    SELECT
        d.department,
        j.job,
        CAST(((CAST(SUBSTR(h.datetime, 6, 2) AS INTEGER) - 1) / 3 + 1) AS TEXT) AS Q,
        COUNT(h.id) AS employees
    FROM
        hired_employees h
        LEFT JOIN departments d ON h.department_id = d.id
        JOIN jobs j ON h.job_id = j.id
    WHERE
        STRFTIME('%Y', h.datetime) = '2021'
    GROUP BY
        d.department,
        j.job,
        Q
)
SELECT
    department,
    job,
    SUM(CASE WHEN Q = '1' THEN employees ELSE 0 END) AS Q1,
    SUM(CASE WHEN Q = '2' THEN employees ELSE 0 END) AS Q2,
    SUM(CASE WHEN Q = '3' THEN employees ELSE 0 END) AS Q3,
    SUM(CASE WHEN Q = '4' THEN employees ELSE 0 END) AS Q4
FROM
    quarterly_employees
GROUP BY
    department,
    job
ORDER BY
    department,
    job;

