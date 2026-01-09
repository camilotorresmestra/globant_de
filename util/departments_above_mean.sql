SELECT
    d.id,
    d.department,
    COUNT(h.id) as total_hired
FROM
    departments d
    JOIN hired_employees h ON d.id = h.department_id -- SQLITE does not have a YEAR function, so we use STRFTIME to extract the year
WHERE
    STRFTIME('%Y', h.datetime) = '2021'
GROUP BY
    d.id,
    d.department
HAVING
    total_hired > (
        SELECT
            AVG(dept_hires)
        FROM
            (
                SELECT
                    COUNT(h2.id) as dept_hires
                FROM
                    departments d2
                    JOIN hired_employees h2 ON d2.id = h2.department_id
                WHERE
                    STRFTIME('%Y', h2.datetime) = '2021'
                GROUP BY
                    d2.id
            )
    )
ORDER BY
    total_hired DESC;