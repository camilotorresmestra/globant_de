"""The basic database model using SQLAlchemy for a SQLite database."""

from __future__ import annotations

import os

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    text,
    DateTime,
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./globant_de.db")

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
)
metadata = MetaData()
connection = engine.connect()

departments = Table(
    "departments",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("department", String),
)

jobs = Table(
    "jobs", metadata, Column("id", Integer, primary_key=True), Column("job", String)
)

hired_employees = Table(
    "hired_employees",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("datetime", String),
    Column("department_id", Integer),
    Column("job_id", Integer),
)

metadata.create_all(
    engine,
)


# TODO: Implement error and exception handling
def insert_department(id: int, department: str):
    ins = departments.insert().values(id=id, department=department)
    with connection.begin():
        connection.execute(ins)


def insert_job(id: int, job: str):
    ins = jobs.insert().values(id=id, job=job)
    with connection.begin():
        connection.execute(ins)


def insert_hired_employee(
    id: int, name: str, datetime: str, department_id: int, job_id: int
):
    ins = hired_employees.insert().values(
        id=id, name=name, datetime=datetime, department_id=department_id, job_id=job_id
    )
    with connection.begin():
        connection.execute(ins)


def get_departments():
    sel = departments.select()
    with connection.begin():
        result = connection.execute(sel)
    return result.fetchall()


def get_jobs():
    sel = jobs.select()
    with connection.begin():
        result = connection.execute(sel)
    return result.fetchall()


# Requirement 1
# Number of employees hired for each job and department in 2021 divided by quarter. The
# table must be ordered alphabetically by department and job.


def query_hired_employees_by_quarter() -> list[dict]:
    query = text("""
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
                """
    )
    with connection.begin():
        result = connection.execute(query)
        rows = result.mappings().all()
    return [dict(row) for row in rows] #keeps the name of the columns
# Requirement 2:
# List of ids, name and number of employees hired of each department that hired more
# employees than the mean of employees hired in 2021 for all the departments, ordered
# by the number of employees hired (descending)

def query_departments_above_mean_hires():
    query = text("""
                SELECT d.id, d.department, COUNT(h.id) as total_hired
                FROM departments d
                JOIN hired_employees h ON d.id = h.department_id
                -- SQLITE does not have a YEAR function, so we use STRFTIME to extract the year
                WHERE STRFTIME('%Y', h.datetime) = '2021'
                GROUP BY d.id, d.department
                HAVING total_hired > (
                    SELECT AVG(dept_hires) FROM (
                        SELECT COUNT(h2.id) as dept_hires
                        FROM departments d2
                        JOIN hired_employees h2 ON d2.id = h2.department_id
                        WHERE STRFTIME('%Y', h2.datetime) = '2021'
                        GROUP BY d2.id
                    )
                )
                ORDER BY total_hired DESC;
                """
    )
    with connection.begin():
        result = connection.execute(query)
        rows = result.mappings().all()
    return [dict(row) for row in rows]  # keeps the name of the columns


if __name__ == "__main__":
    print(get_jobs())
