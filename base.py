"""The basic database model using SQLAlchemy for a SQLite database."""

from __future__ import annotations

import os
from typing import List, Dict
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    text,
    DateTime,
    Engine,
    Connection,
)


class DatabaseManager:
    """Manages database connection and metadata."""
    
    def __init__(self, database_url: str = None):
        """Initialize database manager with connection URL."""
        self.database_url = database_url or os.getenv("DATABASE_URL", "sqlite:///./globant_de.db")
        self.engine = create_engine(
            self.database_url,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        self.metadata = MetaData()
        self.connection = self.engine.connect()
        self._define_tables()
        self.metadata.create_all(self.engine)
    
    def _define_tables(self):
        """Define database tables."""
        self.departments = Table(
            "departments",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("department", String),
        )
        
        self.jobs = Table(
            "jobs", 
            self.metadata, 
            Column("id", Integer, primary_key=True), 
            Column("job", String)
        )
        
        self.hired_employees = Table(
            "hired_employees",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("datetime", String),
            Column("department_id", Integer),
            Column("job_id", Integer),
        )


class Repository:
    """Base repository class for database operations."""
    
    def __init__(self, db_manager: DatabaseManager, table: Table):
        """Initialize repository with database manager and table."""
        self.db_manager = db_manager
        self.table = table
    
    def insert_one(self, **values):
        """Insert a single record."""
        ins = self.table.insert().values(**values)
        with self.db_manager.connection.begin():
            self.db_manager.connection.execute(ins)
    
    def get_all(self):
        """Get all records from the table."""
        sel = self.table.select()
        with self.db_manager.connection.begin():
            result = self.db_manager.connection.execute(sel)
        return result.fetchall()
    
    def insert_many(self, items: List[Dict], batch_size: int = 1000):
        """Insert multiple records with batch processing and conflict handling."""
        if not items:
            return
        
        stmt = sqlite_insert(self.table).on_conflict_do_nothing(index_elements=["id"])
        with self.db_manager.connection.begin():
            for chunk in self._chunks(items, batch_size):
                self.db_manager.connection.execute(stmt, chunk)
    
    @staticmethod
    def _chunks(items: List[Dict], batch_size: int):
        """Split items into chunks for batch processing."""
        for start_index in range(0, len(items), batch_size):
            yield items[start_index : start_index + batch_size]


class DepartmentRepository(Repository):
    """Repository for department operations."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize department repository."""
        super().__init__(db_manager, db_manager.departments)


class JobRepository(Repository):
    """Repository for job operations."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize job repository."""
        super().__init__(db_manager, db_manager.jobs)


class HiredEmployeeRepository(Repository):
    """Repository for hired employee operations."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize hired employee repository."""
        super().__init__(db_manager, db_manager.hired_employees)


class AnalyticsRepository:
    """Repository for analytics queries."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize analytics repository."""
        self.db_manager = db_manager
    
    def query_hired_employees_by_quarter(self) -> List[Dict]:
        """
        Number of employees hired for each job and department in 2021 divided by quarter.
        The table is ordered alphabetically by department and job.
        """
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
        """)
        with self.db_manager.connection.begin():
            result = self.db_manager.connection.execute(query)
            rows = result.mappings().all()
        return [dict(row) for row in rows]
    
    def query_departments_above_mean_hires(self) -> List[Dict]:
        """
        List of ids, name and number of employees hired of each department that hired more
        employees than the mean of employees hired in 2021 for all the departments, ordered
        by the number of employees hired (descending).
        """
        query = text("""
            SELECT d.id, d.department, COUNT(h.id) as total_hired
            FROM departments d
            JOIN hired_employees h ON d.id = h.department_id
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
        """)
        with self.db_manager.connection.begin():
            result = self.db_manager.connection.execute(query)
            rows = result.mappings().all()
        return [dict(row) for row in rows]


# Create a default instance for backward compatibility
_db_manager = DatabaseManager()
_department_repo = DepartmentRepository(_db_manager)
_job_repo = JobRepository(_db_manager)
_hired_employee_repo = HiredEmployeeRepository(_db_manager)
_analytics_repo = AnalyticsRepository(_db_manager)

# Expose module-level variables for backward compatibility
DATABASE_URL = _db_manager.database_url
engine = _db_manager.engine
metadata = _db_manager.metadata
connection = _db_manager.connection
departments = _db_manager.departments
jobs = _db_manager.jobs
hired_employees = _db_manager.hired_employees


# Backward-compatible function wrappers
def insert_department(id: int, department: str):
    """Insert a single department record."""
    _department_repo.insert_one(id=id, department=department)


def insert_job(id: int, job: str):
    """Insert a single job record."""
    _job_repo.insert_one(id=id, job=job)


def insert_hired_employee(
    id: int, name: str, datetime: str, department_id: int, job_id: int
):
    """Insert a single hired employee record."""
    _hired_employee_repo.insert_one(
        id=id, name=name, datetime=datetime, 
        department_id=department_id, job_id=job_id
    )


def get_departments():
    """Get all departments."""
    return _department_repo.get_all()


def get_jobs():
    """Get all jobs."""
    return _job_repo.get_all()


def insert_department_many(departments_list: list[dict], batch_size: int = 1000):
    """Insert multiple departments with batch processing."""
    _department_repo.insert_many(departments_list, batch_size)


def insert_job_many(jobs_list: list[dict], batch_size: int = 1000):
    """Insert multiple jobs with batch processing."""
    _job_repo.insert_many(jobs_list, batch_size)


def insert_hired_employees_many(hired_employees_list: list[dict], batch_size: int = 1000):
    """Insert multiple hired employees with batch processing."""
    _hired_employee_repo.insert_many(hired_employees_list, batch_size)


def query_hired_employees_by_quarter() -> list[dict]:
    """Query hired employees by quarter for analytics."""
    return _analytics_repo.query_hired_employees_by_quarter()


def query_departments_above_mean_hires() -> list[dict]:
    """Query departments with above-mean hires for analytics."""
    return _analytics_repo.query_departments_above_mean_hires()


if __name__ == "__main__":
    print(get_jobs())
