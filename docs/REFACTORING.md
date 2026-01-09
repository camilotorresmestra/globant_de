# base.py Refactoring Documentation

## Overview
The `base.py` module has been refactored from a procedural to an object-oriented design, making it more modular, maintainable, and testable.

## New Object-Oriented Structure

### Classes

#### 1. DatabaseManager
Manages database connection, engine, metadata, and table definitions.

```python
from base import DatabaseManager

# Create a database manager instance
db_manager = DatabaseManager()

# Or with a custom database URL
db_manager = DatabaseManager(database_url="sqlite:///./custom.db")
```

#### 2. Repository (Base Class)
Provides common CRUD operations for all repository classes.

Methods:
- `insert_one(**values)`: Insert a single record
- `get_all()`: Get all records from the table
- `insert_many(items, batch_size=1000)`: Insert multiple records with batch processing

#### 3. DepartmentRepository
Repository for department operations.

```python
from base import DatabaseManager, DepartmentRepository

db_manager = DatabaseManager()
dept_repo = DepartmentRepository(db_manager)

# Insert a single department
dept_repo.insert_one(id=1, department="Engineering")

# Insert multiple departments
departments = [
    {"id": 1, "department": "Engineering"},
    {"id": 2, "department": "Marketing"}
]
dept_repo.insert_many(departments)

# Get all departments
all_depts = dept_repo.get_all()
```

#### 4. JobRepository
Repository for job operations.

```python
from base import DatabaseManager, JobRepository

db_manager = DatabaseManager()
job_repo = JobRepository(db_manager)

# Insert a single job
job_repo.insert_one(id=1, job="Software Engineer")

# Insert multiple jobs
jobs = [
    {"id": 1, "job": "Software Engineer"},
    {"id": 2, "job": "Data Analyst"}
]
job_repo.insert_many(jobs)

# Get all jobs
all_jobs = job_repo.get_all()
```

#### 5. HiredEmployeeRepository
Repository for hired employee operations.

```python
from base import DatabaseManager, HiredEmployeeRepository

db_manager = DatabaseManager()
emp_repo = HiredEmployeeRepository(db_manager)

# Insert a single employee
emp_repo.insert_one(
    id=1, 
    name="John Doe", 
    datetime="2021-01-15T10:30:00", 
    department_id=1, 
    job_id=1
)

# Insert multiple employees
employees = [
    {
        "id": 1,
        "name": "John Doe",
        "datetime": "2021-01-15T10:30:00",
        "department_id": 1,
        "job_id": 1
    }
]
emp_repo.insert_many(employees)
```

#### 6. AnalyticsRepository
Contains analytics query methods.

```python
from base import DatabaseManager, AnalyticsRepository

db_manager = DatabaseManager()
analytics_repo = AnalyticsRepository(db_manager)

# Query employees by quarter
quarterly_data = analytics_repo.query_hired_employees_by_quarter()

# Query departments above mean hires
above_mean = analytics_repo.query_departments_above_mean_hires()
```

## Backward Compatibility

All existing function interfaces are maintained for backward compatibility:

```python
import base

# Old procedural functions still work
base.insert_department(1, "Engineering")
base.insert_job(1, "Software Engineer")
base.insert_hired_employee(1, "John Doe", "2021-01-15", 1, 1)

departments = base.get_departments()
jobs = base.get_jobs()

# Batch insert functions
base.insert_department_many([{"id": 1, "department": "Engineering"}])
base.insert_job_many([{"id": 1, "job": "Software Engineer"}])
base.insert_hired_employees_many([{
    "id": 1,
    "name": "John Doe",
    "datetime": "2021-01-15",
    "department_id": 1,
    "job_id": 1
}])

# Analytics functions
quarterly_data = base.query_hired_employees_by_quarter()
above_mean = base.query_departments_above_mean_hires()
```

## Key Improvements

1. **Eliminated Code Duplication**: The three `insert_*_many` functions are now a single `insert_many` method in the `Repository` base class.

2. **Encapsulation**: Database connection logic is encapsulated in `DatabaseManager`.

3. **Repository Pattern**: Data access is organized using the repository pattern, making it easier to understand and maintain.

4. **Testability**: Can now easily inject different database URLs for testing:
   ```python
   test_db = DatabaseManager(database_url="sqlite:///:memory:")
   test_dept_repo = DepartmentRepository(test_db)
   ```

5. **Modularity**: Can create multiple `DatabaseManager` instances for different databases or contexts.

6. **Separation of Concerns**: Each class has a single, well-defined responsibility.

## Migration Guide

### From Procedural to OOP (Optional)

If you want to use the new OOP structure directly in your code:

**Before:**
```python
import base
base.insert_department_many(departments_list)
```

**After (Optional):**
```python
from base import DatabaseManager, DepartmentRepository

db_manager = DatabaseManager()
dept_repo = DepartmentRepository(db_manager)
dept_repo.insert_many(departments_list)
```

**Note**: The procedural functions still work, so migration is optional. The OOP structure is available for new code or gradual refactoring.
