"""
Implementation of the API endpoint for loading and parsing csv files using FastAPI.
"""

import os
from fastapi import FastAPI, File, UploadFile, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker
import uvicorn
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import base

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./globant_de.db")

# Define the datamodel
engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False, "timeout": 30}
)
metadata = MetaData()

# Crurently the script is loading the data directly into the bronze layer
# TODO: #4 Implement separation of concerns between the layers by using functions from base.py
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# why?
app = FastAPI()


def parse_csv(file_content: str) -> list[list[str]]:
    """
    A dummy CSV parser function that simulates parsing CSV content.
    """
    # Dummy parser function
    lines = file_content.splitlines()
    # we dont expect any headers on this data when inspecting it directly, however a sanity check is added:
    try:
        # try to convert the first value to int. If it fails, we assume headers are present.
        int(lines[0].split(",")[0])
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid CSV format: First column should be integer IDs. No headers are expected",
        )
    # TODO: Eventually manage the case where headers are present
    data = [line.split(",") for line in lines]
    return data


@app.post("/uploadfile/")
def create_upload_file(file: UploadFile = File(...)):
    with file.file as f:
        content = f.read()
    decoded_content = content.decode("utf-8")
    lines = decoded_content.splitlines()
    # validate the size of the file. Reject files with more than 1,000 lines
    #TODO: handlle to divide into chunks and insert them
    BATCH_LIMIT = 1000
    if len(lines) > BATCH_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"File too large: Maximum {BATCH_LIMIT} lines allowed",
        )
    # parse the file and insert data into the database:
    rows = parse_csv(decoded_content)

    if file.filename == "departments.csv":
        payload = [{"id": int(r[0]), "department": r[1]} for r in rows]
        base.insert_department_many(payload, batch_size=1000)
    if file.filename == "departments.csv":
        expected_columns = 2
        for idx, r in enumerate(rows, start=1):
            if len(r) < expected_columns:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid row {idx} in departments.csv: expected at least "
                        f"{expected_columns} columns, got {len(r)}"
                    ),
                )
        payload = [{"id": int(r[0]), "department": r[1]} for r in rows]
        base.insert_department_many(payload, batch_size=1000)

    elif file.filename == "jobs.csv":
        expected_columns = 2
        for idx, r in enumerate(rows, start=1):
            if len(r) < expected_columns:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid row {idx} in jobs.csv: expected at least "
                        f"{expected_columns} columns, got {len(r)}"
                    ),
                )
        payload = [{"id": int(r[0]), "job": r[1]} for r in rows]
        base.insert_job_many(payload, batch_size=1000)

    elif file.filename == "hired_employees.csv":
        expected_columns = 5
        for idx, r in enumerate(rows, start=1):
            if len(r) < expected_columns:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid row {idx} in hired_employees.csv: expected at least "
                        f"{expected_columns} columns, got {len(r)}"
                    ),
                )
            else:
                try:
                    int(r[0])  # id
                    int(r[3])  # department_id
                    int(r[4])  # job_id
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"Invalid data type in row {idx} of hired_employees.csv: "
                            "id, department_id, and job_id should be integers"
                        ),
                    )
        payload = [
            {
                "id": int(r[0]),            
                "name": r[1],
                "datetime": r[2],
                "department_id": int(r[3]),
                "job_id": int(r[4]),
            }
            for r in rows
        ]
        base.insert_hired_employees_many(payload, batch_size=1000)
    return {"filename": file.filename, "status": "uploaded and data inserted"}


@app.post("/create/job/")
def create_job(id: int, job: str):
    base.insert_job(id, job)
    return {"status": "job created"}


@app.post("/create/department/")
def create_department(id: int, department: str):
    base.insert_department(id, department)
    return {"status": "department created"}


@app.post("/create/hired_employee/")
def create_hired_employee(
    id: int, name: str, datetime: str, department_id: int, job_id: int
):
    base.insert_hired_employee(id, name, datetime, department_id, job_id)
    return {"status": "hired employee created"}


# TODO: Implement get functions


@app.get("/analytics/employees_by_quarter/")
def get_employees_by_quarter() -> dict:
    result = base.query_hired_employees_by_quarter()

    return {"status": "success", "data": result}


@app.get("/analytics/departments_above_mean_hires/")
def get_departments_above_mean() -> dict:

    result = base.query_departments_above_mean_hires()
    return {"status": "success", "data": result}


if __name__ == "__main__":
    uvicorn.run(app)
