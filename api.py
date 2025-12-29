'''
Implementation of the API endpoint for loading and parsing csv files using FastAPI.
'''
from fastapi import FastAPI, File, UploadFile, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker
import uvicorn
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

DATABASE_URL = "sqlite:///./globant_de.db"

engine = create_engine(DATABASE_URL)
metadata = MetaData()
connection = engine.connect()

departments = Table(
    'departments', metadata,
    Column('id', Integer, primary_key=True),
    Column('department', String)
)

jobs = Table(
    'jobs', metadata,
    Column('id', Integer, primary_key=True),
    Column('job', String)
)

hired_employees = Table(
    'hired_employees', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('datetime', String),
    Column('department_id', Integer),
    Column('job_id', Integer)
)

metadata.create_all(engine)

# TODO: Implement error and exception handling
def insert_department(id: int, department: str):
    ins = departments.insert().values(id=id, department=department)
    connection.execute(ins)
    connection.commit()
def insert_job(id: int, job: str):
    ins = jobs.insert().values(id=id, job=job)
    connection.execute(ins)
    connection.commit()
def insert_hired_employee(id: int, name: str, datetime: str, department_id: int, job_id: int):
    ins = hired_employees.insert().values(id=id, name=name, datetime=datetime, department_id=department_id, job_id=job_id)
    connection.execute(ins)
    connection.commit()

#Define the datamodel
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#why?
app = FastAPI()
def parse_csv(file_content: str):
    '''
    A dummy CSV parser function that simulates parsing CSV content.
    '''
    # Dummy parser function
    lines = file_content.splitlines()
    headers = lines[0].split(",")
    data = [line.split(",") for line in lines[1:]]
    return {"headers": headers, "data": data}


app = FastAPI()

@app.post("/uploadfile/")
def create_upload_file(file: UploadFile = File(...)):
    with file.file as f:
        content = f.read()
    decoded_content = content.decode("utf-8")
    lines = decoded_content.splitlines()
    for line in lines:
        fields = line.split(",")
        print(fields)
        if file.filename == "departments.csv":
            insert_department(int(fields[0]), fields[1])
        elif file.filename == "jobs.csv":
            insert_job(int(fields[0]), fields[1])
        elif file.filename == "hired_employees.csv":
            insert_hired_employee(int(fields[0]), fields[1], fields[2], int(fields[3]), int(fields[4]))
    return {"filename": file.filename, "status": "uploaded and data inserted"}

@app.post("/create/job/")
def create_job(id: int, job: str):
    insert_job(id, job)
    return {"status": "job created"}

if __name__ == "__main__":
    uvicorn.run(app)