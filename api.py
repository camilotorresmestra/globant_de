'''
Implementation of the API endpoint for loading and parsing csv files using FastAPI.
'''
from fastapi import FastAPI, File, UploadFile, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker
import uvicorn
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

DATABASE_URL = "sqlite:///./globant_de.db"

#Define the datamodel
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30}
)
metadata = MetaData()

#Crurently the script is loading the data directly into the bronze layer
#TODO: Implement separation of concerns between the layers by using functions from base.py
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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

def insert_department(id: int, department: str):
    ins = departments.insert().values(id=id, department=department)
    smt = sqlite_insert(departments).values(id=id, department=department).on_conflict_do_nothing(index_elements=['id'])
    with SessionLocal() as session:
        session.execute(ins)
        session.execute(smt)
        session.commit()

def insert_job(id: int, job: str):
    stmt = (
        sqlite_insert(jobs)
        .values(id=id, job=job)
        .on_conflict_do_nothing(index_elements=["id"])
    )
    with SessionLocal() as session:
        session.execute(stmt)
        session.commit()

def insert_hired_employee(id: int, name: str, datetime: str, department_id: int, job_id: int):
    ins = hired_employees.insert().values(id=id, name=name, datetime=datetime, department_id=department_id, job_id=job_id)
    smt = sqlite_insert(hired_employees).values(id=id, name=name, datetime=datetime, department_id=department_id, job_id=job_id).on_conflict_do_nothing(index_elements=['id'])
    with SessionLocal() as session:
        session.execute(ins)
        session.execute(smt)
        session.commit()



#why?
app = FastAPI()
def parse_csv(file_content: str) -> list[list[str]]:
    '''
    A dummy CSV parser function that simulates parsing CSV content.
    '''
    # Dummy parser function
    lines = file_content.splitlines()
    # we dont expect any headers on this data when inspecting it directly, however a sanity check is added:
    try:
        # try to convert the first value to int. If it fails, we assume headers are present.
        int(lines[0].split(",")[0]) 
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid CSV format: First column should be integer IDs. No headers are expected")
    #TODO: Eventually manage the case where headers are present
    data = [line.split(",") for line in lines]
    return data


app = FastAPI()

@app.post("/uploadfile/")
def create_upload_file(file: UploadFile = File(...)):
    with file.file as f:
        content = f.read()
    decoded_content = content.decode("utf-8")
    lines = decoded_content.splitlines()
    #validate the size of the file. Reject files with more than 1,000 lines
    BATCH_LIMIT = 1000
    if len(lines) > BATCH_LIMIT:
        raise HTTPException(status_code=400, detail=f"File too large: Maximum {BATCH_LIMIT} lines allowed")
    #parse the file and insert data into the database:
    for line in lines:
        fields = line.split(",")
        if file.filename == "departments.csv":
            insert_department(int(fields[0]), fields[1])
        elif file.filename == "jobs.csv":
            insert_job(int(fields[0]), fields[1])
        elif file.filename == "hired_employees.csv":
            insert_hired_employee(int(fields[0]), fields[1], fields[2], fields[3], fields[4])
    return {"filename": file.filename, "status": "uploaded and data inserted"}

@app.post("/create/job/")
def create_job(id: int, job: str):
    insert_job(id, job)
    return {"status": "job created"}

@app.post("/create/department/")
def create_department(id: int, department: str):
    insert_department(id, department)
    return {"status": "department created"}

@app.post("/create/hired_employee/")
def create_hired_employee(id: int, name: str, datetime: str, department_id: int, job_id: int):
    insert_hired_employee(id, name, datetime, department_id, job_id)
    return {"status": "hired employee created"}

#TODO: Implement get functions



#TODO: Implement Get analytics endpoints




if __name__ == "__main__":
    uvicorn.run(app)