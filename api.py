'''
Implementation of the API endpoint for loading and parsing csv files using FastAPI.
'''
from fastapi import FastAPI, File, UploadFile, HTTPException
import uvicorn


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
async def create_upload_file(file: UploadFile = File(...)):
    try:
        #check if the uploaded file is a CSV
        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed.")
    except Exception as e:
        return {"error": str(e)}
    file_content = await file.read()
    parsed_data = parse_csv(file_content.decode("utf-8"))
    return {"filename": file.filename, "parsed_data": parsed_data}


if __name__ == "__main__":
    uvicorn.run(app)