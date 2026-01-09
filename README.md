# Globant Data Engineer Coding Challenge

Hello! This is the code repository for the soluiton to the Globant Data Engineer coding challenge.

The challenge was to create a data engineering pipeline that can provide Analytics for an internal employee management system, through a REST API.

## Architecture
 
First draft of architecture for the solution: 
![alt text](image.png)

## Tech Stack

- Medallion architecture:
  - Bronze: Raw data ingestion from CSV files
  - Silver: Data cleaning and transformation into Data Model
  - Gold: Aggregated data for analytics
- REST API: FastAPI application to serve the aggregated data
- Python to ingest, validate and clean data
- DBMS: SQLite (Migrate to Postgres in a future iteration)
- Docker for container and environment management

## How to run (Docker)

1. Clone the repository.
2. Build and start with Docker Compose:
   ```bash
   docker-compose up --build
   ```
3. Or use Docker directly:
   ```bash
   docker build -t globant_de .
   docker run --rm -it -v $(pwd)/globant_de.db:/app/globant_de.db -e DATABASE_URL=sqlite:///./globant_de.db -p 8000:8000 globant_de
   ```
4. Access the FastAPI docs at `http://localhost:8000/docs` to interact with the API.
   - On Windows Command Prompt use `%cd%` instead of `$(pwd)` (PowerShell: `${PWD}` or `$(pwd).Path`).
5. The SQLite database is persisted by mounting `globant_de.db` from your working directory. Set `UVICORN_RELOAD=` to disable auto-reload in production deployments.

---------

Developed with 💗 by camilotorresmestra
