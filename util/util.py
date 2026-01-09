"""
Utility script to load CSV files from a folder and upload them to the database. To mock the ETL process and bypass the API layer.
"""
import os
import csv
from pathlib import Path
from base import insert_department, insert_job, insert_hired_employee


def load_csv_files(folder_path: str):
    """
    Read all CSV files from the given folder and upload them to the database.
    
    Args:
        folder_path: Path to the folder containing CSV files
    """
    folder = Path(folder_path)
    
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Folder does not exist: {folder_path}")
    
    csv_files = list(folder.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return
    
    for csv_file in csv_files:
        print(f"Processing {csv_file.name}...")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                
                for row in reader:
                    if not row:  # Skip empty rows
                        continue
                    
                    if csv_file.name == "departments.csv":
                        insert_department(int(row[0]), row[1])
                    elif csv_file.name == "jobs.csv":
                        insert_job(int(row[0]), row[1])
                    elif csv_file.name == "hired_employees.csv":
                        insert_hired_employee(
                            *row
                        )
            
            print(f"Successfully loaded {csv_file.name}")
        
        except Exception as e:
            print(f"Error processing {csv_file.name}: {str(e)}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python util.py <folder_path>")
        print("Example: python util.py ./data")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    load_csv_files(folder_path)
