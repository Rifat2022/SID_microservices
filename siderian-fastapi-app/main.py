"""
main.py
This module contains a FastAPI application for managing employee records
stored in a Microsoft SQL Server database. It provides endpoints to 
create, read, update, and delete employee data.
"""
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List, Optional
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


# FastAPI app
app = FastAPI()

# Database URL
SQLALCHEMY_DATABASE_URL = (
    "mssql+pyodbc://siderianadmin:admin123@localhost/fastapi?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create the SQLAlchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Session maker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for SQLAlchemy models
Base = declarative_base()

# Employee model
class Employee(Base):
    __tablename__ = "Employees"  # Replace with your actual table name

    id = Column(Integer, primary_key=True, index=True)  # Primary key
    name = Column(String(255), index=True)              # Name column
    company_id = Column(Integer)  # Foreign key reference to the companies table
    country = Column(String(255))                        # Country column
    phone = Column(String(50))                           # Phone column
    address = Column(String(255))                        # Address column

# Pydantic model for Employee
class EmployeeCreate(BaseModel):
    """
    Create employee create model.
    """
    name: str
    company_id: Optional[int] = None
    country: str
    phone: str
    address: str

class EmployeeResponse(BaseModel):
    id: int
    name: str
    company_id: Optional[int] = None
    country: str
    phone: str
    address: str

class Config:
        orm_mode = True

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create APIs
# Get all Employees 
@app.get("/employees/", response_model=List[EmployeeResponse])
def get_all_employees(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    """
    Retrieve a list of employees with pagination.

    Args:
        skip (int): The number of records to skip (default is 0).
        limit (int): The maximum number of records to return (default is 10).
        db (Session): The database session dependency.

    Returns:
        List[EmployeeResponse]: A list of employees with their details.
    """
    employees = db.query(Employee).order_by(Employee.id).offset(skip).limit(limit).all()
    return employees

# Get Employee using id 

@app.get("/employees/{employee_id}", response_model=EmployeeResponse)
def get_employee(employee_id: int, db: Session = Depends(get_db)):
    employee = db.query(Employee).filter(Employee.id == employee_id).first()
    if employee is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    return employee

# Create Employee Data
@app.post("/employees/", response_model=EmployeeResponse)
def add_employee(employee: EmployeeCreate, db: Session = Depends(get_db)):
    db_employee = Employee(**employee.dict())  # Create an Employee instance
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee

#update Employee Data
@app.put("/employees/{employee_id}", response_model=EmployeeResponse)
def update_employee(employee_id: int, employee: EmployeeCreate, db: Session = Depends(get_db)):
    db_employee = db.query(Employee).filter(Employee.id == employee_id).first()
    if db_employee is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    db_employee.name = employee.name
    db_employee.company_id = employee.company_id
    db_employee.country = employee.country
    db_employee.phone = employee.phone
    db_employee.address = employee.address

    db.commit()
    db.refresh(db_employee)
    return db_employee
#delete Employee Data 
@app.delete("/employees/{employee_id}", response_model=dict)
def delete_employee(employee_id: int, db: Session = Depends(get_db)):
    db_employee = db.query(Employee).filter(Employee.id == employee_id).first()
    if db_employee is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    
    db.delete(db_employee)
    db.commit()
    return {"detail": "Employee deleted successfully"}

# Endpoint to trigger start of DAG
@app.get("/start-dag")
async def start_dag():
    response = requests.post("http://localhost:8080/api/v1/dags/my_dag/dagRuns",
                             json={"conf": {}, "execution_date": datetime.now().isoformat()},
                             auth=("airflow", "airflow"))
    return {"status": "started"} if response.ok else {"status": "failed"}

# Endpoint to trigger end of DAG
@app.get("/end-dag")
async def end_dag():
    # Triggering the final task to print "Hello, World!"
    return {"status": "DAG Ended. Hello, World!"}
