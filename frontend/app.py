from fastapi import FastAPI, UploadFile, File
import pandas as pd
import aiofiles
import os
from celery.result import AsyncResult
from tasks import validate_bulk_emails, celery_app
from fastapi.responses import JSONResponse
from utils import EmailValidator
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import streamlit as st
import requests
import time

app = FastAPI(
    title="Email Validation API",
    description="API for validating email addresses and checking for disposable domains",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create upload directory if it doesn't exist
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Create a global instance of EmailValidator
email_validator = EmailValidator()

@app.get("/")
async def root():
    return {
        "message": "Welcome to Email Validation API",
        "endpoints": {
            "upload": "/upload/ - Upload CSV file with emails for bulk validation",
            "results": "/results/{task_id} - Get validation results for a task",
            "validate_single": "/validate-single/ - Validate a single email address"
        }
    }

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a CSV file containing email addresses for bulk validation.
    The CSV should have one email per line.
    """
    try:
        if not file.filename.endswith('.csv'):
            return JSONResponse(
                status_code=400,
                content={"error": "File must be a CSV file"}
            )

        file_path = f"{UPLOAD_DIR}/{file.filename}"
        
        async with aiofiles.open(file_path, 'wb') as out_file:
            content = await file.read()
            await out_file.write(content)

        df = pd.read_csv(file_path, header=None, names=["email"])
        email_list = df["email"].tolist()

        # Initial validation
        initial_validation = []
        for email in email_list:
            is_valid, message = email_validator.validate_email(email)
            initial_validation.append({
                "email": email,
                "is_valid": is_valid,
                "message": message,
                "syntax_check": is_valid,
                "format_validation": is_valid,
                "dns_verification": is_valid,
                "mx_record_check": is_valid,
                "disposable_email": not is_valid,
                "role_based_email": False,  # Add logic if needed
                "typo_detection": not is_valid,
                "bounce_risk": not is_valid
            })

        # Only proceed with Celery task if there are valid emails
        valid_emails = [item["email"] for item in initial_validation if item["is_valid"]]
        
        if valid_emails:
            task = validate_bulk_emails.delay(valid_emails)
            return {
                "task_id": task.id,
                "message": "Email validation started!",
                "initial_validation": initial_validation,
                "total_emails": len(email_list),
                "valid_emails": len(valid_emails),
                "invalid_emails": len(email_list) - len(valid_emails)
            }
        else:
            return {
                "message": "No valid emails found in the file",
                "validation_results": initial_validation,
                "total_emails": len(email_list),
                "valid_emails": 0,
                "invalid_emails": len(email_list)
            }

    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Error processing file: {str(e)}"}
        )

@app.get("/results/{task_id}")
def get_results(task_id: str):
    """
    Get the results of a bulk email validation task.
    """
    task = AsyncResult(task_id, app=celery_app)

    if task.state == "PENDING":
        return {"task_id": task_id, "status": "PENDING"}

    if task.state == "FAILURE":
        return {"task_id": task_id, "status": "FAILED", "error": str(task.info)}

    if task.state == "SUCCESS":
        results = task.result
        valid_count = sum(1 for r in results if r["is_valid"])
        invalid_count = len(results) - valid_count
        
        # Add additional validation fields to match Streamlit frontend
        for result in results:
            result.update({
                "syntax_check": result["is_valid"],
                "format_validation": result["is_valid"],
                "dns_verification": result["is_valid"],
                "mx_record_check": result["is_valid"],
                "disposable_email": not result["is_valid"],
                "role_based_email": False,
                "typo_detection": not result["is_valid"],
                "bounce_risk": not result["is_valid"]
            })
        
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "results": results,
            "summary": {
                "total_emails": len(results),
                "valid_emails": valid_count,
                "invalid_emails": invalid_count
            }
        }

    return {"task_id": task_id, "status": task.state}

@app.post("/validate-single/")
async def validate_single_email(email: str):
    """
    Validate a single email address.
    """
    is_valid, message = email_validator.validate_email(email)
    suggestions = email_validator.suggest_corrections(email) if not is_valid else []
    
    return {
        "email": email,
        "is_valid": is_valid,
        "message": message,
        "suggestions": suggestions,
        "syntax_check": is_valid,
        "format_validation": is_valid,
        "dns_verification": is_valid,
        "mx_record_check": is_valid,
        "disposable_email": not is_valid,
        "role_based_email": False,
        "typo_detection": not is_valid,
        "bounce_risk": not is_valid
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify the API is running.
    """
    return {
        "status": "healthy",
        "services": {
            "api": "running",
            "celery": "running" if celery_app.control.inspect().active() else "not running"
        }
    }