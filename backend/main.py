from fastapi import FastAPI, UploadFile, File, Form, HTTPException
import pandas as pd
import aiofiles
import os
from celery.result import AsyncResult
from .tasks import validate_bulk_emails, celery_app, validate_email
from fastapi.responses import JSONResponse
from .utils import email_validator, EmailValidator
from fastapi.middleware.cors import CORSMiddleware
import time
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from celery import Celery
from typing import Dict, Any, List
import tempfile
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_validator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# Use the celery_app from tasks.py instead of creating a new instance
celery = celery_app

# Configure Celery
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Simple rate limiting
class RateLimiter:
    def __init__(self, calls_limit: int = 100, time_window: int = 60):
        self.calls_limit = calls_limit
        self.time_window = time_window
        self.calls = defaultdict(list)

    def is_rate_limited(self, client_ip: str) -> bool:
        now = datetime.now()
        window_start = now - timedelta(seconds=self.time_window)

        # Remove old requests
        self.calls[client_ip] = [t for t in self.calls[client_ip] if t > window_start]

        # Check if the client exceeded the limit
        if len(self.calls[client_ip]) >= self.calls_limit:
            return True
        
        # Add this request to history *only if allowed*
        self.calls[client_ip].append(now)
        return False

rate_limiter = RateLimiter()

# Add rate limiting middleware
@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    client_ip = request.client.host
    if rate_limiter.is_rate_limited(client_ip):
        return JSONResponse(
            status_code=429,
            content={"error": "Too many requests. Please try again later."}
        )
    response = await call_next(request)
    return response

# Add better error handling
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global error handler caught: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc)
        }
    )

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

class EmailData(BaseModel):
    email: str

@app.post("/validate/")
async def validate_single_email(email_data: EmailData):
    try:
        result = validate_email(email_data.email)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Create a temporary file in the system's temp directory
        with tempfile.NamedTemporaryFile(delete=False, suffix=file.filename) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        # Start the validation task
        task = validate_bulk_emails.delay(temp_file_path)
        
        return {"task_id": task.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/results/{task_id}")
async def get_results(task_id: str):
    try:
        task_result = AsyncResult(task_id)

        if task_result is None:
            return {"status": "failed", "error": "Task not found"}
        
        if task_result.ready():
            if task_result.successful():
                result = task_result.get()
                if result is None:
                    return {"status": "failed", "error": "Task result is empty"}
                return result
            else:
                return {"status": "failed", "error": str(task_result.result)}

        # Prevent infinite loops by returning an explicit "waiting" state
        if task_result.state == "PENDING":
            return {"status": "waiting", "message": "Task is still in queue. Try again later."}

        if task_result.info:
            return {
                "status": "processing",
                "current": task_result.info.get("current", 0),
                "total": task_result.info.get("total", 0)
            }
        else:
            return {"status": "processing", "message": "Task is in progress."}

    except Exception as e:
        return {"status": "failed", "error": f"Error getting results: {str(e)}"}

@app.post("/validate-single/")
async def validate_single_email(email: str = Form(...)):
    """
    Validate a single email address.
    """
    try:
        logger.info(f"Validating single email: {email}")
        validator = EmailValidator()
        result = validator.validate_email(email)
        return result
    except Exception as e:
        logger.error(f"Error validating single email: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

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
