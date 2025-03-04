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
import shutil

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
    task_default_queue='celery',
    task_namespace='backend.tasks',
    task_track_started=True,
    task_time_limit=3600,  # 1 hour timeout
    worker_prefetch_multiplier=1,  # Prevent worker from prefetching too many tasks
    task_acks_late=True,  # Only acknowledge tasks after they're completed
    task_reject_on_worker_lost=True  # Requeue tasks if worker dies
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
        # Create a temporary file in the uploads directory with unique name
        temp_file_path = os.path.join(UPLOAD_DIR, f"temp_{int(time.time())}_{file.filename}")
        
        # Save the uploaded file
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Start the validation task
        task = validate_bulk_emails.delay(temp_file_path)
        
        return {
            "task_id": task.id,
            "status": "accepted",
            "message": "File uploaded and validation started"
        }
    except Exception as e:
        logger.error(f"Error in upload_file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/results/{task_id}")
async def get_results(task_id: str):
    try:
        task_result = AsyncResult(task_id)

        if task_result is None:
            return {
                "status": "failed",
                "error": "Task not found",
                "state": "NOT_FOUND"
            }
        
        # Handle task failure
        if task_result.state == "FAILURE":
            return {
                "status": "failed",
                "error": str(task_result.result),
                "state": "FAILURE"
            }
        
        # Handle task success
        if task_result.state == "SUCCESS":
            result = task_result.get()
            if result is None:
                return {
                    "status": "failed",
                    "error": "Task result is empty",
                    "state": "SUCCESS"
                }
            return {
                **result,
                "state": "SUCCESS"
            }

        # Task is still in queue
        if task_result.state == "PENDING":
            return {
                "status": "waiting",
                "message": "Task is still in queue. Try again later.",
                "state": "PENDING"
            }

        # Task has started processing
        if task_result.state == "STARTED":
            return {
                "status": "processing",
                "message": "Task is in progress",
                "state": "STARTED"
            }

        # Task is in progress with progress info
        if task_result.info:
            return {
                "status": "processing",
                "current": task_result.info.get("current", 0),
                "total": task_result.info.get("total", 0),
                "state": task_result.state
            }
        
        # Default processing state
        return {
            "status": "processing",
            "message": "Task is in progress",
            "state": task_result.state
        }

    except Exception as e:
        logger.error(f"Error in get_results: {str(e)}")
        return {
            "status": "failed",
            "error": f"Error getting results: {str(e)}",
            "state": "ERROR"
        }

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
    try:
        celery_status = "running" if celery_app.control.inspect().active() else "not running"
        return {
            "status": "healthy",
            "services": {
                "api": "running",
                "celery": celery_status
            }
        }
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return {
            "status": "unhealthy",
            "services": {
                "api": "running",
                "celery": "error"
            },
            "error": str(e)
        }
