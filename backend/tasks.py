from celery import Celery
from .utils import EmailValidator
import pandas as pd
import time
import logging
import os
from email_validator import validate_email as validate_email_lib, EmailNotValidError
import dns.resolver
import re
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Celery with Redis URL from environment variable
celery_app = Celery('tasks', broker=os.getenv('REDIS_URL'), backend=os.getenv('REDIS_URL'))

# Configure Celery
celery_app.conf.update(
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

# [Keep all the existing validation functions: check_disposable_domain, check_role_based_email, etc.]

@celery_app.task(name='backend.tasks.validate_bulk_emails', bind=True)
def validate_bulk_emails(self, file_path: str) -> Dict[str, Any]:
    """Validate multiple email addresses from a file."""
    try:
        # Ensure file exists
        if not os.path.exists(file_path):
            return {
                "status": "failed",
                "error": f"File not found at path: {file_path}",
                "state": "FAILURE"
            }

        # Read the file
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
                if 'email' not in df.columns:
                    return {
                        "status": "failed",
                        "error": "CSV file must contain an 'email' column",
                        "state": "FAILURE"
                    }
                emails = df['email'].tolist()
            else:
                with open(file_path, 'r') as f:
                    emails = [line.strip() for line in f if line.strip()]
        except Exception as e:
            return {
                "status": "failed",
                "error": f"Error reading file: {str(e)}",
                "state": "FAILURE"
            }
        
        if not emails:
            return {
                "status": "failed",
                "error": "No email addresses found in the file",
                "state": "FAILURE"
            }
        
        total_emails = len(emails)
        valid_count = 0
        invalid_count = 0
        results = []
        start_time = time.time()
        
        for i, email in enumerate(emails):
            # Update progress using self instead of task name
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': i,
                    'total': total_emails,
                    'status': 'processing'
                }
            )
            
            # Validate email
            result = validate_email(email)
            results.append(result)
            
            if result['is_valid']:
                valid_count += 1
            else:
                invalid_count += 1
        
        processing_time = time.time() - start_time
        
        # Clean up the temporary file
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.warning(f"Failed to clean up temporary file {file_path}: {str(e)}")
        
        return {
            "status": "completed",
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "total_count": total_emails,
            "processing_time": processing_time,
            "results": results,
            "state": "SUCCESS"
        }
    except Exception as e:
        logger.error(f"Error in validate_bulk_emails: {str(e)}")
        # Clean up the temporary file even if there's an error
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as cleanup_error:
            logger.warning(f"Failed to clean up temporary file {file_path}: {str(cleanup_error)}")
        
        return {
            "status": "failed",
            "error": str(e),
            "state": "FAILURE"
        }
