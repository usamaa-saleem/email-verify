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

# Initialize Celery
celery_app = Celery('tasks', broker=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
celery_app = Celery('tasks', broker="redis-18132.c8.us-east-1-4.ec2.redns.redis-cloud.com:18132", backend="redis-18132.c8.us-east-1-4.ec2.redns.redis-cloud.com:18132")


# Configure Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

def check_disposable_domain(email: str) -> bool:
    """Check if the email domain is from a disposable email service."""
    disposable_domains = {
        'tempmail.com', 'throwawaymail.com', 'mailinator.com', 'tempmail.net',
        'disposablemail.com', 'tempmailaddress.com', 'tempmail.ninja',
        'tempmail.plus', 'tempmail.website', 'tempmail.ws', 'tempmail.xyz',
        'tempmail.org', 'tempmail.co', 'tempmail.io', 'tempmail.us',
        'tempmail.me', 'tempmail.pro', 'tempmail.site', 'tempmail.tech',
        'tempmail.tools', 'tempmail.world', 'tempmail.zone', 'tempmail.space',
        'tempmail.app', 'tempmail.dev', 'tempmail.cloud', 'tempmail.digital',
        'tempmail.email', 'tempmail.fun', 'tempmail.guru', 'tempmail.live',
        'tempmail.name', 'tempmail.network', 'tempmail.online', 'tempmail.pw',
        'tempmail.services', 'tempmail.systems', 'tempmail.tech', 'tempmail.today',
        'tempmail.top', 'tempmail.work', 'tempmail.world', 'tempmail.wtf',
        'tempmail.xyz', 'tempmail.zone', 'tempmail.plus', 'tempmail.ninja',
        'tempmail.website', 'tempmail.ws', 'tempmail.org', 'tempmail.co',
        'tempmail.io', 'tempmail.us', 'tempmail.me', 'tempmail.pro',
        'tempmail.site', 'tempmail.tech', 'tempmail.tools', 'tempmail.world',
        'tempmail.zone', 'tempmail.space', 'tempmail.app', 'tempmail.dev',
        'tempmail.cloud', 'tempmail.digital', 'tempmail.email', 'tempmail.fun',
        'tempmail.guru', 'tempmail.live', 'tempmail.name', 'tempmail.network',
        'tempmail.online', 'tempmail.pw', 'tempmail.services', 'tempmail.systems',
        'tempmail.tech', 'tempmail.today', 'tempmail.top', 'tempmail.work',
        'tempmail.world', 'tempmail.wtf', 'tempmail.xyz', 'tempmail.zone'
    }
    domain = email.split('@')[1].lower()
    return domain in disposable_domains

def check_role_based_email(email: str) -> bool:
    """Check if the email is a role-based email address."""
    role_based_prefixes = {
        'admin', 'administrator', 'info', 'contact', 'support', 'help',
        'sales', 'marketing', 'billing', 'accounts', 'accounting', 'hr',
        'human.resources', 'careers', 'jobs', 'recruitment', 'recruiting',
        'media', 'press', 'pr', 'public.relations', 'webmaster', 'postmaster',
        'abuse', 'spam', 'security', 'legal', 'compliance', 'privacy',
        'data.protection', 'gdpr', 'dpo', 'noc', 'network.operations',
        'soc', 'security.operations', 'devops', 'operations', 'ops',
        'helpdesk', 'service.desk', 'it.support', 'technical.support',
        'customer.service', 'customer.support', 'customer.care',
        'customerservice', 'customersupport', 'customercare'
    }
    local_part = email.split('@')[0].lower()
    return any(local_part.startswith(prefix) for prefix in role_based_prefixes)

def check_typo_detection(email: str) -> bool:
    """Check for common typos in email addresses."""
    common_typos = {
        'gmail.com': ['gamil.com', 'gmial.com', 'gmal.com', 'gmai.com'],
        'yahoo.com': ['yaho.com', 'yahooo.com', 'yahu.com'],
        'hotmail.com': ['hotmal.com', 'hotmai.com', 'hotmial.com'],
        'outlook.com': ['outlok.com', 'outlock.com', 'outluk.com'],
        'aol.com': ['ao.com', 'aoll.com'],
        'icloud.com': ['icloud.com', 'icloud.net'],
        'protonmail.com': ['protonmal.com', 'protonmai.com'],
        'zoho.com': ['zoho.co', 'zoho.in'],
        'yandex.com': ['yandex.ru', 'yandex.net'],
        'gmx.com': ['gmx.net', 'gmx.de'],
        'live.com': ['live.net', 'live.co.uk'],
        'me.com': ['me.net', 'me.org'],
        'inbox.com': ['inbox.net', 'inbox.org'],
        'fastmail.com': ['fastmail.net', 'fastmail.org'],
        'tutanota.com': ['tutanota.de', 'tutanota.net'],
        'yahoo.co.uk': ['yahoo.uk', 'yahoo.co'],
        'gmail.co.uk': ['gmail.uk', 'gmail.co'],
        'outlook.co.uk': ['outlook.uk', 'outlook.co'],
        'icloud.co.uk': ['icloud.uk', 'icloud.co'],
        'protonmail.ch': ['protonmail.com', 'protonmail.net'],
        'zoho.eu': ['zoho.com', 'zoho.net'],
        'yandex.ua': ['yandex.com', 'yandex.net'],
        'gmx.net': ['gmx.com', 'gmx.org'],
        'live.co.uk': ['live.com', 'live.net'],
        'me.net': ['me.com', 'me.org'],
        'inbox.net': ['inbox.com', 'inbox.org'],
        'fastmail.net': ['fastmail.com', 'fastmail.org'],
        'tutanota.de': ['tutanota.com', 'tutanota.net']
    }
    
    domain = email.split('@')[1].lower()
    return any(domain in typos for typos in common_typos.values())

def check_bounce_risk(email: str) -> str:
    """Check the bounce risk level of the email address."""
    risk_factors = 0
    
    # Check for disposable email
    if check_disposable_domain(email):
        risk_factors += 3
    
    # Check for role-based email
    if check_role_based_email(email):
        risk_factors += 2
    
    # Check for typos
    if check_typo_detection(email):
        risk_factors += 2
    
    # Check domain age (simplified)
    domain = email.split('@')[1]
    try:
        dns.resolver.resolve(domain, 'MX')
    except dns.resolver.NXDOMAIN:
        risk_factors += 3
    except dns.resolver.NoAnswer:
        risk_factors += 2
    except Exception:
        risk_factors += 1
    
    # Determine risk level
    if risk_factors >= 5:
        return 'high'
    elif risk_factors >= 3:
        return 'medium'
    else:
        return 'low'

def validate_email(email: str) -> Dict[str, Any]:
    """Validate a single email address."""
    try:
        # Basic email validation
        validation = validate_email_lib(email)
        email = validation.normalized
        
        # Additional checks
        disposable = check_disposable_domain(email)
        role_based = check_role_based_email(email)
        typo = check_typo_detection(email)
        bounce_risk = check_bounce_risk(email)
        
        # Check DNS and MX records
        domain = email.split('@')[1]
        try:
            dns.resolver.resolve(domain, 'MX')
            dns_check = True
            mx_check = True
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            dns_check = False
            mx_check = False
        
        return {
            "email": email,
            "is_valid": True,
            "syntax_check": True,
            "format_validation": True,
            "dns_verification": dns_check,
            "mx_record_check": mx_check,
            "disposable_email": disposable,
            "role_based_email": role_based,
            "typo_detection": typo,
            "bounce_risk": bounce_risk,
            "message": "Email is valid"
        }
    except EmailNotValidError as e:
        # Generate detailed error message
        error_messages = []
        if "The email address is not valid" in str(e):
            error_messages.append("Invalid email format")
        if "The domain name" in str(e):
            error_messages.append("Invalid domain name")
        if "The MX record" in str(e):
            error_messages.append("No valid MX record found")
        
        # Add additional checks
        if check_disposable_domain(email):
            error_messages.append("Disposable email domain detected")
        if check_role_based_email(email):
            error_messages.append("Role-based email detected")
        if check_typo_detection(email):
            error_messages.append("Possible typo in domain name")
        
        error_message = " | ".join(error_messages) if error_messages else str(e)
        
        return {
            "email": email,
            "is_valid": False,
            "syntax_check": False,
            "format_validation": False,
            "dns_verification": False,
            "mx_record_check": False,
            "disposable_email": check_disposable_domain(email),
            "role_based_email": check_role_based_email(email),
            "typo_detection": check_typo_detection(email),
            "bounce_risk": "high",
            "message": error_message
        }

@celery_app.task
def validate_bulk_emails(file_path: str) -> Dict[str, Any]:
    """Validate multiple email addresses from a file."""
    try:
        # Read the file
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            emails = df['email'].tolist()
        else:
            with open(file_path, 'r') as f:
                emails = [line.strip() for line in f if line.strip()]
        
        total_emails = len(emails)
        valid_count = 0
        invalid_count = 0
        results = []
        start_time = time.time()
        
        for i, email in enumerate(emails):
            # Update progress
            validate_bulk_emails.update_state(
                state='PROGRESS',
                meta={'current': i, 'total': total_emails}
            )
            
            # Validate email
            result = validate_email(email)
            results.append(result)
            
            if result['is_valid']:
                valid_count += 1
            else:
                invalid_count += 1
        
        processing_time = time.time() - start_time
        
        return {
            "status": "completed",
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "total_count": total_emails,
            "processing_time": processing_time,
            "results": results
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }
