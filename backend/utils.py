from fastapi import FastAPI, UploadFile, File
import pandas as pd
import aiofiles
import os
from celery.result import AsyncResult
from fastapi.responses import JSONResponse
from typing import List, Set, Dict, Any
import tldextract
from email_validator import validate_email, EmailNotValidError
import re
import dns.resolver
import socket
from datetime import datetime
import logging

app = FastAPI()

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

logger = logging.getLogger(__name__)

class EmailValidator:
    def __init__(self):
        # Common typo patterns in email addresses
        self.typo_patterns = {
            'gmail.com': ['gamil.com', 'gmal.com', 'gmial.com', 'gmaill.com', 'gamil.com', 'gmaill.com'],
            'yahoo.com': ['yaho.com', 'yhaoo.com', 'yahooo.com', 'yaho.com'],
            'hotmail.com': ['hotmal.com', 'hotmai.com', 'hotmial.com', 'hotmaill.com'],
            'outlook.com': ['outlok.com', 'outlock.com', 'outlok.com'],
            'icloud.com': ['icloud.com', 'icloud.com', 'icloud.com'],
            'protonmail.com': ['protonmal.com', 'protonmai.com', 'protonmial.com'],
            'aol.com': ['aol.com', 'aol.com'],
            'live.com': ['live.com', 'live.com'],
            'me.com': ['me.com', 'me.com'],
            'mac.com': ['mac.com', 'mac.com']
        }
        
        # Common disposable email domains
        self.disposable_domains = {
            'tempmail.com', 'tempmail.net', 'tempmail.org',
            'throwawaymail.com', 'throwawaymail.net',
            'mailinator.com', 'mailinator.net',
            'tempmailaddress.com', 'tempmail.plus',
            'tempmail.ninja', 'tempmail.xyz',
            'tempmail.ws', 'tempmail.us',
            'tempmail.co.uk', 'tempmail.de',
            'tempmail.fr', 'tempmail.it',
            'tempmail.es', 'tempmail.ru',
            'tempmail.jp', 'tempmail.cn',
            'tempmail.in', 'tempmail.com.br',
            'tempmail.com.au', 'tempmail.co.nz',
            'tempmail.co.za', 'tempmail.ae',
            'tempmail.sg', 'tempmail.hk',
            'tempmail.tw', 'tempmail.kr',
            'tempmail.vn', 'tempmail.th',
            'tempmail.id', 'tempmail.my',
            'tempmail.ph', 'tempmail.pk',
            'tempmail.bd', 'tempmail.lk',
            'tempmail.mm', 'tempmail.kh',
            'tempmail.la', 'tempmail.bt',
            'tempmail.np', 'tempmail.mv',
            'tempmail.io', 'tempmail.app',
            'tempmail.dev', 'tempmail.tech',
            'tempmail.cloud', 'tempmail.digital',
            'tempmail.space', 'tempmail.world',
            'tempmail.site', 'tempmail.online',
            'tempmail.website', 'tempmail.blog',
            'tempmail.store', 'tempmail.shop',
            'tempmail.biz', 'tempmail.info',
            'tempmail.name', 'tempmail.pro',
            'tempmail.network', 'tempmail.systems',
            'tempmail.services', 'tempmail.solutions',
            'tempmail.agency', 'tempmail.studio',
            'tempmail.media', 'tempmail.design',
            'tempmail.art', 'tempmail.music',
            'tempmail.games', 'tempmail.fun',
            'tempmail.live', 'tempmail.tv',
            'tempmail.news', 'tempmail.tech',
            'tempmail.science', 'tempmail.education',
            'tempmail.school', 'tempmail.university',
            'tempmail.college', 'tempmail.academy',
            'tempmail.institute', 'tempmail.center',
            'tempmail.foundation', 'tempmail.association',
            'tempmail.organization', 'tempmail.society',
            'tempmail.club', 'tempmail.group',
            'tempmail.team', 'tempmail.crew',
            'tempmail.family', 'tempmail.friends',
            'tempmail.community', 'tempmail.social',
            'tempmail.chat', 'tempmail.messaging',
            'tempmail.communication', 'tempmail.contact',
            'tempmail.support', 'tempmail.help',
            'tempmail.assistance', 'tempmail.guide',
            'tempmail.tutorial', 'tempmail.learning',
            'tempmail.knowledge', 'tempmail.wisdom',
            'tempmail.expert', 'tempmail.professional',
            'tempmail.career', 'tempmail.jobs',
            'tempmail.work', 'tempmail.business',
            'tempmail.enterprise', 'tempmail.company',
            'tempmail.corporation', 'tempmail.inc',
            'tempmail.ltd', 'tempmail.llc',
            'tempmail.co', 'tempmail.com',
            'tempmail.net', 'tempmail.org',
            'tempmail.edu', 'tempmail.gov',
            'tempmail.mil', 'tempmail.int'
        }

        # Update role-based patterns to be more specific
        self.role_based_patterns = {
            'admin', 'administrator', 'webmaster', 'postmaster', 'hostmaster',
            'info', 'contact', 'support', 'help', 'helpdesk', 'mail',
            'sales', 'marketing', 'billing', 'accounts', 'accounting',
            'careers', 'jobs', 'recruitment', 'hr', 'human.resources',
            'abuse', 'security', 'spam', 'noc', 'dns', 'whois'
        }

    def check_syntax(self, email: str) -> bool:
        """Check basic email syntax."""
        try:
            # Basic email regex pattern
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            return bool(re.match(pattern, email))
        except Exception:
            return False

    def check_format(self, email: str) -> bool:
        """Check email format and structure."""
        try:
            # Split into local and domain parts
            local_part, domain = email.split('@')
            
            # Check local part length
            if len(local_part) > 64:
                return False
            
            # Check domain length
            if len(domain) > 255:
                return False
            
            # Check for consecutive dots
            if '..' in local_part or '..' in domain:
                return False
            
            # Check domain has at least one dot
            if '.' not in domain:
                return False
            
            return True
        except Exception:
            return False

    def check_dns(self, email: str) -> bool:
        """Check if domain has valid DNS records."""
        try:
            domain = email.split('@')[1]
            dns.resolver.resolve(domain, 'A')
            return True
        except Exception:
            return False

    def check_mx(self, email: str) -> bool:
        """Check if domain has valid MX records."""
        try:
            domain = email.split('@')[1]
            dns.resolver.resolve(domain, 'MX')
            return True
        except Exception:
            return False

    def is_disposable_domain(self, email: str) -> bool:
        """Check if email domain is disposable."""
        domain = email.split('@')[1].lower()
        return domain in self.disposable_domains

    def is_role_based_email(self, email: str) -> bool:
        """Check if email is role-based."""
        local_part = email.split('@')[0].lower()
        return any(pattern in local_part for pattern in self.role_based_patterns)

    def check_typo(self, email: str) -> bool:
        """Check for common typos in email."""
        domain = email.split('@')[1].lower()
        return any(typo in domain for typo in self.typo_patterns)

    def assess_bounce_risk(self, email: str) -> str:
        """Assess the risk of email bouncing."""
        if not self.check_syntax(email):
            return 'high'
        if not self.check_format(email):
            return 'high'
        if not self.check_dns(email):
            return 'high'
        if not self.check_mx(email):
            return 'high'
        if self.is_disposable_domain(email):
            return 'high'
        if self.is_role_based_email(email):
            return 'medium'
        if self.check_typo(email):
            return 'medium'
        return 'low'

    def validate_email(self, email: str) -> Dict[str, Any]:
        """Validate an email address and return detailed results."""
        try:
            # Basic syntax check
            syntax_check = self.check_syntax(email)
            if not syntax_check:
                return {
                    'is_valid': False,
                    'email': email,
                    'details': {
                        'syntax_check': False,
                        'format_validation': False,
                        'dns_verification': False,
                        'mx_record_check': False,
                        'disposable_domain': False,
                        'role_based_email': False,
                        'typo_detection': False,
                        'bounce_risk': 'high'
                    }
                }
            
            # Format validation
            format_valid = self.check_format(email)
            if not format_valid:
                return {
                    'is_valid': False,
                    'email': email,
                    'details': {
                        'syntax_check': True,
                        'format_validation': False,
                        'dns_verification': False,
                        'mx_record_check': False,
                        'disposable_domain': False,
                        'role_based_email': False,
                        'typo_detection': False,
                        'bounce_risk': 'high'
                    }
                }
            
            # DNS verification
            dns_valid = self.check_dns(email)
            if not dns_valid:
                return {
                    'is_valid': False,
                    'email': email,
                    'details': {
                        'syntax_check': True,
                        'format_validation': True,
                        'dns_verification': False,
                        'mx_record_check': False,
                        'disposable_domain': False,
                        'role_based_email': False,
                        'typo_detection': False,
                        'bounce_risk': 'high'
                    }
                }
            
            # MX record check
            mx_valid = self.check_mx(email)
            if not mx_valid:
                return {
                    'is_valid': False,
                    'email': email,
                    'details': {
                        'syntax_check': True,
                        'format_validation': True,
                        'dns_verification': True,
                        'mx_record_check': False,
                        'disposable_domain': False,
                        'role_based_email': False,
                        'typo_detection': False,
                        'bounce_risk': 'high'
                    }
                }
            
            # Additional checks
            disposable = self.is_disposable_domain(email)
            role_based = self.is_role_based_email(email)
            typo = self.check_typo(email)
            bounce_risk = self.assess_bounce_risk(email)
            
            return {
                'is_valid': True,
                'email': email,
                'details': {
                    'syntax_check': True,
                    'format_validation': True,
                    'dns_verification': True,
                    'mx_record_check': True,
                    'disposable_domain': disposable,
                    'role_based_email': role_based,
                    'typo_detection': typo,
                    'bounce_risk': bounce_risk
                }
            }
            
        except Exception as e:
            logger.error(f"Error validating email {email}: {str(e)}")
            return {
                'is_valid': False,
                'email': email,
                'details': {
                    'syntax_check': False,
                    'format_validation': False,
                    'dns_verification': False,
                    'mx_record_check': False,
                    'disposable_domain': False,
                    'role_based_email': False,
                    'typo_detection': False,
                    'bounce_risk': 'high'
                }
            }

    def suggest_corrections(self, email: str) -> List[str]:
        """Suggest possible corrections for an email address."""
        suggestions = []
        
        # Extract domain and suffix
        ext = tldextract.extract(email)
        domain = ext.domain
        suffix = ext.suffix
        
        # Check for common domain typos
        for correct_domain, typos in self.typo_patterns.items():
            if domain + '.' + suffix in typos:
                suggestions.append(email.replace(domain + '.' + suffix, correct_domain))
        
        return suggestions

# Create a global instance
email_validator = EmailValidator()

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    file_path = f"{UPLOAD_DIR}/{file.filename}"
    
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await file.read()
        await out_file.write(content)

    df = pd.read_csv(file_path, header=None, names=["email"])
    email_list = df["email"].tolist()

    task = validate_bulk_emails.delay(email_list)

    return {"task_id": task.id, "message": "Email validation started!"}

@app.get("/results/{task_id}")
def get_results(task_id: str):
    task = AsyncResult(task_id, app=celery_app)

    if task.state == "PENDING":
        return {"task_id": task_id, "status": "PENDING"}

    if task.state == "FAILURE":
        return {"task_id": task_id, "status": "FAILED", "error": str(task.info)}

    if task.state == "SUCCESS":
        return {
            "task_id": task_id,
            "status": "SUCCESS",
            "results": task.result
        }

    return {"task_id": task_id, "status": task.state}

def validate_email_address(email: str) -> tuple[bool, str]:
    """
    Validate a single email address using the EmailValidator instance
    """
    return email_validator.validate_email(email)
    return email_validator.validate_email(email)