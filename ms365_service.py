# ms365_service.py
"""
Microsoft 365 to Oracle Employee Sync Service

Architecture:
    MS 365 Admin → Graph API → ms365_service → Oracle → employee_service → Redis

Features:
- Webhook listener for real-time user creation events
- Polling fallback for missed events
- Automatic employee record creation in Oracle
- Default department/designation assignment
- Email notification support

Port: 7700
Run: uvicorn ms365_service:app --host 0.0.0.0 --port 7700 --reload
"""

import os
import traceback
import json
import hashlib
import uuid
import threading
import oracledb
import redis
import httpx
import io
import re
import logging
import boto3
import asyncio
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timedelta, timezone
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Header, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaConsumer, KafkaProducer
from msgraph import GraphServiceClient
from azure.identity import ClientSecretCredential
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from boto3.dynamodb.conditions import Attr

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

scheduler = BackgroundScheduler()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEBUG_MEETING_COMPLETED_BOOKING_ID = os.getenv(
    "DEBUG_MEETING_COMPLETED_BOOKING_ID",
    "275a3ee2-5e19-4205-b784-414e06c32c18",
).strip()

# ── Kafka Producer ──────────────────────────────────────────────
_kafka_producer = None
try:
    _kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    print("✅ Kafka producer connected (ms365_service)")
except Exception as _ke:
    print(f"⚠️  Kafka producer unavailable (ms365_service): {_ke}")

def _kafka_publish(topic: str, payload: dict) -> None:
    """Fire-and-forget Kafka publish."""
    if _kafka_producer is None:
        print(f"⚠️  Kafka publish skipped [{topic}] because producer is unavailable")
        return
    try:
        print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
        _kafka_producer.send(topic, value=payload)
        _kafka_producer.flush(timeout=2)
        print(f"📤 Kafka → {topic}: {payload}")
    except Exception as e:
        print(f"⚠️  Kafka publish failed [{topic}]: {e}")

# ================= SERVICE URLS =================

HOME_SERVICE_URL = os.getenv("HOME_SERVICE_URL","http://localhost:5001")
MEETING_SERVICE_URL = os.getenv("MEETING_SERVICE_URL","http://localhost:9000")
CHATBOT_SERVICE_URL = os.getenv("CHATBOT_SERVICE_URL","http://localhost:7600")
ASSET_SERVICE_URL = os.getenv("ASSET_SERVICE_URL","http://localhost:8090")
INTERNSHIP_SERVICE_URL = os.getenv("INTERNSHIP_SERVICE_URL","http://localhost:5050")
EMPLOYEE_SERVICE_URL = os.getenv("EMPLOYEE_SERVICE_URL","http://localhost:8002")
BLOGGER_SERVICE_URL = os.getenv("BLOGGER_SERVICE_URL","http://localhost:7500")
REDIS_SERVICE_URL = os.getenv("REDIS_SERVICE_URL","http://localhost:6390")
BRS_SERVICE_URL = os.getenv("BRS_SERVICE_URL","http://localhost:8020")
LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'
STATIC_CDN = "https://d1pjjckqswt5z7.cloudfront.net"

CANONICAL_HOST = os.getenv("CANONICAL_HOST","www.chakorahub.com").strip().lower()
INTERNSHIP_PUBLIC_HOST = os.getenv("INTERNSHIP_PUBLIC_HOST","api.chakorahub.com").strip().lower()

# ─────────────────────────────────────────────
# 2. CONFIG — MS Graph + S3
# ─────────────────────────────────────────────
class Settings:
    # Microsoft Graph
    MS_TENANT_ID: str       = os.getenv("MS_TENANT_ID",     '')
    MS_CLIENT_ID: str       = os.getenv("MS_CLIENT_ID",     '')
    MS_CLIENT_SECRET: str   = os.getenv("MS_CLIENT_SECRET", '')

    MS_AUTHORITY: str       = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
    MS_SCOPE: list[str]     = ["https://graph.microsoft.com/.default"]
    MS_GRAPH_BASE: str      = "https://graph.microsoft.com/v1.0"

    # AWS S3
    S3_BUCKET: str          = os.getenv("S3_BUCKET",        "chakorahub-rag-s3")
    S3_PREFIX: str          = os.getenv("S3_PREFIX",         "Morning_Connect_Notes/")
    AWS_REGION: str         = os.getenv("AWS_REGION",        "eu-north-1")     # adjust to your region
    AWS_ACCESS_KEY_ID: str  = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    # Teams channel filter keywords (notes are matched by subject/body)
    NOTE_KEYWORDS: list[str] = ["morning connect", "daily connect", "daily standup", "morning standup"]

settings = Settings()

# Webhook Configuration
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "your-webhook-secret-key")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://chakorahub.com/api/ms365/webhook")

# MS365 credential aliases used across the module
MS365_TENANT_ID = settings.MS_TENANT_ID
MS365_CLIENT_ID = settings.MS_CLIENT_ID
MS365_CLIENT_SECRET = settings.MS_CLIENT_SECRET

# Oracle Configuration
ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREE")
ORACLE_USER = os.getenv("ORACLE_USER", "system")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Chakorahub123")

# Default Values for New Employees
DEFAULT_DEPT_ID = "DEPT001"  # HR or General
DEFAULT_DESIGNATION_ID = "DES001"  # Entry Level
DEFAULT_MANAGER_ID = None  # Will be set manually later
DEFAULT_LOCATION_ID = "LOC001"  # Headquarters

# Polling Configuration
POLL_INTERVAL_MINUTES = 30  # Check for new users every 30 minutes
ENABLE_PERIODIC_USER_SYNC = os.getenv("ENABLE_PERIODIC_USER_SYNC", "false").strip().lower() in ("1", "true", "yes", "on")

organizer_email = os.getenv("MS_ORGANIZER")

# ==========================================
# FASTAPI APP
# ==========================================

app = FastAPI(
    title="MS365 Employee Sync Service",
    description="Sync Microsoft 365 users to Oracle employee database",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# INITIALIZE SERVICES
# ==========================================

# Microsoft Graph Client
try:
    credential = ClientSecretCredential(
        tenant_id=MS365_TENANT_ID,
        client_id=MS365_CLIENT_ID,
        client_secret=MS365_CLIENT_SECRET
    )
    graph_client = GraphServiceClient(credentials=credential)
    print("✅ Microsoft Graph client initialized")
except Exception as e:
    print(f"❌ Graph client initialization failed: {e}")
    graph_client = None

# ==========================================
# PYDANTIC MODELS
# ==========================================

class MS365User(BaseModel):
    id: str
    displayName: str
    userPrincipalName: str
    mail: Optional[str] = None
    jobTitle: Optional[str] = None
    department: Optional[str] = None
    mobilePhone: Optional[str] = None
    officeLocation: Optional[str] = None

class WebhookNotification(BaseModel):
    subscriptionId: str
    clientState: Optional[str] = None
    changeType: str
    resource: str
    resourceData: Dict[str, Any]

class EmployeeCreateRequest(BaseModel):
    employee_name: str
    email: EmailStr
    department: Optional[str] = None
    designation: Optional[str] = None
    manager_id: Optional[str] = None
    notes: Optional[str] = None

class SyncResponse(BaseModel):
    success: bool
    message: str
    employees_synced: int
    details: List[Dict] = []

# ==========================================
# DATABASE & OTHER FUNCTIONS
# ==========================================

async def sync_all_meeting_transcripts():

    try:
        organizer_email = os.getenv("MS_ORGANIZER")
        print("=" * 60)
        print(f"🚀 Sync started for organizer: {organizer_email}")
        print("=" * 60)

        # Step 1
        user = await get_user_by_email(organizer_email)
        user_id = user["id"]

        # Step 2
        meetings = await get_user_meetings(user_id)

        # Step 3
        for meeting in meetings:
            meeting_id = meeting["id"]
            transcripts = await get_meeting_transcripts(
                user_id,
                meeting_id
            )

            for transcript in transcripts:
                transcript_id = transcript["id"]
                content = await download_transcript_preview(
                    transcript_id, user_id
                )

                await upload_transcript_to_s3(
                    content,
                    meeting
                )

        print("✅ Transcript sync completed")

    except Exception as e:
        print(f"❌ Transcript sync failed: {e}")

def get_db_connection():
    """Create Oracle database connection"""
    try:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
        )
        conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise

def employee_exists(email: str) -> bool:
    """Check if employee already exists in database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT EMPLOYEE_ID FROM EMP_NRM_EMPLOYEES WHERE LOWER(EMAIL) = LOWER(:1)",
            (email,)
        )
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return result is not None
    
    except Exception as e:
        print(f"❌ Error checking employee existence: {e}")
        return False

def generate_employee_id() -> str:
    """Generate unique employee ID"""
    # Format: EMP + timestamp + random
    timestamp = datetime.now().strftime("%Y%m%d")
    random_suffix = uuid.uuid4().hex[:4].upper()
    return f"EMP{timestamp}{random_suffix}"

def get_or_create_department(dept_name: str) -> str:
    """Get department ID or create new department"""
    if not dept_name or dept_name.strip() == "":
        return DEFAULT_DEPT_ID
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if department exists
        cursor.execute(
            "SELECT DEPT_ID FROM EMP_NRM_DEPARTMENTS WHERE LOWER(DEPT_NAME) = LOWER(:1)",
            (dept_name,)
        )
        
        result = cursor.fetchone()
        
        if result:
            dept_id = result[0]
        else:
            # Create new department
            dept_id = f"DEPT{uuid.uuid4().hex[:6].upper()}"
            cursor.execute(
                """
                INSERT INTO EMP_NRM_DEPARTMENTS (DEPT_ID, DEPT_NAME, DESCRIPTION, CREATED_AT)
                VALUES (:1, :2, :3, :4)
                """,
                (dept_id, dept_name, f"Auto-created from MS365: {dept_name}", datetime.now())
            )
            conn.commit()
            print(f"✅ Created new department: {dept_name} ({dept_id})")
        
        cursor.close()
        conn.close()
        
        return dept_id
    
    except Exception as e:
        print(f"❌ Error getting/creating department: {e}")
        return DEFAULT_DEPT_ID

def get_or_create_designation(title: str) -> str:
    """Get designation ID or create new designation"""
    if not title or title.strip() == "":
        return DEFAULT_DESIGNATION_ID
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if designation exists
        cursor.execute(
            "SELECT DESIGNATION_ID FROM EMP_NRM_DESIGNATIONS WHERE LOWER(TITLE) = LOWER(:1)",
            (title,)
        )
        
        result = cursor.fetchone()
        
        if result:
            designation_id = result[0]
        else:
            # Create new designation
            designation_id = f"DES{uuid.uuid4().hex[:6].upper()}"
            cursor.execute(
                """
                INSERT INTO EMP_NRM_DESIGNATIONS (DESIGNATION_ID, TITLE, DESCRIPTION, LEVEL, CREATED_AT)
                VALUES (:1, :2, :3, :4, :5)
                """,
                (designation_id, title, f"Auto-created from MS365: {title}", "Entry", datetime.now())
            )
            conn.commit()
            print(f"✅ Created new designation: {title} ({designation_id})")
        
        cursor.close()
        conn.close()
        
        return designation_id
    
    except Exception as e:
        print(f"❌ Error getting/creating designation: {e}")
        return DEFAULT_DESIGNATION_ID

def create_employee_in_db(
    employee_name: str,
    email: str,
    department: Optional[str] = None,
    designation: Optional[str] = None,
    manager_id: Optional[str] = None,
    notes: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create employee records in Oracle
    Creates entries in both EMP_NRM_EMPLOYEES and EMP_NRM_JOB_WORK
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Generate IDs
        employee_id = generate_employee_id()
        application_id = f"APP{uuid.uuid4().hex[:8].upper()}"
        job_id = f"JOB{uuid.uuid4().hex[:8].upper()}"
        
        # Get department and designation IDs
        dept_id = get_or_create_department(department) if department else DEFAULT_DEPT_ID
        designation_id = get_or_create_designation(designation) if designation else DEFAULT_DESIGNATION_ID
        
        # Insert into EMP_NRM_EMPLOYEES
        cursor.execute(
            """
            INSERT INTO EMP_NRM_EMPLOYEES (
                APPLICATION_ID, EMPLOYEE_NAME, APPLIED_DATE, STATUS, CREATED_AT,
                EMAIL, NOTES, ADMIN_USERNAME, EMPLOYEE_ID
            ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
            """,
            (
                application_id,
                employee_name,
                datetime.now().date(),
                "Active",
                datetime.now(),
                email,
                notes or f"Auto-synced from Microsoft 365 on {datetime.now().strftime('%Y-%m-%d')}",
                "ms365_sync",
                employee_id
            )
        )
        
        # Insert into EMP_NRM_JOB_WORK
        cursor.execute(
            """
            INSERT INTO EMP_NRM_JOB_WORK (
                JOB_ID, EMPLOYEE_ID, DEPT_ID, DESIGNATION_ID, MANAGER_ID, LOCATION_ID, CREATED_AT
            ) VALUES (:1, :2, :3, :4, :5, :6, :7)
            """,
            (
                job_id,
                employee_id,
                dept_id,
                designation_id,
                manager_id,
                DEFAULT_LOCATION_ID,
                datetime.now()
            )
        )
        
        conn.commit()
        _kafka_publish("employee.created", {
            "employee_id": employee_id,
            "employee_name": employee_name,
            "email": email,
            "department": department,
            "designation": designation,
            "source": "ms365_sync",
            "created_at": datetime.utcnow().isoformat(),
        })
        cursor.close()
        conn.close()
        
        print(f"✅ Created employee: {employee_name} ({employee_id})")
        
        # Invalidate employee cache in Redis
        invalidate_employee_cache(employee_id)
        
        return {
            "success": True,
            "employee_id": employee_id,
            "employee_name": employee_name,
            "email": email,
            "department": department,
            "designation": designation
        }
    
    except Exception as e:
        print(f"❌ Error creating employee: {e}")
        traceback.print_exc()
        raise

def invalidate_employee_cache(employee_id: str = None):
    try:
        with httpx.Client(timeout=5) as client:
            if employee_id:
                for key in [f"employee:{employee_id}", f"emp_service:profile:{employee_id}", f"user:{employee_id}"]:
                    client.delete(f"{REDIS_SERVICE_URL}/apicache/delete", params={"cache_key": key})

            # Scan + delete emp_service:* pattern
            scan_resp = client.get(f"{REDIS_SERVICE_URL}/redis/scan", params={"pattern": "emp_service:*", "db": 5})
            keys = scan_resp.json().get("keys", [])
            if keys:
                client.post(f"{REDIS_SERVICE_URL}/redis/delete", json={"keys": keys, "db": 5})

        print("✅ Invalidated employee cache via redis_service")
    except Exception as e:
        print(f"⚠️ Cache invalidation error: {e}")

# ==========================================
# MICROSOFT GRAPH FUNCTIONS
# ==========================================

async def get_all_ms365_users() -> List[MS365User]:
    """Fetch all users from Microsoft 365"""
    if not graph_client:
        raise Exception("Graph client not initialized")
    
    try:
        users_response = await graph_client.users.get()
        
        users = []
        if users_response and users_response.value:
            for user in users_response.value:
                users.append(MS365User(
                    id=user.id,
                    displayName=user.display_name or "Unknown",
                    userPrincipalName=user.user_principal_name or "",
                    mail=user.mail,
                    jobTitle=user.job_title,
                    department=user.department,
                    mobilePhone=user.mobile_phone,
                    officeLocation=user.office_location
                ))
        
        print(f"✅ Fetched {len(users)} users from MS365")
        return users
    
    except Exception as e:
        print(f"❌ Error fetching MS365 users: {e}")
        traceback.print_exc()
        raise

async def get_ms365_user_by_id(user_id: str) -> Optional[MS365User]:
    """Fetch specific user from Microsoft 365"""
    if not graph_client:
        raise Exception("Graph client not initialized")
    
    try:
        user = await graph_client.users.by_user_id(user_id).get()
        
        if user:
            return MS365User(
                id=user.id,
                displayName=user.display_name or "Unknown",
                userPrincipalName=user.user_principal_name or "",
                mail=user.mail,
                jobTitle=user.job_title,
                department=user.department,
                mobilePhone=user.mobile_phone,
                officeLocation=user.office_location
            )
        
        return None
    
    except Exception as e:
        print(f"❌ Error fetching MS365 user {user_id}: {e}")
        return None

# ==========================================
# SYNC FUNCTIONS
# ==========================================

async def sync_ms365_user_to_oracle(ms365_user: MS365User) -> Dict[str, Any]:
    """
    Sync a single MS365 user to Oracle
    Returns sync result with details
    """
    try:
        # Use mail if available, otherwise use userPrincipalName
        email = ms365_user.mail or ms365_user.userPrincipalName
        
        # Check if already exists
        if employee_exists(email):
            print(f"⏭️ Employee already exists: {email}")
            return {
                "success": False,
                "reason": "already_exists",
                "email": email,
                "name": ms365_user.displayName
            }
        
        # Create employee in Oracle
        result = create_employee_in_db(
            employee_name=ms365_user.displayName,
            email=email,
            department=ms365_user.department,
            designation=ms365_user.jobTitle,
            manager_id=None,  # Will be set manually
            notes=f"Synced from MS365. Office: {ms365_user.officeLocation or 'N/A'}, Phone: {ms365_user.mobilePhone or 'N/A'}"
        )
        
        return result
    
    except Exception as e:
        print(f"❌ Error syncing user {ms365_user.displayName}: {e}")
        return {
            "success": False,
            "reason": "error",
            "error": str(e),
            "name": ms365_user.displayName,
            "email": ms365_user.mail or ms365_user.userPrincipalName
        }

async def sync_all_ms365_users() -> SyncResponse:
    """
    Sync all MS365 users to Oracle
    Only creates new employees, doesn't update existing
    """
    try:
        # Fetch all MS365 users
        ms365_users = await get_all_ms365_users()
        
        synced_count = 0
        details = []
        
        for user in ms365_users:
            result = await sync_ms365_user_to_oracle(user)
            
            if result["success"]:
                synced_count += 1
            
            details.append(result)
        
        return SyncResponse(
            success=True,
            message=f"Synced {synced_count} new employees from {len(ms365_users)} total MS365 users",
            employees_synced=synced_count,
            details=details
        )
    
    except Exception as e:
        print(f"❌ Sync all error: {e}")
        traceback.print_exc()
        return SyncResponse(
            success=False,
            message=f"Sync failed: {str(e)}",
            employees_synced=0,
            details=[]
        )

# ==========================================
# API ENDPOINTS
# ==========================================

@app.get("/")
def root():
    redis_status = "disconnected"
    try:
        r = httpx.get(f"{REDIS_SERVICE_URL}/health", timeout=2)
        redis_status = "connected" if r.json().get("success") else "disconnected"
    except:
        pass
    return {
        "service": "MS365 Employee Sync Service",
        "version": "1.0.0",
        "status": "running",
        "graph_client": "connected" if graph_client else "disconnected",
        "redis": redis_status,
    }

@app.get("/health")
def health():
    health_status = {
        "status": "healthy",
        "graph_api": "connected" if graph_client else "disconnected",
        "redis": "disconnected",
        "oracle": "unknown"
    }
    redis_status = "disconnected"
    try:
        r = httpx.get(f"{REDIS_SERVICE_URL}/health", timeout=2)
        redis_status = "connected" if r.json().get("success") else "disconnected"
    except:
        pass
    
    # Test Oracle
    try:
        conn = get_db_connection()
        conn.cursor().execute("SELECT 1")
        conn.close()
        health_status["oracle"] = "connected"
    except:
        health_status["oracle"] = "disconnected"
    
    return health_status

@app.post("/sync/all")
async def sync_all_users(background_tasks: BackgroundTasks):
    """
    Manually trigger sync of all MS365 users to Oracle
    Runs in background
    """
    background_tasks.add_task(sync_all_ms365_users)
    
    return {
        "success": True,
        "message": "Sync started in background. Check /sync/status for results."
    }

@app.post("/sync/user/{user_id}")
async def sync_specific_user(user_id: str):
    """
    Sync a specific MS365 user to Oracle
    """
    try:
        # Fetch user from MS365
        ms365_user = await get_ms365_user_by_id(user_id)
        
        if not ms365_user:
            raise HTTPException(status_code=404, detail="User not found in MS365")
        
        # Sync to Oracle
        result = await sync_ms365_user_to_oracle(ms365_user)
        
        if result["success"]:
            return {
                "success": True,
                "message": f"User {ms365_user.displayName} synced successfully",
                "employee_id": result.get("employee_id"),
                "details": result
            }
        else:
            return {
                "success": False,
                "message": f"User sync failed: {result.get('reason')}",
                "details": result
            }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook")
async def webhook_endpoint(
    request: Request,
    validationToken: Optional[str] = None
):
    """
    Webhook endpoint for MS365 subscription notifications
    
    Step 1: Validation (when creating subscription)
    Step 2: Handle notifications (when user created/updated)
    """
    # Validation request (subscription creation)
    if validationToken:
        print(f"✅ Webhook validation: {validationToken}")
        return {
            "validationToken": validationToken
        }
    
    # Notification request
    try:
        body = await request.json()
        
        # Extract notifications
        if "value" in body:
            for notification in body["value"]:
                # Verify client state (security)
                if notification.get("clientState") != WEBHOOK_SECRET:
                    print(f"⚠️ Invalid client state in webhook")
                    continue
                
                # Check if it's a user creation event
                if notification.get("changeType") == "created":
                    resource = notification.get("resource", "")
                    
                    # Extract user ID from resource URL
                    # Format: "Users/{user-id}"
                    if "Users/" in resource:
                        user_id = resource.split("Users/")[1].split("/")[0]
                        
                        print(f"📨 New user created webhook: {user_id}")
                        
                        # Sync the new user (async)
                        ms365_user = await get_ms365_user_by_id(user_id)
                        if ms365_user:
                            await sync_ms365_user_to_oracle(ms365_user)
        
        return {"success": True}
    
    except Exception as e:
        print(f"❌ Webhook error: {e}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.post("/employee/create")
async def create_employee_manual(request: EmployeeCreateRequest):
    """
    Manually create employee record in Oracle
    Use this if MS365 sync is not available
    """
    try:
        # Check if already exists
        if employee_exists(request.email):
            raise HTTPException(status_code=400, detail="Employee with this email already exists")
        
        # Create employee
        result = create_employee_in_db(
            employee_name=request.employee_name,
            email=request.email,
            department=request.department,
            designation=request.designation,
            manager_id=request.manager_id,
            notes=request.notes
        )
        
        return {
            "success": True,
            "message": "Employee created successfully",
            "employee_id": result["employee_id"],
            "details": result
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/employees/list")
def list_employees(limit: int = 100, offset: int = 0):
    """
    List all employees from Oracle
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT 
                e.EMPLOYEE_ID,
                e.EMPLOYEE_NAME,
                e.EMAIL,
                e.STATUS,
                e.CREATED_AT,
                d.DEPT_NAME,
                des.TITLE as DESIGNATION
            FROM EMP_NRM_EMPLOYEES e
            LEFT JOIN EMP_NRM_JOB_WORK jw ON e.EMPLOYEE_ID = jw.EMPLOYEE_ID
            LEFT JOIN EMP_NRM_DEPARTMENTS d ON jw.DEPT_ID = d.DEPT_ID
            LEFT JOIN EMP_NRM_DESIGNATIONS des ON jw.DESIGNATION_ID = des.DESIGNATION_ID
            ORDER BY e.CREATED_AT DESC
            OFFSET :2 ROWS FETCH NEXT :1 ROWS ONLY
            """,
            (limit, offset)
        )
        
        rows = cursor.fetchall()
        employees = [
            {
                "EMPLOYEE_ID": row[0],
                "EMPLOYEE_NAME": row[1],
                "EMAIL": row[2],
                "STATUS": row[3],
                "CREATED_AT": row[4],
                "DEPT_NAME": row[5],
                "DESIGNATION": row[6],
            }
            for row in rows
        ]
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "count": len(employees),
            "employees": employees
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ms365/users")
async def get_ms365_users():
    """
    Get all users from Microsoft 365 (for comparison)
    """
    try:
        users = await get_all_ms365_users()
        return {
            "success": True,
            "count": len(users),
            "users": [user.dict() for user in users]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/cache/invalidate")
def invalidate_cache_endpoint(employee_id: Optional[str] = None):
    """
    Invalidate employee cache in Redis
    """
    try:
        invalidate_employee_cache(employee_id)
        return {
            "success": True,
            "message": f"Cache invalidated" + (f" for {employee_id}" if employee_id else " for all employees")
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==========================================
# BACKGROUND TASKS
# ==========================================

from fastapi_utils.tasks import repeat_every

@app.on_event("startup")
@repeat_every(seconds=POLL_INTERVAL_MINUTES * 60)  # Run every 30 minutes
async def periodic_sync():
    """
    Periodic background sync of MS365 users
    Runs every 30 minutes to catch any missed webhook events
    """
    if not ENABLE_PERIODIC_USER_SYNC:
        return

    print(f"🔄 Starting periodic MS365 sync...")
    try:
        result = await sync_all_ms365_users()
        print(f"✅ Periodic sync complete: {result.employees_synced} new employees")
    except Exception as e:
        print(f"❌ Periodic sync error: {e}")

@app.on_event("startup")
async def startup_transcript_scheduler():

    print("🚀 MS365 Transcript Service Started")

    scheduler.add_job(
        process_completed_meetings,
        trigger="interval",
        minutes=30,
        max_instances=1,
        coalesce=True
    )

    scheduler.start()

    print("✅ Scheduler started")

# ==========================================
# STARTUP EVENT
# ==========================================

@app.on_event("startup")
async def startup_event():
    print("=" * 60)
    print("🚀 MS365 Employee Sync Service Starting")
    print("=" * 60)
    print(f"📊 Oracle: {ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}")
    print(f"🔴 Redis: {REDIS_SERVICE_URL}")
    print(f"🔵 MS365 Tenant: {MS365_TENANT_ID}")
    print(f"📡 Webhook URL: {WEBHOOK_URL}")
    print(f"⏰ Poll Interval: {POLL_INTERVAL_MINUTES} minutes")
    print(f"👥 Periodic User Sync: {'enabled' if ENABLE_PERIODIC_USER_SYNC else 'disabled'}")
    print("=" * 60)

# ==========================================
# TEAMS MEETING CREATION
# ==========================================

class CreateMeetingRequest(BaseModel):
    date: str
    start_time: str
    duration_minutes: int
    attendee_email: str


@app.post("/teams/create-meeting")
async def create_teams_meeting(req: CreateMeetingRequest):
    """
    Create a Microsoft Teams online meeting via Graph API.
    Uses the organizer account (MS_ORGANIZER) as the meeting host.
    Returns the join URL.
    """
    start_time = req.start_time.strip()
    if start_time.count(":") == 1:
        start_time = f"{start_time}:00"

    try:
        start_dt = datetime.fromisoformat(f"{req.date}T{start_time}")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date/start_time format: {exc}")

    end_dt = start_dt + timedelta(minutes=req.duration_minutes)
    # Graph event payload expects local datetime + a Windows timezone name.
    graph_tz = os.getenv("MS_GRAPH_TIMEZONE", "India Standard Time")
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

    organizer = os.getenv("MS_ORGANIZER", "support@chakorahub.com")

    token_url = (
        f"https://login.microsoftonline.com/{MS365_TENANT_ID}/oauth2/v2.0/token"
    )
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": MS365_CLIENT_ID,
                "client_secret": MS365_CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default",
            },
        )
        if token_resp.is_error:
            raise HTTPException(
                status_code=502,
                detail=f"MS token request failed: {token_resp.status_code} - {token_resp.text}",
            )
        access_token = token_resp.json()["access_token"]
        graph_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        meeting_resp = await client.post(
            f"https://graph.microsoft.com/v1.0/users/{organizer}/events",
            headers=graph_headers,
            json={
                "subject": f"ChakoraHub Session – {req.date} {start_time}",
                "start": {
                    "dateTime": start_iso,
                    "timeZone": graph_tz,
                },
                "end": {
                    "dateTime": end_iso,
                    "timeZone": graph_tz,
                },
                "isOnlineMeeting": True,
                "onlineMeetingProvider": "teamsForBusiness",
                "attendees": [
                    {
                        "emailAddress": {
                            "address": req.attendee_email,
                        },
                        "type": "required",
                    }
                ],
            },
        )
        if meeting_resp.is_error:
            raise HTTPException(
                status_code=502,
                detail=f"Graph create meeting failed: {meeting_resp.status_code} - {meeting_resp.text}",
            )
        data = meeting_resp.json()

        online_meeting = data.get("onlineMeeting") or {}
        join_url = online_meeting.get("joinUrl") or data.get("onlineMeetingUrl")

        if not join_url and data.get("id"):
            event_resp = await client.get(
                f"https://graph.microsoft.com/v1.0/users/{organizer}/events/{data['id']}?$select=onlineMeeting,onlineMeetingUrl",
                headers=graph_headers,
            )
            if not event_resp.is_error:
                event_data = event_resp.json()
                if event_data:
                    data.update(event_data)

    online_meeting = data.get("onlineMeeting") or {}
    join_url = online_meeting.get("joinUrl") or data.get("onlineMeetingUrl")

    # Fallback: check nested onlineMeeting object explicitly.
    if not join_url and "onlineMeeting" in data:
        join_url = data["onlineMeeting"].get("joinUrl")

    if not join_url:
        print(f"⚠️ Warning: Response received but no joinUrl found in payload keys: {list(data.keys())}")

    online_meeting_id = (data.get("onlineMeeting") or {}).get("id", "")
    if not online_meeting_id and join_url:
    # joinUrl → lookup the actual onlineMeeting object to get its ID
        async with httpx.AsyncClient() as client2:
            token = access_token  # reuse token from above
            lookup_resp = await client2.get(
                f"https://graph.microsoft.com/v1.0/users/{organizer}/onlineMeetings"
                f"?$filter=JoinWebUrl eq '{join_url}'",
                headers={"Authorization": f"Bearer {token}"}
            )
            if lookup_resp.status_code == 200:
                items = lookup_resp.json().get("value", [])
                if items:
                    online_meeting_id = items[0].get("id", "")
                    print(f"✅ Online meeting ID resolved via JoinUrl | id={online_meeting_id}")
                else:
                    print(f"⚠️ No onlineMeeting found for joinUrl")
            else:
                print(f"⚠️ onlineMeeting lookup failed | status={lookup_resp.status_code} | body={lookup_resp.text}")

    print(f"✅ Teams meeting created | join_url={join_url} | online_meeting_id={online_meeting_id}")
    return {"join_url": join_url, "meeting_id": online_meeting_id}

# ── Kafka Consumer: meeting.booked ──────────────────────────────

def _consume_meeting_booked():
    """
    Step 12+13: Consumes 'meeting.booked', creates Teams link via Graph API,
    then publishes 'teams.link.created' for downstream consumers.
    Runs in a background thread so it doesn't block FastAPI.
    """
    try:
        consumer = KafkaConsumer(
            "meeting.booked",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="ms365-meeting-booked-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: meeting.booked")
    except Exception as e:
        print(f"⚠️  Kafka consumer failed to start (meeting.booked): {e}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value
        booking_id   = event.get("booking_id")
        attendee     = event.get("student_email")
        date         = event.get("date")
        start_time   = event.get("start_time")
        duration_min = event.get("duration_minutes", 60)

        if not booking_id or not attendee or not date or not start_time:
            print(f"⚠️ Invalid meeting.booked event payload: {event}")
            continue

        print(f"📥 Kafka ← meeting.booked | booking_id={booking_id}")

        # Skip if meeting_service already created the Teams link
        if event.get("teams_link"):
            print(f"ℹ️  Teams link already present in event, skipping creation: {event['teams_link']}")
            payload = {
                **event,
                "booking_date": event.get("booking_date") or date,
                "student_email": event.get("student_email") or attendee,
                "meeting_link": event["teams_link"],
                "meeting_id": event.get("meeting_id", ""),
                "attendee": attendee,
                "student_name": event.get("student_name", ""),
                "purpose": event.get("purpose", ""),
                "payment_id": event.get("payment_id", ""),
                "order_id": event.get("order_id", ""),
                "transcript_status": event.get("transcript_status", "PENDING"),
                "correlation_id": event.get("correlation_id", booking_id),
                "source": "passthrough",
                "published_at": datetime.utcnow().isoformat(),
            }
            _kafka_publish("teams.link.created", payload)
            continue

        try:
            req = CreateMeetingRequest(
                date=date,
                start_time=start_time,
                duration_minutes=duration_min,
                attendee_email=attendee,
            )
            meeting_data = asyncio.run(create_teams_meeting(req))
            join_url = meeting_data.get("join_url", "")
            meeting_id = meeting_data.get("meeting_id", "")

            if not join_url:
                raise RuntimeError("create_teams_meeting succeeded but join_url is empty")

            print(f"✅ Teams link created via Kafka consumer | booking_id={booking_id} | url={join_url}")

            payload = {
                **event,
                "booking_date": event.get("booking_date") or date,
                "student_email": event.get("student_email") or attendee,
                "meeting_link": join_url,
                "meeting_id": meeting_id,
                "attendee": attendee,
                "student_name": event.get("student_name", ""),
                "purpose": event.get("purpose", ""),
                "payment_id": event.get("payment_id", ""),
                "order_id": event.get("order_id", ""),
                "transcript_status": event.get("transcript_status", "PENDING"),
                "correlation_id": event.get("correlation_id", booking_id),
                "source": "ms365_service",
                "published_at": datetime.utcnow().isoformat(),
            }
            _kafka_publish("teams.link.created", payload)
            if meeting_id:
                bookings_table.update_item(
                Key={"bookingId": booking_id},
                UpdateExpression="SET meeting_id = :m",
                ExpressionAttributeValues={":m": meeting_id}
            )
            print(f"✅ meeting_id saved to DynamoDB | booking_id={booking_id}")

        except Exception as exc:
            print(f"❌ Teams link creation failed | booking_id={booking_id} | error={exc}")
            _kafka_publish("teams.link.failed", {
                "booking_id": booking_id,
                "attendee": attendee,
                "error": str(exc),
                "source": "ms365_service",
                "published_at": datetime.utcnow().isoformat(),
            })



# ----------- Latest code 

dynamodb = boto3.resource(
    "dynamodb",
    region_name="eu-north-1"
)
bookings_table = dynamodb.Table("Bookings")

s3_client = boto3.client(
    "s3",
    region_name="eu-north-1"
)
TRANSCRIPT_BUCKET = "chakorahub-meeting-s3"

def calculate_end_time(
    booking_date,
    start_time,
    duration_minutes
):
    start_dt = datetime.strptime(
        f"{booking_date} {start_time}",
        "%Y-%m-%d %H:%M"
    )
    return start_dt + timedelta(
        minutes=int(duration_minutes)
    )

def upload_transcript_to_s3(
    booking_id,
    transcript_text,
    booking_record
):
    key = f"transcripts/{booking_id}.json"
    payload = {
        "booking_id": booking_id,
        "student_email": booking_record.get("student_email"),
        "meeting_id": booking_record.get("meeting_id"),
        "transcript": transcript_text,
        "uploaded_at": datetime.utcnow().isoformat()
    }
    s3_client.put_object(
        Bucket=TRANSCRIPT_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )
    print(f"✅ Uploaded transcript to S3 | booking_id={booking_id} | key={key} | chars={len(transcript_text or '')}")
    return key


async def lookup_meeting_id_by_join_url(user_id: str, join_url: str) -> str:
    """Resolve Graph online meeting id from a Teams join URL for a user."""
    token = await get_graph_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings",
            params={"$filter": f"JoinWebUrl eq '{join_url}'"},
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.status_code != 200:
            print(f"⚠️ onlineMeeting lookup failed | user_id={user_id} | status={resp.status_code} | body={resp.text}")
            return ""
        items = resp.json().get("value", [])
        if not items:
            decoded_join_url = unquote(join_url)
            if decoded_join_url != join_url:
                retry_resp = await client.get(
                    f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings",
                    params={"$filter": f"JoinWebUrl eq '{decoded_join_url}'"},
                    headers={"Authorization": f"Bearer {token}"},
                )
                if retry_resp.status_code == 200:
                    items = retry_resp.json().get("value", [])
            if not items:
                print(f"⚠️ onlineMeeting lookup returned no matches | user_id={user_id}")
                return ""
        meeting_id = items[0].get("id", "")
        if meeting_id:
            print(f"✅ onlineMeeting id resolved from join URL | user_id={user_id} | meeting_id={meeting_id}")
        return meeting_id


def extract_user_oid_from_teams_link(teams_link: str) -> str:
    """Extract organizer Oid from Teams join URL context query param."""
    try:
        parsed = urlparse(teams_link)
        context_vals = parse_qs(parsed.query).get("context") or []
        if not context_vals:
            return ""
        context_raw = unquote(context_vals[0])
        context = json.loads(context_raw)
        return (context.get("Oid") or "").strip()
    except Exception:
        return ""

def process_completed_meetings():
    print("🔄 Checking completed meetings...")
    # booking_date/start_time are stored in local business time; compare in local time
    now = datetime.now()
    organizer = (os.getenv("MS_ORGANIZER") or "").strip()
    organizer_user_id = ""
    if organizer:
        try:
            organizer_user_id = asyncio.run(get_user_by_email(organizer)).get("id", "")
            print(f"✅ Organizer resolved for transcript fetch | email={organizer} | user_id={organizer_user_id}")
        except Exception as exc:
            print(f"⚠️ Organizer lookup failed | email={organizer} | error={exc}")
    else:
        print("⚠️ MS_ORGANIZER is empty; transcript fetch will be skipped when meeting_id exists")

    response = bookings_table.scan(
        FilterExpression=Attr("transcript_status").eq("PENDING")
    )
    for booking in response.get("Items", []):
        booking_id = booking.get("bookingId")
        if booking.get("transcript_status") != "PENDING":
            if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
                print(f"🎯 Skip reason for target booking | booking_id={booking_id} | transcript_status={booking.get('transcript_status')}")
            continue
        print(f"🧾 Booking snapshot | booking_id={booking_id} | keys={sorted(list(booking.keys()))} | meeting_id={booking.get('meeting_id') or 'missing'} | transcript_status={booking.get('transcript_status')}")
        if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
            print(f"🎯 meeting.completed debug target matched in scheduler | booking_id={booking_id}")
        
        booking_id = booking["bookingId"]
        meeting_id = booking.get("meeting_id")
        teams_link = (booking.get("teams_link") or booking.get("meeting_link") or "").strip()
        booking_user_id = organizer_user_id
        if not booking_user_id and teams_link:
            booking_user_id = extract_user_oid_from_teams_link(teams_link)
            if booking_user_id:
                print(f"✅ Derived organizer user_id from teams_link context | booking_id={booking_id} | user_id={booking_user_id}")

        if not meeting_id and teams_link and booking_user_id:
            print(f"🔎 Attempting meeting_id backfill from teams_link | booking_id={booking_id}")
            try:
                resolved_meeting_id = asyncio.run(lookup_meeting_id_by_join_url(booking_user_id, teams_link))
                if resolved_meeting_id:
                    meeting_id = resolved_meeting_id
                    bookings_table.update_item(
                        Key={"bookingId": booking_id},
                        UpdateExpression="SET meeting_id = :m",
                        ExpressionAttributeValues={":m": meeting_id},
                    )
                    booking["meeting_id"] = meeting_id
                    print(f"✅ meeting_id backfilled from teams_link | booking_id={booking_id} | meeting_id={meeting_id}")
                else:
                    print(f"⚠️ meeting_id backfill failed from teams_link | booking_id={booking_id}")
            except Exception as exc:
                print(f"⚠️ meeting_id backfill error | booking_id={booking_id} | error={exc}")

        print(f"🧭 Processing booking | booking_id={booking_id} | meeting_id={meeting_id or 'missing'} | status={booking.get('transcript_status')}")
        end_time = calculate_end_time(
            booking["booking_date"],
            booking["start_time"],
            booking["duration_minutes"]
        )
        print(f"⏰ Evaluating booking {booking_id} | end_time={end_time} | now={now}")
        if now < end_time:
            if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
                print(f"🎯 Skip reason for target booking | booking_id={booking_id} | now before end_time")
            continue
        try:
            transcript_text = ""
            if meeting_id:
                print(f"📥 Fetching transcript from Graph | booking_id={booking_id} | meeting_id={meeting_id}")
                if booking_user_id:
                    transcript_text = await_fetch_transcript(meeting_id, booking_user_id)
                else:
                    print(f"⚠️ Skipping transcript fetch due to missing organizer user id | booking_id={booking_id}")
                print(f"📄 Transcript fetch result | booking_id={booking_id} | chars={len(transcript_text or '')}")
            else:
                print(f"⚠️ No meeting_id found for completed booking | booking_id={booking_id} | booking will not be uploaded to S3")

            if not meeting_id:
                print(f"⏭️ Skipping transcript upload because meeting_id is missing | booking_id={booking_id}")
                if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
                    print(f"🎯 Skip reason for target booking | booking_id={booking_id} | meeting_id missing")
                continue

            if not (transcript_text or "").strip():
                print(f"⏭️ Skipping transcript upload because transcript text is empty | booking_id={booking_id} | meeting_id={meeting_id}")
                if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
                    print(f"🎯 Skip reason for target booking | booking_id={booking_id} | transcript empty")
                continue

            s3_key = upload_transcript_to_s3(
                booking_id,
                transcript_text,
                booking
            )
            print(f"🗂️ Transcript stored reference | booking_id={booking_id} | s3_key={s3_key}")
            bookings_table.update_item(
                Key={
                    "bookingId": booking_id
                },
                UpdateExpression="""
                    SET transcript_status=:s,
                        transcript_s3_key=:k,
                        meeting_completed_at=:t
                """,
                ExpressionAttributeValues={
                    ":s": "COMPLETED",
                    ":k": s3_key,
                    ":t": datetime.utcnow().isoformat()
                }
            )
            _kafka_publish(
                "meeting.completed",
                {
                    "booking_id": booking_id,
                    "student_email": booking.get("student_email"),
                    "meeting_id": meeting_id,
                    "transcript_s3_key": s3_key,
                    "status": "COMPLETED"
                }
            )
            if DEBUG_MEETING_COMPLETED_BOOKING_ID and booking_id == DEBUG_MEETING_COMPLETED_BOOKING_ID:
                print(f"🎯 meeting.completed publish attempted for target booking | booking_id={booking_id} | s3_key={s3_key}")
            print(
                f"✅ Transcript processed | {booking_id}"
            )
        except Exception as exc:
            print(
                f"❌ Transcript processing failed | {booking_id} | {exc}"
            )

async def get_user_by_email(email: str):
    print(f"🔎 Graph user lookup by email | email={email}")
    token = await get_graph_access_token()
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"https://graph.microsoft.com/v1.0/users/{email}",
            headers={
                "Authorization": f"Bearer {token}"
            }
        )
        response.raise_for_status()
        data = response.json()
        print(f"✅ Graph user lookup success | email={email} | user_id={data.get('id')}")
        return data

async def get_user_meetings(user_principal: str, start_iso: Optional[str] = None, end_iso: Optional[str] = None):
    now_utc = datetime.now(timezone.utc)
    if not start_iso:
        start_iso = (now_utc - timedelta(days=7)).isoformat().replace("+00:00", "Z")
    if not end_iso:
        end_iso = (now_utc + timedelta(days=60)).isoformat().replace("+00:00", "Z")

    print(
        f"🔎 Graph calendarView lookup | user={user_principal} | start={start_iso} | end={end_iso}"
    )
    token = await get_graph_access_token()
    meetings: List[Dict[str, Any]] = []

    params = {
        "startDateTime": start_iso,
        "endDateTime": end_iso,
        "$top": "100",
        "$orderby": "start/dateTime",
        "$select": "id,subject,start,end,organizer,isOnlineMeeting,onlineMeeting,onlineMeetingUrl,webLink,type,seriesMasterId",
    }

    next_url = f"https://graph.microsoft.com/v1.0/users/{user_principal}/calendarView"

    async with httpx.AsyncClient() as client:
        while next_url:
            response = await client.get(
                next_url,
                headers={"Authorization": f"Bearer {token}"},
                params=params if next_url.endswith("/calendarView") else None,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code if exc.response else 500
                response_text = exc.response.text if exc.response else str(exc)
                if status_code == 403:
                    raise HTTPException(
                        status_code=403,
                        detail=(
                            "Graph calendar access forbidden for this mailbox. "
                            "Ensure app permissions include Calendars.Read and admin consent is granted. "
                            f"Mailbox: {user_principal}. Graph response: {response_text}"
                        ),
                    )
                raise HTTPException(
                    status_code=status_code,
                    detail=f"Graph calendarView failed for {user_principal}: {response_text}",
                )
            payload = response.json() or {}
            meetings.extend(payload.get("value", []))
            next_url = payload.get("@odata.nextLink")
            params = None

    print(f"✅ Graph calendarView lookup success | user={user_principal} | meetings={len(meetings)}")
    return meetings


@app.get("/teams/calendar/{email}")
async def get_teacher_calendar(
    email: str,
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    try:
        print(f"[teams_calendar] request email={email} start={start} end={end}")
        meetings = await get_user_meetings(email, start, end)
        print(f"[teams_calendar] meetings_raw={len(meetings)}")
        normalized = []
        for m in meetings:
            online = m.get("onlineMeeting") or {}
            organizer = (m.get("organizer") or {}).get("emailAddress") or {}
            normalized.append(
                {
                    "id": m.get("id"),
                    "subject": m.get("subject") or "Teams Meeting",
                    "start": (m.get("start") or {}).get("dateTime", ""),
                    "end": (m.get("end") or {}).get("dateTime", ""),
                    "start_timezone": (m.get("start") or {}).get("timeZone", ""),
                    "end_timezone": (m.get("end") or {}).get("timeZone", ""),
                    "organizer": organizer.get("name") or organizer.get("address") or "",
                    "organizer_email": organizer.get("address") or "",
                    "joinUrl": online.get("joinUrl") or m.get("onlineMeetingUrl") or "",
                    "webLink": m.get("webLink") or "",
                    "type": m.get("type") or "singleInstance",
                    "seriesMasterId": m.get("seriesMasterId") or "",
                }
            )

        print(f"[teams_calendar] meetings_normalized={len(normalized)}")
        if normalized:
            sample = normalized[0]
            print(
                "[teams_calendar] sample "
                f"subject={sample.get('subject')} start={sample.get('start')} end={sample.get('end')}"
            )

        return {
            "success": True,
            "email": email,
            "start": start,
            "end": end,
            "meetings": normalized,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ teams calendar fetch failed | email={email} | error={e}")
        raise HTTPException(status_code=500, detail=str(e))
    
async def get_meeting_transcripts(
    user_id,
    meeting_id
):
    try:
        print(f"🔎 Graph transcripts lookup | user_id={user_id} | meeting_id={meeting_id}")
        token = await get_graph_access_token()
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings/{meeting_id}/transcripts",
                headers={
                    "Authorization": f"Bearer {token}"
                }
            )
            if response.status_code != 200:
                print(f"⚠️ Graph transcripts lookup failed | user_id={user_id} | meeting_id={meeting_id} | status={response.status_code}")
                return []
            transcripts = response.json().get(
                "value",
                []
            )
            print(f"✅ Graph transcripts lookup success | user_id={user_id} | meeting_id={meeting_id} | transcripts={len(transcripts)}")
            return transcripts
    except Exception as e:
        print(
            f"Transcript lookup failed: {e}"
        )
        return []

async def download_transcript_preview(
    transcript_id, user_id
):
    try:
        token = await get_graph_access_token()
        async with httpx.AsyncClient() as client:
            response = await client.get(
                # f"https://graph.microsoft.com/v1.0/me/onlineMeetings/transcripts/{transcript_id}/content",
                f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings/transcripts/{transcript_id}/content",
                headers={
                    "Authorization": f"Bearer {token}"
                }
            )
            if response.status_code != 200:
                return ""
            return response.text
    except Exception as e:
        print(
            f"Transcript download failed: {e}"
        )
        return ""
    
# def await_fetch_transcript(
#     meeting_id, user_id):
#     try:
#         async def _inner():
#             print(f"🔎 Graph transcript content lookup | meeting_id={meeting_id}")
#             token = await get_graph_access_token()
#             async with httpx.AsyncClient() as client:
#                 response = await client.get(
#                     # f"https://graph.microsoft.com/v1.0/me/onlineMeetings/{meeting_id}/transcripts",
#                     f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings/{meeting_id}/transcripts",
#                     headers={
#                         "Authorization":
#                         f"Bearer {token}"
#                     }
#                 )
#                 if response.status_code != 200:
#                     print(f"⚠️ Graph transcript content lookup failed | meeting_id={meeting_id} | status={response.status_code}")
#                     return ""
#                 data = response.json()
#                 print(f"✅ Graph transcript content response | meeting_id={meeting_id} | rows={len(data.get('value', []))}")
#                 transcript_text = []
#                 for row in data.get(
#                     "value",
#                     []
#                 ):
#                     if row.get("content"):
#                         transcript_text.append(
#                             row["content"]
#                         )
#                 return "\n".join(
#                     transcript_text
#                 )
#         return asyncio.run(
#             _inner()
#         )
#     except Exception as e:
#         print(
#             f"Transcript fetch failed: {e}"
#         )
#         return ""

def await_fetch_transcript(meeting_id, user_id):
    try:
        async def _inner():
            print(f"🔎 Graph transcript list lookup | meeting_id={meeting_id} | user_id={user_id}")
            token = await get_graph_access_token()
            async with httpx.AsyncClient() as client:

                # ── CALL 1: Get list of transcript IDs for this meeting ──
                list_resp = await client.get(
                    f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings/{meeting_id}/transcripts",
                    headers={"Authorization": f"Bearer {token}"}
                )
                if list_resp.status_code != 200:
                    print(f"⚠️ Transcript list fetch failed | status={list_resp.status_code} | body={list_resp.text}")
                    return ""

                transcript_list = list_resp.json().get("value", [])
                print(f"✅ Found {len(transcript_list)} transcript(s) | meeting_id={meeting_id}")

                if not transcript_list:
                    print(f"⚠️ No transcripts available yet for meeting_id={meeting_id}")
                    return ""

                # ── CALL 2: For each transcript ID, fetch the actual text content ──
                transcript_text = []
                for row in transcript_list:
                    transcript_id = row.get("id")
                    if not transcript_id:
                        continue

                    print(f"📥 Fetching transcript content | transcript_id={transcript_id}")
                    content_resp = await client.get(
                        f"https://graph.microsoft.com/v1.0/users/{user_id}/onlineMeetings/{meeting_id}/transcripts/{transcript_id}/content",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "Accept": "text/vtt"   # VTT = timestamped text; use text/plain for plain text
                        }
                    )
                    if content_resp.status_code == 200:
                        transcript_text.append(content_resp.text)
                        print(f"✅ Transcript content fetched | transcript_id={transcript_id} | chars={len(content_resp.text)}")
                    else:
                        print(f"⚠️ Transcript content fetch failed | transcript_id={transcript_id} | status={content_resp.status_code} | body={content_resp.text}")

                return "\n".join(transcript_text)

        return asyncio.run(_inner())

    except Exception as e:
        print(f"❌ Transcript fetch failed | meeting_id={meeting_id} | error={e}")
        return ""

async def get_graph_access_token():
    token_url = (
        f"https://login.microsoftonline.com/"
        f"{MS365_TENANT_ID}"
        f"/oauth2/v2.0/token"
    )
    async with httpx.AsyncClient() as client:
        response = await client.post(
            token_url,
            data={
                "grant_type":
                "client_credentials",

                "client_id":
                MS365_CLIENT_ID,

                "client_secret":
                MS365_CLIENT_SECRET,

                "scope":
                "https://graph.microsoft.com/.default",
            }
        )
        response.raise_for_status()
        return response.json()[
            "access_token"
        ]

# --------- Till here

@app.on_event("startup")
async def start_kafka_consumer():
    """Start the meeting.booked consumer in a background thread on service startup."""
    t = threading.Thread(target=_consume_meeting_booked, daemon=True, name="kafka-meeting-booked")
    t.start()
    print("🚀 Kafka consumer thread started: meeting.booked")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("ms365_service:app", host="0.0.0.0", port=7700, reload=True)
