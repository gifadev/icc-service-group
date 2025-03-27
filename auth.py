from fastapi import APIRouter, HTTPException, Depends, Form
from datetime import datetime
import psycopg2
import psycopg2.extras
from database_config import get_db_connection
import jwt
from cryptography.fernet import Fernet
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

KEY = os.environ.get("KEY").encode() 
fernet = Fernet(KEY)

def encrypt_password(password: str) -> str:
    encrypted = fernet.encrypt(password.encode())
    return encrypted.decode()

def decrypt_password(encrypted_password: str) -> str:
    decrypted = fernet.decrypt(encrypted_password.encode())
    return decrypted.decode()

def verify_password(plain_password: str, encrypted_password: str) -> bool:
    try:
        return plain_password == decrypt_password(encrypted_password)
    except Exception:
        return False
    
# Konfigurasi JWT
SECRET_KEY = os.environ.get("SECRET_KEY")
ALGORITHM = os.environ.get("ALGORITHM")

def create_access_token(data: dict) -> str:
    token = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

@router.post("/register")
def register(
    username: str = Form(...),
    password: str = Form(...),
    group_id: int = Form(...)
):
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = "SELECT id FROM users WHERE username = %s"
        cursor.execute(query, (username,)) 
        existing = cursor.fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="Username already registered")
        
        encrypted_pw = encrypt_password(password)
        created_at = datetime.utcnow()

        insert_query = """
            INSERT INTO users (username, password, role, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        cursor.execute(insert_query, (username, encrypted_pw, 'admin', created_at))
        user_id = cursor.fetchone()["id"]
        connection.commit()
        
        cursor.execute("SELECT user_id FROM device_group WHERE id = %s", (group_id,))
        group_info = cursor.fetchone()
        if group_info is None:
            raise HTTPException(status_code=400, detail="Selected group not found.")
        if group_info["user_id"] is not None:
            raise HTTPException(status_code=400, detail="Selected group already has an owner.")
        
        cursor.execute("UPDATE device_group SET user_id = %s WHERE id = %s", (user_id, group_id))
        connection.commit()
        
        return {
            "status": "success",
            "msg": "User created successfully",
            "user": {
                "id": user_id,
                "username": username,
                "group_id": group_id,
                "created_at": created_at.isoformat()
            }
        }
    except psycopg2.Error as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        connection.rollback()
        raise HTTPException(status_code=400, detail=f"Error: {e}")
    finally:
        cursor.close()
        connection.close()


@router.post("/login")
def login(
    username: str = Form(...),
    password: str = Form(...)
):
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query, (username,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        
        if not verify_password(password, user["password"]):
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        
        access_token = create_access_token(data={"sub": str(user["id"])})
        return {
            "status": "success",
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user["id"],
                "username": user["username"],
                "role": user["role"]
            }
        }
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {e}")
    finally:
        cursor.close()
        connection.close()
