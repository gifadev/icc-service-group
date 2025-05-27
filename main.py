import asyncio
from fastapi import FastAPI, HTTPException, Depends, status, Form, WebSocket
import threading
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from typing import List 
from data_queries import (
    get_campaign_for_ws,
)
from auth import router as auth_router
from fastapi.security import OAuth2PasswordBearer
import jwt
from database_config import get_db_connection
import requests
import datetime
import psycopg2.extras
from broadcaster import manager
import os
from dotenv import load_dotenv

load_dotenv()


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sertakan router auth di bawah prefix /auth
app.include_router(auth_router, prefix="/auth")

# ==================== SETUP AUTENTIKASI ====================
SECRET_KEY = os.environ.get("SECRET_KEY")
ALGORITHM = os.environ.get("ALGORITHM")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token tidak valid1",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token tidak valid2",
            headers={"WWW-Authenticate": "Bearer"},
        )
    connection = get_db_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = "SELECT * FROM users WHERE id = %s"
        cursor.execute(query, (user_id,))
        user = cursor.fetchone()
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User tidak ditemukan",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        connection.close()


def require_role(allowed_roles: list):
    def role_checker(current_user: dict = Depends(get_current_user)):
        if current_user.get("role") not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient privileges"
            )
        return current_user
    return role_checker


def create_campaign(campaign_id: int, campaign_name: str, device_ids: list, group_id: int) -> int:
    created_at = datetime.datetime.utcnow()
    insert_campaign_query = """
        INSERT INTO campaign (id, name, status, group_id, time_start)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(insert_campaign_query, (campaign_id, campaign_name, 'active', group_id, created_at))
                generated_campaign_id = cursor.fetchone()[0]
                for device_id in device_ids:
                    cursor.execute(
                        "INSERT INTO campaign_devices (campaign_id, device_id) VALUES (%s, %s)",
                        (generated_campaign_id, device_id)
                    )
            connection.commit()
        return generated_campaign_id
    except Exception as e:
        raise e


def update_campaign_status(campaign_id, new_status):
    connection = get_db_connection()
    cursor = connection.cursor()
    sql = "UPDATE campaign SET status = %s WHERE id = %s"
    cursor.execute(sql, (new_status, campaign_id))
    connection.commit()
    cursor.close()
    connection.close()

def stop_campaign_devices(campaign_id: int):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Ambil device yang terlibat pada campaign aktif
        cursor.execute("""
            SELECT d.id, d.ip 
            FROM campaign_devices cd
            JOIN devices d ON cd.device_id = d.id
            WHERE cd.campaign_id = %s
        """, (campaign_id,))
        devices = cursor.fetchall()
    except Exception as e:
        raise Exception(f"Error retrieving devices for campaign {campaign_id}: {e}")
    finally:
        cursor.close()
        conn.close()

    # Untuk setiap device, kirim request stop dan update status
    for device in devices:
        ip = device.get("ip")
        if ip:
            url = f"http://{ip}:8003/stop-capture/{campaign_id}"
            try:
                resp = requests.get(url)
                resp_json = resp.json()
                # Update status device
                conn_update = get_db_connection()
                try:
                    cursor_update = conn_update.cursor()
                    update_query = "UPDATE devices SET is_running = FALSE WHERE id = %s"
                    cursor_update.execute(update_query, (device["id"],))
                    conn_update.commit()
                except Exception as ex:
                    print(f"Error updating is_running for device {device['id']}: {ex}")
                finally:
                    cursor_update.close()
                    conn_update.close()
            except Exception as e:
                print(f"Error stopping capture for device {device['id']} at {ip}: {e}")

    # Update campaign status dan time_stop
    try:
        conn_campaign = get_db_connection()
        cursor_campaign = conn_campaign.cursor()
        time_stop = datetime.datetime.utcnow()
        update_query_campaign = "UPDATE campaign SET status = %s, time_stop = %s WHERE id = %s"
        cursor_campaign.execute(update_query_campaign, ('stopped', time_stop, campaign_id))
        conn_campaign.commit()
    except Exception as e:
        raise Exception(f"Error updating campaign status: {e}")
    finally:
        cursor_campaign.close()
        conn_campaign.close()


stop_event = threading.Event()
capture_thread = None

# @app.post("/start-capture")
# async def start_capture(
#     campaign_id: int = Form(...),
#     campaign_name: str = Form(...),
#     device_ids: str = Form(...),
#     group_id: int = Form(...)
# ):
#     # === 1. Cek campaign aktif berdasarkan status ENUM 'active' ===
#     conn = get_db_connection()
#     active_campaign = None
#     try:
#         cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#         # Perbaikan: hilangkan koma ekstra setelah campaign_id
#         cursor.execute("""
#             SELECT c.id AS campaign_id
#             FROM campaign c 
#             WHERE c.status = 'active'
#             ORDER BY c.time_start DESC
#             LIMIT 1
#         """)
#         active_campaign = cursor.fetchone()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error checking active campaigns: {e}")
#     finally:
#         cursor.close()
#         conn.close()

#     # === 3. Parse device_ids menjadi list of int ===
#     try:
#         original_device_ids = [int(x.strip()) for x in device_ids.split(",") if x.strip()]
#     except Exception as e:
#         raise HTTPException(status_code=400, detail="Invalid device_ids format")
    
#     # === 5. Buat campaign baru dan broadcast info campaign ===
#     try:
#         # Panggil fungsi create_campaign dengan parameter lengkap
#         new_campaign_id = create_campaign(campaign_id, campaign_name, original_device_ids, group_id)
#         await manager.broadcast(f"Campaign '{campaign_name}' (ID: {new_campaign_id}) telah dimulai")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error creating campaign: {e}")
    
#     # Jalankan listener WS secara asynchronous setelah 10 detik (Untuk menerima data WS dari device)
#     async def run_ws_listener():
#         await asyncio.sleep(10)
#         process = await asyncio.create_subprocess_exec("python3", "wsReceivedata.py")
#     asyncio.create_task(run_ws_listener())

#     responses = []
#     # === 6. Untuk setiap device id yang dipilih, ambil IP dan kirim request ke device ===
#     for device_id in original_device_ids:
#         conn = get_db_connection()
#         try:
#             cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#             cursor.execute("SELECT ip FROM devices WHERE id = %s", (device_id,))
#             device = cursor.fetchone()
#         except Exception as e:
#             continue
#         finally:
#             cursor.close()
#             conn.close()
        
#         if device and device.get("ip"):
#             ip = device.get("ip")
#             url = f"http://{ip}:8003/start-capture"
#             data_payload = {
#                 "campaign_name": campaign_name,
#                 "campaign_id": new_campaign_id
#             }
#             try:
#                 resp = requests.post(url, data=data_payload)
#                 resp_json = resp.json()
#                 # === 7. Update status device dan campaign ===
#                 conn_update = get_db_connection()
#                 try:
#                     cursor_update = conn_update.cursor()
#                     # Update device status: set is_running = TRUE
#                     update_query = "UPDATE devices SET is_running = TRUE WHERE id = %s"
#                     cursor_update.execute(update_query, (device_id,))
#                     # Update campaign status: set status ke 'active'
#                     update_query = "UPDATE campaign SET status = 'active' WHERE id = %s"
#                     cursor_update.execute(update_query, (new_campaign_id,))
#                     conn_update.commit()
#                 except Exception as ex:
#                     print(f"Error updating is_running for device {device_id}: {ex}")
#                 finally:
#                     cursor_update.close()
#                     conn_update.close()
#             except Exception as e:
#                 resp_json = {"error": str(e)}
#             responses.append({"device_id": device_id, "ip": ip, "response": resp_json})
    
#     return {
#         "message": "Live capture started successfully",
#         "campaign_id": new_campaign_id,
#         "campaign_name": campaign_name,
#         "device_responses": responses
#     }
@app.post("/start-capture")
async def start_capture(
    campaign_id: int = Form(...),
    campaign_name: str = Form(...),
    device_ips: str = Form(...), 
    group_id: int = Form(...)
):
    # === 1. Cek campaign aktif berdasarkan status ENUM 'active' ===
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM campaign
                WHERE status = 'active'
                  AND group_id = %s
                LIMIT 1
                """,
                (group_id,)
            )
            if cursor.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail=f"Group {group_id} already has an active campaign. Cannot create a new one"
                )
    except HTTPException:
        # lempar ulang HTTPException agar FastAPI bisa tangani
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking active campaign: {e}")
    finally:
        conn.close()

    # === 2. Parse device_ips menjadi list of IP string ===
    try:
        original_device_ips = [x.strip() for x in device_ips.split(",") if x.strip()]
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid device_ips format")
    
    # === 3. Konversi IP menjadi device_id dengan query ke database ===
    device_ids = []
    for ip in original_device_ips:
        conn = get_db_connection()
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SELECT id FROM devices WHERE ip = %s", (ip,))
            result = cursor.fetchone()
            if result and result.get("id"):
                device_ids.append(result["id"])
            else:
                raise HTTPException(status_code=404, detail=f"Device dengan IP {ip} tidak ditemukan di database")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error converting IP ke device_id: {e}")
        finally:
            cursor.close()
            conn.close()

    # === 4. Buat campaign baru dan broadcast info campaign ===
    try:
        new_campaign_id = create_campaign(campaign_id, campaign_name, device_ids, group_id)
        await manager.broadcast(new_campaign_id, f"Campaign '{campaign_name}' (ID: {new_campaign_id}) telah dimulai")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating campaign: {e}")
    
    # Jalankan listener WS secara asynchronous setelah 10 detik
    async def run_ws_listener():
        await asyncio.sleep(10)
        process = await asyncio.create_subprocess_exec("python3", "wsReceivedata.py")
    asyncio.create_task(run_ws_listener())

    responses = []
    # === 5. Untuk setiap device berdasarkan IP, kirim request ke device dan update status berdasarkan IP ===
    for ip in original_device_ips:
        url = f"http://{ip}:8003/start-capture"
        data_payload = {
            "campaign_name": campaign_name,
            "campaign_id": new_campaign_id
        }
        try:
            resp = requests.post(url, data=data_payload)
            resp_json = resp.json()
            # Update status device di database berdasarkan IP
            conn_update = get_db_connection()
            try:
                cursor_update = conn_update.cursor()
                update_device_query = "UPDATE devices SET is_running = TRUE WHERE ip = %s"
                cursor_update.execute(update_device_query, (ip,))
                update_campaign_query = "UPDATE campaign SET status = 'active' WHERE id = %s"
                cursor_update.execute(update_campaign_query, (new_campaign_id,))
                conn_update.commit()
            except Exception as ex:
                print(f"Error updating device untuk IP {ip}: {ex}")
            finally:
                cursor_update.close()
                conn_update.close()
        except Exception as e:
            resp_json = {"error": str(e)}
        responses.append({"ip": ip, "response": resp_json})
    
    return {
        "message": "Live capture started successfully",
        "campaign_id": new_campaign_id,
        "campaign_name": campaign_name,
        "device_responses": responses
    }


@app.websocket("/ws/{campaign_id}")
async def websocket_endpoint(websocket: WebSocket, campaign_id: int):
    # await manager.connect(websocket)
    await manager.connect(campaign_id, websocket)
    print(f" Klien WebSocket terhubung untuk campaign {campaign_id}")
    try:
        while True:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SELECT status FROM campaign WHERE id = %s", (campaign_id,))
            campaign = cursor.fetchone()
            campaign_data = get_campaign_for_ws(campaign_id)
            if not campaign_data or campaign["status"] == "stopped":
                print(f"Campaign {campaign_id} telah dihentikan. Menutup koneksi WebSocket.")
                await websocket.send_json({
                    "message": "Campaign has been stopped.",
                })
                
                # Tutup koneksi WebSocket
                await websocket.close()
                break

            if campaign["status"] == "paused":
                await websocket.send_json({
                    "message": "Campaign is paused.",
                    "data": campaign_data
                    })
            else:
                await websocket.send_json({
                    "message": "send data campaign.",
                    "data": campaign_data
                })

            await asyncio.sleep(5)
    except Exception as e:
        print(f"Klien terputus dari campaign {campaign_id}: {e}")
    finally:
        manager.disconnect(websocket)



@app.post("/pause-capture")
async def pause_capture(
    campaign_id: int = Form(...),
    current_user: dict = Depends(require_role(["admin", "superadmin"]))
):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT status FROM campaign WHERE id = %s", (campaign_id,))
        campaign = cursor.fetchone()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        # if campaign["status"] != "active":
        #     raise HTTPException(status_code=400, detail="Campaign is not active; cannot pause.")
        # Update status menjadi 'paused'
        cursor.execute("UPDATE campaign SET status = 'paused' WHERE id = %s", (campaign_id,))
        conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error pausing campaign: {e}")
    finally:
        cursor.close()
        conn.close()
    return {
        "message": f"Campaign {campaign_id} paused successfully."
        }

@app.post("/resume-capture")
async def resume_capture(
    campaign_id: int = Form(...)
):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT status FROM campaign WHERE id = %s", (campaign_id,))
        campaign = cursor.fetchone()
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found.")
        if campaign["status"] != "paused":
            raise HTTPException(status_code=400, detail="Campaign is not paused; cannot resume.")
        # Update status menjadi 'active'
        cursor.execute("UPDATE campaign SET status = 'active' WHERE id = %s", (campaign_id,))
        conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resuming campaign: {e}")
    finally:
        cursor.close()
        conn.close()
    return {"message": f"Campaign {campaign_id} resumed successfully."}


@app.post("/stop-capture")
async def stop_capture(
    campaign_id: int = Form(...)
):
    import datetime
    # Ambil daftar device_id yang terkait dengan campaign dari tabel campaign_devices
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT device_id FROM campaign_devices WHERE campaign_id = %s", (campaign_id,))
        campaign_devices = cursor.fetchall()
        device_ids_list = [row["device_id"] for row in campaign_devices]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving campaign devices: {e}")
    finally:
        cursor.close()
        conn.close()
    
    if not device_ids_list:
        raise HTTPException(status_code=400, detail="No devices associated with this campaign.")

    responses = []
    # Untuk setiap device yang terkait, ambil IP dari tabel devices dan kirim request stop-capture ke device
    for device_id in device_ids_list:
        conn = get_db_connection()
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("SELECT ip FROM devices WHERE id = %s", (device_id,))
            device = cursor.fetchone()
        except Exception as e:
            print(f"Error retrieving device {device_id}: {e}")
            continue
        finally:
            cursor.close()
            conn.close()
        
        if device and device.get("ip"):
            ip = device.get("ip")
            url = f"http://{ip}:8003/stop-capture/{campaign_id}"
            try:
                resp = requests.get(url)
                resp_json = resp.json()
                # Update status device: set is_running = FALSE
                conn_update = get_db_connection()
                try:
                    cursor_update = conn_update.cursor()
                    update_query = "UPDATE devices SET is_running = FALSE WHERE id = %s"
                    cursor_update.execute(update_query, (device_id,))
                    conn_update.commit()
                except Exception as ex:
                    print(f"Error updating is_running for device {device_id}: {ex}")
                finally:
                    cursor_update.close()
                    conn_update.close()
            except Exception as e:
                resp_json = {"error": str(e)}
            responses.append({"device_id": device_id, "ip": ip, "response": resp_json})
    
    # Setelah mengirim perintah stop ke setiap device, update campaign status (misalnya, set status = FALSE dan catat time_stop)
    try:
        conn_campaign = get_db_connection()
        cursor_campaign = conn_campaign.cursor()
        time_stop = datetime.datetime.utcnow()
        update_query_campaign = "UPDATE campaign SET status = 'stop', time_stop = %s WHERE id = %s"
        cursor_campaign.execute(update_query_campaign, (time_stop, campaign_id))
        conn_campaign.commit()
    except Exception as e:
        print(f"Error updating campaign status: {e}")
    finally:
        cursor_campaign.close()
        conn_campaign.close()
    
    await manager.broadcast(campaign_id, "Campaign has been stopped.")
    await manager.close_campaign_connections(campaign_id)

    return {
        "message": "Live capture stopped on devices associated with the campaign",
        "campaign_id": campaign_id,
        "device_responses": responses
    }

@app.post("/add-devices", status_code=201)
async def add_device(
    serial_number: str = Form(...),
    ip: str = Form(...),
    lat: float = Form(None),
    long: float = Form(None)
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = """
                INSERT INTO devices (
                    serial_number, ip, lat, long, is_connected, is_running, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (serial_number) DO UPDATE SET
                    ip = EXCLUDED.ip,
                    lat = EXCLUDED.lat,
                    long = EXCLUDED.long,
                    is_connected = EXCLUDED.is_connected,
                    is_running = EXCLUDED.is_running;
                """
                cur.execute(query, (serial_number, ip, lat, long, True, False))
        return {"message": "Device added/updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/devices", response_model=List[dict], status_code=200)
async def get_all_devices():
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT id, serial_number, ip, lat, long, is_connected, is_running, created_at
                    FROM devices
                    ORDER BY created_at DESC
                """)
                devices = cur.fetchall()
                return [dict(device) for device in devices]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.put("/edit-device/{device_id}", status_code=200)
async def edit_device_by_id(
    device_id: int,
    serial_number: str = Form(None),
    ip: str = Form(None),
    lat: float = Form(None),
    long: float = Form(None),
    is_connected: bool = Form(None),
    is_running: bool = Form(None)
):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                query = "UPDATE devices SET "
                updates = []
                params = []
                
                if serial_number is not None:
                    updates.append("serial_number = %s")
                    params.append(serial_number)
                if ip is not None:
                    updates.append("ip = %s")
                    params.append(ip)
                if lat is not None:
                    updates.append("lat = %s")
                    params.append(lat)
                if long is not None:
                    updates.append("long = %s")
                    params.append(long)
                if is_connected is not None:
                    updates.append("is_connected = %s")
                    params.append(is_connected)
                if is_running is not None:
                    updates.append("is_running = %s")
                    params.append(is_running)
                
                if not updates:
                    raise HTTPException(status_code=400, detail="No fields to update")
                
                query += ", ".join(updates) + " WHERE id = %s"
                params.append(device_id)
                
                cur.execute(query, params)
                
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Device not found")
                
        return {"message": "Device updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.delete("/devices/{device_id}", status_code=200)
async def delete_device(device_id: int):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM campaign_devices WHERE device_id = %s", (device_id,))
                cur.execute("DELETE FROM lte_data WHERE device_id = %s", (device_id,))
                cur.execute("DELETE FROM gsm_data WHERE device_id = %s", (device_id,))
                cur.execute("DELETE FROM devices WHERE id = %s", (device_id,))
                
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Device not found")
                
        return {
            "message": "Device deleted successfully",
            "details": {
                "campaign_relations_deleted": cur.rowcount,
                "device_deleted": device_id
            }
        }
    
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Database operation failed",
                "message": str(e),
                "solution": "Ensure no other tables reference this device"
            }
        )
    
    finally:
        conn.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8004)
