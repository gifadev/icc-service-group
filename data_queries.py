# data_queries.py
import psycopg2
import psycopg2.extras
from database_config import get_db_connection  
from auth import encrypt_password, fernet
from fastapi import HTTPException
import datetime


def create_campaign(campaign_name: str, user_id: int, device_ids: list) -> int:
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        created_at = datetime.datetime.utcnow()
        insert_query = """
            INSERT INTO campaign (name, status, time_start, user_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        cursor.execute(insert_query, (campaign_name, 'active', created_at, user_id))
        campaign_id = cursor.fetchone()[0]
        for device_id in device_ids:
            cursor.execute("INSERT INTO campaign_devices (campaign_id, device_id) VALUES (%s, %s)",
                           (campaign_id, device_id))
        connection.commit()
        return campaign_id
    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()


def get_latest_campaign_with_data():
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil campaign terbaru; gunakan kolom time_start sebagai timestamp
        cursor.execute("SELECT id, time_start AS timestamp FROM campaign ORDER BY id DESC LIMIT 1")
        latest_campaign = cursor.fetchone()
        
        if not latest_campaign:
            return None
        id_campaign = latest_campaign['id']
        id_device = latest_campaign['id_device']
        
        # Ambil data GSM
        cursor.execute("SELECT * FROM gsm_data WHERE id_campaign = %s", (id_campaign,))
        gsm_data = cursor.fetchall()
        
        # Ambil data LTE
        cursor.execute("SELECT * FROM lte_data WHERE id_campaign = %s", (id_campaign,))
        lte_data = cursor.fetchall()

        cursor.execute("""
            SELECT d.*, dg.group_name 
            FROM devices d 
            JOIN device_group dg ON d.group_id = dg.id 
            WHERE d.id = %s
        """, (id_device,))
        device_data = cursor.fetchone()

        gsm_count = len(gsm_data)
        lte_count = len(lte_data)
        total = gsm_count + lte_count
        
        return {
            "status": "success",
            "id_campaign": latest_campaign['id'],
            "device": device_data,
            "timestamp": latest_campaign['timestamp'],
            "gsm_data": gsm_data,
            "lte_data": lte_data,
            "gsm_count": gsm_count, 
            "lte_count": lte_count,
            "total_count": total 
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_campaign_data_by_id(id_campaign):
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil informasi campaign; gunakan time_start sebagai timestamp
        cursor.execute("SELECT id, time_start AS timestamp FROM campaign WHERE id = %s", (id_campaign,))
        campaign = cursor.fetchone()
        
        if not campaign:
            return None
        
        # Ambil data GSM
        cursor.execute("SELECT * FROM gsm_data WHERE id_campaign = %s", (id_campaign,))
        gsm_data = cursor.fetchall()
        
        # Ambil data LTE
        cursor.execute("SELECT * FROM lte_data WHERE id_campaign = %s", (id_campaign,))
        lte_data = cursor.fetchall()

        gsm_count = len(gsm_data)
        lte_count = len(lte_data)
        total = gsm_count + lte_count
        
        return {
            "status": "success",
            "id_campaign": campaign['id'],
            "timestamp": campaign['timestamp'],
            "gsm_data": gsm_data,
            "lte_data": lte_data,
            "gsm_count": gsm_count, 
            "lte_count": lte_count,
            "total_count": total 
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_all_campaigns_data():
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Ambil data campaign
        cursor.execute("""
            SELECT id, name, status, time_start, time_stop 
            FROM campaign 
            ORDER BY id DESC
        """)
        campaigns = cursor.fetchall()
        
        result = {
            "status": "success",
            "campaigns": []
        }
        
        for campaign in campaigns:
            campaign_id = campaign['id']

            count_cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            count_cursor.execute(
                "SELECT COUNT(*) as cnt FROM gsm_data WHERE campaign_id = %s AND status = FALSE",
                (campaign_id,)
            )
            threat_gsm = count_cursor.fetchone()['cnt']
            count_cursor.execute(
                "SELECT COUNT(*) as cnt FROM lte_data WHERE campaign_id = %s AND status = FALSE",
                (campaign_id,)
            )
            threat_lte = count_cursor.fetchone()['cnt']
            threat_bts_count = threat_gsm + threat_lte

            count_cursor.execute(
                "SELECT COUNT(*) as cnt FROM gsm_data WHERE campaign_id = %s AND status = TRUE",
                (campaign_id,)
            )
            real_gsm = count_cursor.fetchone()['cnt']
            count_cursor.execute(
                "SELECT COUNT(*) as cnt FROM lte_data WHERE campaign_id = %s AND status = TRUE",
                (campaign_id,)
            )
            real_lte = count_cursor.fetchone()['cnt']
            real_bts_count = real_gsm + real_lte
            
            # Query untuk mengambil device yang terkait dengan campaign melalui tabel campaign_devices.
            count_cursor.execute("""
                SELECT 
                    d.id AS device_id,
                    d.serial_number,
                    d.ip,
                    d.is_connected,
                    d.is_running,
                    d.created_at AS device_created_at,
                    dg.id AS group_id,
                    dg.group_name,
                    dg.description,
                    dg.created_at AS group_created_at
                FROM campaign_devices cd
                JOIN devices d ON cd.device_id = d.id
                LEFT JOIN device_group dg ON d.group_id = dg.id
                WHERE cd.campaign_id = %s
                ORDER BY d.id
            """, (campaign_id,))
            devices = count_cursor.fetchall()

            device_list = []
            for row in devices:
                device_info = {
                    "id": row["device_id"],
                    "serial_number": row["serial_number"],
                    "ip": row["ip"],
                    "is_connected": row["is_connected"],
                    "is_running": row["is_running"],
                    "created_at": row["device_created_at"]
                }
                if row["group_id"] is not None:
                    device_info["group"] = {
                        "id": row["group_id"],
                        "group_name": row["group_name"],
                        "description": row["description"],
                        "created_at": row["group_created_at"]
                    }
                else:
                    device_info["group"] = None
                device_list.append(device_info)
            
            campaign_data = {
                "id_campaign": campaign_id,
                "name": campaign.get('name', ''),
                "status": campaign['status'],
                "time_start": campaign['time_start'],
                "time_stop": campaign['time_stop'],
                "threat_bts_count": threat_bts_count,
                "real_bts_count": real_bts_count,
                "devices": device_list  # Sertakan informasi device dan group
            }
            result["campaigns"].append(campaign_data)
            count_cursor.close()
        
        return result
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_latest_campaign_with_unified_data(page: int = 1, limit: int = 10):
    """
    Menggabungkan data GSM dan LTE dari campaign terbaru, melakukan pagination terpadu,
    dan mengembalikan:
      - Data campaign (id, name, status, time_start, time_stop)
      - List data GSM dan LTE (masing-masing sudah diberi tanda 'type')
      - Jumlah BTS total, threat BTS (status False), dan real BTS (status True)
      - Informasi paging: page dan limit
    """
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil campaign terbaru beserta kolom time_start dan time_stop
        cursor.execute("""
            SELECT id, name, status, time_start, time_stop 
            FROM campaign 
            ORDER BY id DESC 
            LIMIT 1
        """)
        latest_campaign = cursor.fetchone()
        if not latest_campaign:
            return None

        id_campaign = latest_campaign['id']
        
        # Ambil data GSM dan tandai dengan tipe 'gsm'
        cursor.execute("SELECT * FROM gsm_data WHERE campaign_id = %s", (id_campaign,))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Ambil data LTE dan tandai dengan tipe 'lte'
        cursor.execute("SELECT * FROM lte_data WHERE campaign_id = %s", (id_campaign,))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan data GSM dan LTE
        combined_data = gsm_data + lte_data
        total_count = len(combined_data)
        # Hitung threat BTS: misalnya, status False dianggap threat
        threat_bts_count = sum(1 for item in combined_data if not item.get("status"))
        # Hitung real BTS: status True
        real_bts_count = sum(1 for item in combined_data if item.get("status"))
        
        # Lakukan pagination pada data gabungan
        start_index = (page - 1) * limit
        end_index = start_index + limit
        paginated_data = combined_data[start_index:end_index]
        
        gsm_data_paginated = [item for item in paginated_data if item['type'] == 'gsm']
        lte_data_paginated = [item for item in paginated_data if item['type'] == 'lte']
        print("ini lte data ",lte_data_paginated)
        
        return {
            "status": "success",
            "campaign": {
                "id_campaign": latest_campaign['id'],
                "name": latest_campaign.get("name", ""),
                "status": latest_campaign["status"],
                "time_start": latest_campaign["time_start"],
                "time_stop": latest_campaign["time_stop"]
            },
            "gsm_data": gsm_data_paginated,
            "lte_data": lte_data_paginated,
            "page": page,
            "limit": limit,
            "gsm_count": len(gsm_data),
            "lte_count": len(lte_data),
            "total_bts": total_count,
            "threat_bts_count": threat_bts_count,
            "real_bts_count": real_bts_count
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_campaign_with_unified_data_by_id(campaign_id: int, page: int = 1, limit: int = 10):
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil data campaign lengkap (termasuk time_start dan time_stop)
        cursor.execute(
            "SELECT id, name, status, time_start, time_stop FROM campaign WHERE id = %s",
            (campaign_id,)
        )
        campaign = cursor.fetchone()
        if not campaign:
            return None
        
        # Ambil data GSM untuk campaign_id tertentu
        cursor.execute("SELECT * FROM gsm_data WHERE campaign_id = %s", (campaign_id,))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Ambil data LTE untuk campaign_id tertentu
        cursor.execute("SELECT * FROM lte_data WHERE campaign_id = %s", (campaign_id,))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan data GSM dan LTE
        combined_data = gsm_data + lte_data
        total_count = len(combined_data)
        # Hitung jumlah threat BTS (status False) dan real BTS (status True)
        threat_bts_count = sum(1 for item in combined_data if item.get("status") == False)
        real_bts_count = sum(1 for item in combined_data if item.get("status") == True)
        
        # Lakukan pagination pada data gabungan
        start_index = (page - 1) * limit
        end_index = start_index + limit
        paginated_data = combined_data[start_index:end_index]
        
        gsm_data_paginated = [item for item in paginated_data if item['type'] == 'gsm']
        lte_data_paginated = [item for item in paginated_data if item['type'] == 'lte']
        
        # --- Bagian Baru: Ambil informasi device terkait campaign ---
        # Asumsi: relasi campaign dengan device tersimpan di tabel campaign_devices
        cursor.execute("""
            SELECT 
                d.id AS device_id,
                d.serial_number,
                d.ip,
                d.is_connected,
                d.is_running,
                d.created_at AS device_created_at
            FROM campaign_devices cd
            JOIN devices d ON cd.device_id = d.id
            WHERE cd.campaign_id = %s
            ORDER BY d.id
        """, (campaign_id,))
        devices = cursor.fetchall()
        
        return {
            "status": "success",
            "campaign": {
                "id": campaign["id"],
                "name": campaign.get("name", ""),
                "status": campaign["status"],
                "time_start": campaign["time_start"],
                "time_stop": campaign["time_stop"],
            },
            "devices": devices,
            "gsm_data": gsm_data_paginated,
            "lte_data": lte_data_paginated, 
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "gsm_total": len(gsm_data),
            "lte_total": len(lte_data),
            "threat_bts_count": threat_bts_count,
            "real_bts_count": real_bts_count
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()



def get_campaign_for_ws(campaign_id: int):
    try:
        connection = get_db_connection()
        if connection is None:
            print("Koneksi database gagal!")
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Ambil data campaign lengkap
        cursor.execute(
            "SELECT id, name, group_id, status, time_start, time_stop FROM campaign WHERE id = %s",
            (campaign_id,)
        )
        campaign = cursor.fetchone()

        if not campaign:
            print(f"Campaign dengan ID {campaign_id} tidak ditemukan!")
            return None

        # Konversi datetime ke string ISO 8601
        campaign["time_start"] = campaign["time_start"].isoformat() if isinstance(campaign["time_start"], datetime.datetime) else None
        campaign["time_stop"] = campaign["time_stop"].isoformat() if isinstance(campaign["time_stop"], datetime.datetime) else None

        # Ambil data GSM dengan join ke tabel devices untuk mendapatkan info device
        cursor.execute("""
            SELECT g.*, d.ip AS device_ip 
            FROM gsm_data g 
            JOIN devices d ON g.device_id = d.id 
            WHERE g.campaign_id = %s
        """, (campaign_id,))
        gsm_data = cursor.fetchall()

        # Ambil data LTE dengan join ke tabel devices untuk mendapatkan info device
        cursor.execute("""
            SELECT l.*, d.ip AS device_ip 
            FROM lte_data l 
            JOIN devices d ON l.device_id = d.id 
            WHERE l.campaign_id = %s
        """, (campaign_id,))
        lte_data = cursor.fetchall()

        # Ambil data devices terkait dengan campaign melalui many-to-many campaign_devices
        cursor.execute("""
            SELECT d.* 
            FROM campaign_devices cd
            JOIN devices d ON cd.device_id = d.id
            WHERE cd.campaign_id = %s
        """, (campaign_id,))
        devices = cursor.fetchall()

        # Fungsi untuk membersihkan data: mengubah RealDictRow ke dictionary biasa dan konversi datetime ke ISO string
        def clean_data(row):
            row_dict = dict(row)
            # Jika field device_ip ada, tambahkan key 'ip'
            if "device_ip" in row_dict:
                row_dict["ip"] = row_dict["device_ip"]
            return {
                k: (v.isoformat() if isinstance(v, datetime.datetime) else v)
                for k, v in row_dict.items()
            }

        gsm_data_cleaned = [clean_data(row) for row in gsm_data]
        lte_data_cleaned = [clean_data(row) for row in lte_data]
        devices_cleaned = [clean_data(row) for row in devices]

        # Tandai tiap data dengan tipe masing-masing
        for row in gsm_data_cleaned:
            row["type"] = "gsm"
        for row in lte_data_cleaned:
            row["type"] = "lte"

        # Gabungkan data GSM dan LTE untuk perhitungan statistik (opsional)
        combined_data = gsm_data_cleaned + lte_data_cleaned
        total_count = len(combined_data)
        threat_bts_count = sum(1 for item in combined_data if item.get("status") is False)
        real_bts_count = sum(1 for item in combined_data if item.get("status") is True)

        result = {
            "status": "success",
            "campaign": campaign,
            "gsm_data": gsm_data_cleaned,
            "lte_data": lte_data_cleaned,
            "devices": devices_cleaned,
            "total_count": total_count,
            "gsm_total": len(gsm_data_cleaned),
            "lte_total": len(lte_data_cleaned),
            "threat_bts_count": threat_bts_count,
            "real_bts_count": real_bts_count
        }
        return result

    except Exception as e:
        print(f"Error di get_campaign_for_ws: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()



# def delete_campaign_by_id(id_campaign: int):
#     try:
#         connection = get_db_connection()
#         if connection is None:
#             return None

#         cursor = connection.cursor()
        
#         cursor.execute("DELETE FROM gsm_data WHERE id_campaign = %s", (id_campaign,))
#         cursor.execute("DELETE FROM lte_data WHERE id_campaign = %s", (id_campaign,))
#         cursor.execute("DELETE FROM campaign WHERE id = %s", (id_campaign,))
        
#         connection.commit()
#         return True
#     except Exception as e:
#         print(f"Error: {e}")
#         return None
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()

# def search_campaign_data(id_campaign: int, query: str):
#     try:
#         connection = get_db_connection()
#         if connection is None:
#             return None

#         cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
#         like_pattern = f"%{query}%"
        
#         gsm_query = """
#             SELECT * FROM gsm_data
#             WHERE id_campaign = %s AND (
#                 CAST(mcc AS TEXT) LIKE %s OR 
#                 CAST(mnc AS TEXT) LIKE %s OR 
#                 operator LIKE %s OR 
#                 CAST(local_area_code AS TEXT) LIKE %s OR 
#                 CAST(cell_identity AS TEXT) LIKE %s
#             )
#         """
#         cursor.execute(gsm_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
#         gsm_data = cursor.fetchall()
        
#         lte_query = """
#             SELECT * FROM lte_data
#             WHERE id_campaign = %s AND (
#                 mcc LIKE %s OR 
#                 mnc LIKE %s OR 
#                 operator LIKE %s OR 
#                 CAST(cell_identity AS TEXT) LIKE %s OR 
#                 tracking_area_code LIKE %s OR 
#                 frequency_band_indicator LIKE %s
#             )
#         """
#         cursor.execute(lte_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
#         lte_data = cursor.fetchall()
        
#         gsm_count = len(gsm_data)
#         lte_count = len(lte_data)
#         total_count = gsm_count + lte_count
        
#         return {
#             "status": "success",
#             "id_campaign": id_campaign,
#             "gsm_data": gsm_data,
#             "lte_data": lte_data,
#             "gsm_count": gsm_count,
#             "lte_count": lte_count,
#             "total_count": total_count
#         }
        
#     except Exception as e:
#         print(f"Error: {e}")
#         return None
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()

def search_campaign_data_paginate(id_campaign: int, query: str, page: int = 1, limit: int = 10):
    try:
        connection = get_db_connection()
        if connection is None:
            print("db tidak connect")
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        like_pattern = f"%{query}%"
        
        # Query untuk tabel GSM (casting kolom numeric ke text dan menggunakan ILIKE)
        gsm_query = """
            SELECT * FROM gsm_data
            WHERE campaign_id = %s AND (
                CAST(mcc AS TEXT) ILIKE %s OR 
                CAST(mnc AS TEXT) ILIKE %s OR 
                operator ILIKE %s OR 
                CAST(local_area_code AS TEXT) ILIKE %s OR 
                CAST(cell_identity AS TEXT) ILIKE %s
            )
        """
        cursor.execute(gsm_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
        gsm_data = cursor.fetchall()
        for row in gsm_data:
            row['type'] = 'gsm'
        
        # Query untuk tabel LTE dengan ILIKE
        lte_query = """
            SELECT * FROM lte_data
            WHERE campaign_id = %s AND (
                mcc ILIKE %s OR 
                mnc ILIKE %s OR 
                operator ILIKE %s OR 
                CAST(cell_identity AS TEXT) ILIKE %s OR 
                tracking_area_code ILIKE %s OR 
                frequency_band_indicator ILIKE %s
            )
        """
        cursor.execute(lte_query, (id_campaign, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern))
        lte_data = cursor.fetchall()
        for row in lte_data:
            row['type'] = 'lte'
        
        # Gabungkan hasil pencarian dari kedua tabel
        combined_data = gsm_data + lte_data
        total_count = len(combined_data)
        
        # Lakukan pagination pada data gabungan
        start_index = (page - 1) * limit
        end_index = start_index + limit
        paginated_data = combined_data[start_index:end_index]
        
        # Pisahkan data hasil pagination berdasarkan tipe
        gsm_data_paginated = [item for item in paginated_data if item['type'] == 'gsm']
        lte_data_paginated = [item for item in paginated_data if item['type'] == 'lte']
        
        return {
            "status": "success",
            "id_campaign": id_campaign,
            "gsm_data": gsm_data_paginated,
            "lte_data": lte_data_paginated,
            "page": page,
            "limit": limit,
            "total_count": total_count,
            "gsm_total": len(gsm_data),
            "lte_total": len(lte_data)
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()


def list_devices():
    connection = get_db_connection()
    try:
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT 
                d.id AS device_id,
                d.serial_number,
                d.ip,
                d.is_connected,
                d.is_running,
                d.lat,
                d.long,
                d.created_at AS device_created_at,
                dg.id AS group_id,
                dg.group_name,
                dg.description,
                dg.created_at AS group_created_at
            FROM devices d
            LEFT JOIN device_group dg ON d.group_id = dg.id
            ORDER BY d.id ASC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        devices = []
        for row in rows:
            device = {
                "id": row["device_id"],
                "serial_number": row["serial_number"],
                "ip": row["ip"],
                "is_connected": row["is_connected"],
                "is_running": row["is_running"],
                "lat":row["lat"],
                "long":row["long"],
                "created_at": row["device_created_at"]
            }
            # Jika device memiliki group, masukkan detail group ke dalam sub-objek
            if row["group_id"] is not None:
                device["group"] = {
                    "id": row["group_id"],
                    "group_name": row["group_name"],
                    "description": row["description"],
                    "created_at": row["group_created_at"]
                }
            else:
                device["group"] = None
            devices.append(device)
            
        return {"status": "success", "devices": devices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        connection.close()


def device_information():
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        
        # Inactive count: device yang tidak terhubung (is_connected = FALSE)
        cursor.execute("SELECT COUNT(*) FROM devices WHERE is_connected = FALSE")
        inactive_count = cursor.fetchone()[0]

        # Available count: device yang terhubung tapi tidak running (is_connected = TRUE AND is_running = FALSE)
        cursor.execute("SELECT COUNT(*) FROM devices WHERE is_connected = TRUE AND is_running = FALSE")
        available_count = cursor.fetchone()[0]

        # Running count: device yang sedang running (is_running = TRUE)
        cursor.execute("SELECT COUNT(*) FROM devices WHERE is_running = TRUE")
        running_count = cursor.fetchone()[0]

        # Threat BTS: jumlah data GSM dan LTE dengan status FALSE
        cursor.execute("SELECT COUNT(*) FROM gsm_data WHERE status = FALSE")
        threat_gsm = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM lte_data WHERE status = FALSE")
        threat_lte = cursor.fetchone()[0]
        threat_bts = threat_gsm + threat_lte

        # Total BTS: total data dari gsm_data dan lte_data
        cursor.execute("SELECT COUNT(*) FROM gsm_data")
        total_gsm = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM lte_data")
        total_lte = cursor.fetchone()[0]
        total_bts = total_gsm + total_lte

        # Count per generation:
        # Asumsikan semua data GSM merupakan 2G.
        count_2g = total_gsm
        count_4g = total_lte

        return {
            "inactive": inactive_count,
            "available": available_count,
            "running": running_count,
            "threat_bts": threat_bts,
            "total_bts": total_bts,
            "total_4G": count_4g,
            "total_2G": count_2g
        }
    except Exception as e:
        print(f"Error retrieving status counts: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def device_information_detail(id : int):
    conn= get_db_connection()
    try:
        cursor =  conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM devices WHERE id = %s", (id,))
        device = cursor.fetchone()
        return device
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

def devicegroup():
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT 
                dg.id AS group_id,
                dg.group_name,
                dg.description,
                dg.created_at AS group_created_at,
                dg.user_id,
                u.email AS owner_email,
                u.username AS owner_username,
                u.role AS owner_role,
                d.id AS device_id,
                d.serial_number,
                d.ip,
                d.is_connected,
                d.is_running,
                d.created_at AS device_created_at
            FROM device_group dg
            LEFT JOIN users u ON dg.user_id = u.id
            LEFT JOIN devices d ON dg.id = d.group_id
            ORDER BY dg.id;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        groups = {}
        for row in rows:
            group_id = row["group_id"]
            if group_id not in groups:
                groups[group_id] = {
                    "group_id": group_id,
                    "group_name": row["group_name"],
                    "description": row["description"],
                    "created_at": row["group_created_at"],
                    "group_status": "assigned" if row["user_id"] is not None else "unassigned",
                    "owner": {
                        "user_id": row["user_id"],
                        "email": row["owner_email"],
                        "username": row["owner_username"],
                        "role": row["owner_role"]
                    } if row["user_id"] is not None else None,
                    "devices": []
                }
            # Jika ada data device (LEFT JOIN dapat menghasilkan nilai NULL untuk device)
            if row.get("device_id") is not None:
                device = {
                    "device_id": row["device_id"],
                    "serial_number": row["serial_number"],
                    "ip": row["ip"],
                    "is_connected": row["is_connected"],
                    "is_running": row["is_running"],
                    "created_at": row["device_created_at"]
                }
                groups[group_id]["devices"].append(device)
                
        return {"status": "success", "device_groups": list(groups.values())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()


def get_all_campaigns(page: int = 1, limit: int = 10):
    try:
        connection = get_db_connection()
        if connection is None:
            return None

        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Ambil total jumlah campaign
        cursor.execute("SELECT COUNT(*) AS total FROM campaign")
        total_campaigns = cursor.fetchone()["total"]
        
        offset = (page - 1) * limit
        
        # Ambil data campaign dengan pagination
        cursor.execute("""
            SELECT id, name, status, time_start, time_stop, user_id 
            FROM campaign 
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        campaigns = cursor.fetchall()
        
        results = []
        for camp in campaigns:
            campaign_id = camp["id"]
            # Ambil data user (pembuat campaign)
            cursor.execute("""
                SELECT id, username, email, role 
                FROM users 
                WHERE id = %s
            """, (camp["user_id"],))
            user = cursor.fetchone()
            
            # Ambil daftar device yang terlibat (JOIN campaign_devices dan devices)
            cursor.execute("""
                SELECT d.id, d.serial_number, d.ip, d.is_connected, d.created_at 
                FROM campaign_devices cd
                JOIN devices d ON cd.device_id = d.id
                WHERE cd.campaign_id = %s
            """, (campaign_id,))
            devices = cursor.fetchall()
            device_count = len(devices)
            
            # Hitung jumlah BTS dari GSM dan LTE
            cursor.execute("SELECT COUNT(*) AS count FROM gsm_data WHERE campaign_id = %s", (campaign_id,))
            gsm_count = cursor.fetchone()["count"]
            cursor.execute("SELECT COUNT(*) AS count FROM lte_data WHERE campaign_id = %s", (campaign_id,))
            lte_count = cursor.fetchone()["count"]
            total_bts = gsm_count + lte_count
            # Diasumsikan jumlah threads sama dengan jumlah device
            thread_count = device_count

            summary = {
                "id_campaign": campaign_id,
                "campaign_name": camp.get("name", ""),
                "status": camp["status"],
                "time_start": camp["time_start"],
                "time_stop": camp["time_stop"],
                "user": user,
                "jumlah_device": device_count,
                "devices": devices,
                "jumlah_bts": total_bts,
                "jumlah_threads": thread_count
            }
            results.append(summary)
        
        return {
            "status": "success",
            "page": page,
            "limit": limit,
            "total_campaigns": total_campaigns,
            "campaigns": results
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def remove_device(device_id):
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # Cek apakah device ada
        cursor.execute("SELECT group_id FROM devices WHERE id = %s", (device_id,))
        device_row = cursor.fetchone()
        if not device_row:
            raise HTTPException(status_code=404, detail="Device not found")

        # Jika device sudah tidak ada group, kembalikan pesan
        if device_row["group_id"] is None:
            raise HTTPException(status_code=400, detail="Device does not belong to any group")

        # Update group_id = NULL
        update_query = "UPDATE devices SET group_id = NULL WHERE id = %s"
        cursor.execute(update_query, (device_id,))
        conn.commit()

        return {
            "status": "success",
            "message": f"Device {device_id} removed from group successfully."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()


def addDeviceToGroup(device_id, group_id):
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # Cek apakah device ada dan belum punya group
        cursor.execute("SELECT group_id FROM devices WHERE id = %s", (device_id,))
        device_row = cursor.fetchone()
        if not device_row:
            raise HTTPException(status_code=404, detail="Device not found")

        # Jika device sudah punya group, tolak
        if device_row["group_id"] is not None:
            raise HTTPException(status_code=400, detail="Device already belongs to a group")

        # Cek apakah group ada
        cursor.execute("SELECT id FROM device_group WHERE id = %s", (group_id,))
        group_row = cursor.fetchone()
        if not group_row:
            raise HTTPException(status_code=404, detail="Group not found")

        # Update devices: set group_id ke group_id yang diinginkan
        update_query = "UPDATE devices SET group_id = %s WHERE id = %s"
        cursor.execute(update_query, (group_id, device_id))
        conn.commit()

        return {
            "status": "success",
            "message": f"Device {device_id} added to group {group_id} successfully."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

def deleteuser(user_id):
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection error")

    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # 1. Cek apakah user dengan user_id tersebut ada
        cursor.execute("SELECT id, email, username FROM users WHERE id = %s", (user_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        # 2. Cek apakah ada group yang dimiliki user ini, set user_id ke NULL
        cursor.execute("SELECT id FROM device_group WHERE user_id = %s", (user_id,))
        groups_owned = cursor.fetchall()
        if groups_owned:
            # Set user_id = NULL untuk semua group yang dimiliki user ini
            cursor.execute("UPDATE device_group SET user_id = NULL WHERE user_id = %s", (user_id,))

        # 3. Hapus user dari tabel users
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))

        conn.commit()

        return {
            "status": "success",
            "message": f"User with id={user_id} has been deleted successfully.",
            "deleted_user": {
                "id": user_id,
                "email": user_row["email"],
                "username": user_row["username"]
            }
        }
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def editUser(user_id, username , password, group_id):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        # Cek apakah user dengan user_id tersebut ada
        cursor.execute("SELECT id FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Jika email diberikan, cek apakah sudah digunakan oleh user lain
        if username:
            cursor.execute("SELECT id FROM users WHERE username = %s AND id <> %s", (username, user_id))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="username already in use by another user.")

        # Jika group_id diberikan, cek apakah group ada dan valid
        if group_id is not None:
            cursor.execute("SELECT user_id FROM device_group WHERE id = %s", (group_id,))
            group_info = cursor.fetchone()
            if not group_info:
                raise HTTPException(status_code=404, detail="Selected group not found.")
            # Perbolehkan update jika group belum memiliki owner atau jika owner tersebut adalah user yang sedang diedit
            if group_info["user_id"] is not None and group_info["user_id"] != user_id:
                raise HTTPException(status_code=400, detail="Selected group already has an owner.")
        
        # Siapkan update query dinamis berdasarkan field yang diupdate
        update_fields = []
        update_values = []
        if username:
            update_fields.append("username = %s")
            update_values.append(username)
        if password:
            # Gantikan fungsi hashing dengan enkripsi reversible
            encrypted_pw = encrypt_password(password)
            update_fields.append("password = %s")
            update_values.append(encrypted_pw)
        
        if update_fields:
            # Tambahkan kondisi WHERE
            update_values.append(user_id)
            update_query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"
            cursor.execute(update_query, tuple(update_values))
            conn.commit()

        # Jika group_id diberikan, update device_group hanya jika nilai group_id berubah
        if group_id is not None:
            cursor.execute("UPDATE device_group SET user_id = %s WHERE id = %s", (user_id, group_id))
            conn.commit()

        # Kembalikan data user terbaru
        cursor.execute("SELECT id, username, created_at FROM users WHERE id = %s", (user_id,))
        updated_user = cursor.fetchone()
        return {
            "status": "success",
            "message": "User updated successfully",
            "user": updated_user
        }
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e.pgerror}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()



def listUser():
    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT 
                u.id,
                u.email,
                u.username,
                u.name,
                u.role,
                u.created_at,
                dg.id AS group_id,
                dg.group_name,
                dg.description AS group_description,
                dg.created_at AS group_created_at
            FROM users u
            LEFT JOIN device_group dg ON u.id = dg.user_id
            ORDER BY u.id;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        users = []
        for row in rows:
            user = {
                "id": row["id"],
                "email": row["email"],
                "username": row["username"],
                "name": row["name"],
                "role": row["role"],
                "created_at": row["created_at"],
            }
            # Jika ada data group, masukkan ke dalam objek group
            if row["group_id"] is not None:
                user["group"] = {
                    "group_id": row["group_id"],
                    "group_name": row["group_name"],
                    "group_description": row["group_description"],
                    "group_created_at": row["group_created_at"],
                }
            else:
                user["group"] = None
            users.append(user)

        return {"status": "success", "users": users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


def getPassword(user_id: str) -> str:
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute("SELECT password FROM users WHERE id = %s", (user_id,))
        password_record = cursor.fetchone()

        if not password_record:
            raise HTTPException(status_code=404, detail="User tidak ditemukan")

        encrypted_password = password_record["password"]

        decrypted_password = fernet.decrypt(encrypted_password.encode()).decode()

        return decrypted_password

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()