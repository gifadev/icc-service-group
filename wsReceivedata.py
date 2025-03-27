import asyncio
import websockets
import json
import psycopg2
import psycopg2.extras
from database_config import get_db_connection

# Fungsi untuk upsert device berdasarkan serial_number
def upsert_device(cur, device):
    query = """
    INSERT INTO devices (serial_number, ip, is_connected, created_at)
    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (serial_number) DO UPDATE SET
      ip = EXCLUDED.ip,
      is_connected = EXCLUDED.is_connected;
    """
    cur.execute(query, (
        device["serial_number"],
        device["ip"],
        bool(device["is_connected"])
    ))


# Fungsi untuk memasukkan data GSM
def insert_gsm_data(cur, campaign_id, device_id, gsm_list):
    query = """
        INSERT INTO gsm_data (
            campaign_id, device_id, mcc, mnc, operator, local_area_code, 
            arfcn, cell_identity, rxlev, rxlev_access_min, status, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (campaign_id, device_id, mcc, mnc, local_area_code, cell_identity)
        DO UPDATE SET
            operator = EXCLUDED.operator,
            arfcn = EXCLUDED.arfcn,
            rxlev = EXCLUDED.rxlev,
            rxlev_access_min = EXCLUDED.rxlev_access_min,
            status = EXCLUDED.status,
            created_at = CURRENT_TIMESTAMP;

        """
    for gsm in gsm_list:
        # Jika status tidak ada, default True (sesuaikan dengan logika)
        status_value = True if gsm.get("status") is None else bool(gsm["status"])
        cur.execute(query, (
            campaign_id,
            device_id,
            gsm["mcc"],
            gsm["mnc"],
            gsm["operator"],
            int(gsm["local_area_code"]) if gsm.get("local_area_code") is not None else None,
            int(gsm["arfcn"]) if gsm.get("arfcn") is not None else None,
            int(gsm["cell_identity"]) if gsm.get("cell_identity") is not None else None,
            int(gsm["rxlev"]) if gsm.get("rxlev") is not None else None,
            float(gsm["rxlev_access_min"]) if gsm.get("rxlev_access_min") is not None else None,
            status_value
        ))

# Fungsi untuk memasukkan data LTE
def insert_lte_data(cur, campaign_id, device_id, lte_list):
    query = """
    INSERT INTO lte_data (
        campaign_id, device_id, mcc, mnc, operator, arfcn, cell_identity, 
        tracking_area_code, frequency_band_indicator, signal_level, snr, rx_lev_min, status, created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (campaign_id, device_id, mcc, mnc, tracking_area_code, cell_identity)
    DO UPDATE SET
        operator = EXCLUDED.operator,
        arfcn = EXCLUDED.arfcn,
        frequency_band_indicator = EXCLUDED.frequency_band_indicator,
        signal_level = EXCLUDED.signal_level,
        snr = EXCLUDED.snr,
        rx_lev_min = EXCLUDED.rx_lev_min,
        status = EXCLUDED.status,
        created_at = CURRENT_TIMESTAMP;
    """
    for lte in lte_list:
        status_value = True if lte.get("status") is None else bool(lte["status"])
        cur.execute(query, (
            campaign_id,
            device_id,
            lte["mcc"],
            lte["mnc"],
            lte["operator"],
            lte["arfcn"],
            lte["cell_identity"],
            lte["tracking_area_code"],
            lte["frequency_band_indicator"],
            lte["signal_level"],
            lte["snr"],
            int(lte["rx_lev_min"]) if lte.get("rx_lev_min") is not None else None,
            status_value
        ))

def process_message(message):
    try:
        data = json.loads(message)
    except Exception as e:
        print("Gagal memparsing JSON:", e)
        return

    campaign_data = data.get("campaign")
    device_data = data.get("device")
    gsm_list = data.get("gsm_data", [])
    lte_list = data.get("lte_data", [])
    
    if campaign_data is None:
        print("Error processing message: campaign data is missing")
        return
    if device_data is None:
        print("Error processing message: device data is missing")
        return

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                cur.execute("SET search_path TO public;")
                # Upsert device berdasarkan serial_number
                upsert_device(cur, device_data)
                
                # Ambil device id dari database berdasarkan serial_number
                serial_number = device_data.get("serial_number")
                cur.execute("SELECT id FROM devices WHERE serial_number = %s", (serial_number,))
                db_device = cur.fetchone()
                if db_device is None:
                    print("Error: device tidak ditemukan di database setelah upsert")
                    return
                device_db_id = db_device["id"]

                
                # Insert data GSM dan LTE menggunakan campaign_id dan device_db_id
                insert_gsm_data(cur, campaign_data["id"], device_db_id, gsm_list)
                insert_lte_data(cur, campaign_data["id"], device_db_id, lte_list)
    except Exception as e:
        print("Error processing messagenya:", e)
    finally:
        conn.close()

# Fungsi asynchronous untuk mendengarkan WebSocket dari satu device
async def listen_ws(uri: str):
    print(f"Membuka koneksi ke {uri}")
    try:
        async with websockets.connect(uri) as websocket:
            while True:
                message = await websocket.recv()
                # print(f"Pesan diterima dari {uri}: {message}")
                print(f"Pesan diterima dari {uri}")
                process_message(message)
    except Exception as e:
        print(f"Error pada koneksi {uri}: {e}")

# Fungsi utama untuk mengambil IP device dari DB dan membuat task WebSocket untuk masing-masing
async def main():
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT ip FROM devices")
        devices = cursor.fetchall()
    except Exception as e:
        print("Error retrieving devices:", e)
        return
    finally:
        cursor.close()
        conn.close()

    tasks = []
    for device in devices:
        ip = device.get("ip")
        if not ip:
            continue
        ws_uri = f"ws://{ip}:8003/ws"
        tasks.append(asyncio.create_task(listen_ws(ws_uri)))
    
    if tasks:
        await asyncio.gather(*tasks)
    else:
        print("Tidak ada device yang ditemukan.")

if __name__ == "__main__":
    asyncio.run(main())


