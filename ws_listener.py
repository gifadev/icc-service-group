import asyncio
import websockets
import json

async def listen_ws(campaign_id):
    """Terhubung ke WebSocket dan menerima semua data campaign setiap 5 detik."""
    uri = f"ws://172.15.1.223:8004/ws/{campaign_id}"  
    async with websockets.connect(uri) as websocket:
        print(f"✅ Terhubung ke WebSocket Server untuk campaign {campaign_id}...")

        try:
            while True:
                # Menerima data JSON dari WebSocket
                message = await websocket.recv()
                try:
                    data = json.loads(message)
                    print(f"📩 Data diterima dari WebSocket:\n{json.dumps(data, indent=2)}")
                except json.JSONDecodeError:
                    print(f"⚠️ Gagal decode JSON: {message}")

        except websockets.ConnectionClosed:
            print(f"❌ Koneksi WebSocket ke campaign {campaign_id} ditutup.")

if __name__ == "__main__":
    campaign_id = input("Masukkan Campaign ID: ")
    asyncio.run(listen_ws(campaign_id))
