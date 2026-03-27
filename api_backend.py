from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
import asyncio
import requests
import pandas as pd
from datetime import datetime

app = FastAPI(title="Crypto Market Analytics API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("postgresql://postgres:3FNvGAK1xToGd4tD@db.bsikaqyhjcraxjmkztpi.supabase.co:5432/postgres")
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

# --- MOTOR ETL INTEGRADO (Para que corra gratis) ---
async def motor_ingesta_continua():
    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ETL ejecutándose...")
            url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
            response = requests.get(url)
            data = response.json()
            
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                for coin in ['bitcoin', 'ethereum']:
                    current_price = float(data[coin]['usd'])
                    
                    # Insertar datos (puedes simplificar los cálculos aquí)
                    cur.execute(
                        "INSERT INTO crypto_history (coin, price_usd, timestamp, var_porcentual, media_movil) VALUES (%s, %s, %s, %s, %s)",
                        (coin, current_price, datetime.now(), 0.0, current_price)
                    )
                conn.commit()
                cur.close()
                conn.close()
                print("✔️ Datos insertados en Supabase.")
        except Exception as e:
            print(f"❌ Error en el motor interno: {e}")
        
        await asyncio.sleep(60) # Espera 1 minuto entre actualizaciones

@app.on_event("startup")
async def iniciar_etl():
    # Esto arranca el motor en segundo plano apenas inicia la API
    asyncio.create_task(motor_ingesta_continua())

# --- ENDPOINTS ---
@app.post("/api/v1/login")
def login(datos: dict):
    if datos.get("usuario") == "cliente" and datos.get("password") == "secreto123":
        return {"access_token": "admin123", "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Error")

@app.get("/api/v1/precios/{moneda}")
def obtener_precio(moneda: str):
    conn = get_db_connection()
    if not conn: raise HTTPException(status_code=500)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT price_usd, var_porcentual, media_movil FROM crypto_history WHERE coin = %s ORDER BY timestamp DESC LIMIT 1", (moneda.lower(),))
    res = cur.fetchone()
    conn.close()
    return res
