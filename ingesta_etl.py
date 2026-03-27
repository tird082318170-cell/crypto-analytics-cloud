import requests
import psycopg2
import time
import pandas as pd
from datetime import datetime

# 1. Conexión a Supabase (REEMPLAZA TU_PASSWORD)
# Asegúrate de quitar los corchetes [] al poner tu contraseña
URL_DB = "postgresql://postgres.bsikaqyhjcraxjmkztpi:sRtBUzTstyxFyfwc@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

def conectar_db():
    try:
        conn = psycopg2.connect(URL_DB)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Supabase: {e}")
        return None

def preparar_tablas():
    conn = conectar_db()
    if not conn: return
    cur = conn.cursor()
    # Crear tabla si no existe
    cur.execute('''
        CREATE TABLE IF NOT EXISTS crypto_history (
            id SERIAL PRIMARY KEY,
            coin VARCHAR(50),
            price_usd FLOAT,
            timestamp TIMESTAMP,
            var_porcentual FLOAT,
            media_movil FLOAT
        )
    ''')
    cur.close()
    conn.close()

def procesar_y_guardar():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iniciando ciclo ETL...")
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        conn = conectar_db()
        if not conn: return
        cur = conn.cursor()

        for coin in ['bitcoin', 'ethereum']:
            current_price = float(data[coin]['usd'])
            
            # Obtener historial para cálculos
            cur.execute("SELECT price_usd FROM crypto_history WHERE coin = %s ORDER BY timestamp DESC LIMIT 4", (coin,))
            historial = cur.fetchall()
            
            # Cálculos de Transformación
            precios_lista = [current_price] + [fila[0] for fila in historial]
            df = pd.DataFrame(precios_lista, columns=['precio'])
            
            var_pct = 0.0
            if len(df) > 1:
                precio_anterior = df.iloc[1]['precio']
                var_pct = ((current_price - precio_anterior) / precio_anterior) * 100
            
            media_movil = df['precio'].mean()

            # Carga de datos
            cur.execute(
                """INSERT INTO crypto_history (coin, price_usd, timestamp, var_porcentual, media_movil) 
                   VALUES (%s, %s, %s, %s, %s)""",
                (coin, current_price, datetime.now(), float(round(var_pct, 4)), float(round(media_movil, 2)))
            )
            print(f"✔️ {coin.upper()}: ${current_price} (Guardado)")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error en el pipeline: {e}")

if __name__ == "__main__":
    preparar_tablas()
    while True:
        procesar_y_guardar()
        time.sleep(30)
