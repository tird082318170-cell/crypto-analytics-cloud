import requests
import redis
import psycopg2
import json
import time
import pandas as pd
from datetime import datetime

# 1. Conexiones (Manteniendo el puerto 5433 que solucionó el problema)
r = redis.Redis(host='192.168.0.88', port=6379, db=0)
conn = psycopg2.connect("postgresql://postgres:[Cesarmvaldez2001!]@db.xxxx.supabase.co:5432/postgres")
conn.autocommit = True  # Facilita la creación y alteración de tablas
cur = conn.cursor()

# 2. Preparar la base de datos (Evolución del esquema)
cur.execute('''
    CREATE TABLE IF NOT EXISTS crypto_history (
        id SERIAL PRIMARY KEY,
        coin VARCHAR(50),
        price_usd FLOAT,
        timestamp TIMESTAMP
    )
''')
# Añadir nuevas columnas si no existen (Manejo de errores simple)
try:
    cur.execute('ALTER TABLE crypto_history ADD COLUMN var_porcentual FLOAT;')
    cur.execute('ALTER TABLE crypto_history ADD COLUMN media_movil FLOAT;')
except psycopg2.errors.DuplicateColumn:
    pass # Las columnas ya existen

def procesar_y_guardar():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iniciando ciclo ETL...")
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    
    try:
        # EXTRACCIÓN (Extract)
        response = requests.get(url)
        data = response.json()
        
        # Guardar crudo en Redis (Caché rápido para el Frontend)
        r.set('crypto_live', json.dumps(data))
        
        for coin, values in data.items():
            current_price = values['usd']
            
            # Recuperar últimos 4 precios para el cálculo de Media Móvil usando Pandas
            cur.execute(f"SELECT price_usd FROM crypto_history WHERE coin = '{coin}' ORDER BY timestamp DESC LIMIT 4")
            historial = cur.fetchall()
            
            # TRANSFORMACIÓN (Transform) - Lógica de Negocio
            precios_lista = [current_price] + [fila[0] for fila in historial]
            df = pd.DataFrame(precios_lista, columns=['precio'])
            
            # Cálculo 1: Variación Porcentual respecto a la lectura anterior
            var_pct = 0.0
            if len(df) > 1:
                precio_anterior = df.iloc[1]['precio']
                var_pct = ((current_price - precio_anterior) / precio_anterior) * 100
                
            # Cálculo 2: Media Móvil (Promedio de las últimas lecturas)
            media_movil = df['precio'].mean()
            
            # Detección de anomalías (Backend alert system)
            if abs(var_pct) > 0.5: # Si el precio cambia más de 0.5% en 30 segundos
                print(f"⚠️ ALERTA DE VOLATILIDAD: {coin.upper()} varió un {var_pct:.3f}%")
            
           # CARGA (Load) - Asegurando que los datos sean nativos de Python
            cur.execute(
                """INSERT INTO crypto_history (coin, price_usd, timestamp, var_porcentual, media_movil) 
                   VALUES (%s, %s, %s, %s, %s)""",
                (coin, current_price, datetime.now(), float(round(var_pct, 4)), float(round(media_movil, 2)))
            )
            
            print(f"✔️ {coin.upper()}: Precio=${current_price} | Var={var_pct:.4f}% | MediaMovil=${media_movil:.2f}")
            
    except Exception as e:
        print("❌ Error en el pipeline:", e)

# Ejecución continua
if __name__ == "__main__":
    print("🚀 Motor ETL Analítico Iniciado (Presiona Ctrl+C para detener)")
    while True:
        procesar_y_guardar()
        time.sleep(30)