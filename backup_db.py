import os
import time
import psycopg2
import pandas as pd
from datetime import datetime

# Configuración de la base de datos
# En lugar de usar host, port, etc., usa la URL:
conn = psycopg2.connect("postgresql://postgres.bsikaqyhjcraxjmkztpi:sRtBUzTstyxFyfwc@aws-1-us-east-1.pooler.supabase.com:6543/postgres")

# Carpeta donde se guardarán los backups (Simula el Azure Blob Storage)
BACKUP_DIR = "./backups"
if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)

def realizar_backup():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_backup = os.path.join(BACKUP_DIR, f"backup_crypto_{timestamp}.csv")
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iniciando respaldo de seguridad (ISO 22301)...")
    
    try:
        # 1. Conectar a la base de datos
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        
        # 2. Extraer toda la información de la tabla usando Pandas
        query = "SELECT * FROM crypto_history ORDER BY timestamp ASC"
        df = pd.read_sql_query(query, conn)
        
        # 3. Guardar los datos en un archivo CSV
        df.to_csv(archivo_backup, index=False)
        
        print(f"✔️ Backup exitoso. Se guardaron {len(df)} registros en: {archivo_backup}")
        
    except Exception as e:
        print(f"❌ Error al realizar el backup: {e}")
        
    finally:
        # Cerrar la conexión
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    print("🛡️ Servicio de Backups Continuos Iniciado (Vía Pandas)")
    while True:
        realizar_backup()
        # El documento exige backups cada 15 minutos (15 * 60 = 900 segundos)
        # Para probar que funciona rápido ahora mismo, cámbialo a 30 segundos temporalmente si quieres ver el archivo
        espera_segundos = 900 
        print(f"Esperando {espera_segundos/60} minutos para el siguiente respaldo...")
        time.sleep(espera_segundos)
