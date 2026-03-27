from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras

app = FastAPI(title="Crypto Market Analytics API")

# Habilitar CORS para que nuestro Frontend web pueda conectarse
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # En producción, aquí iría el dominio real
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Usando la Connection String directamente
DB_URL = "postgresql://postgres:[sRtBUzTstyxFyfwc]@db.xxxx.supabase.co:5432/postgres"

def get_db_connection():
    try:
        return psycopg2.connect(DB_URL)
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        return None

# --- NUEVO: Sistema de Login ---
@app.post("/api/v1/login")
def login(datos: dict):
    usuario = datos.get("usuario")
    password = datos.get("password")
    
    # Credenciales de prueba (Simulando Azure Active Directory / Entra ID)
    if usuario == "cliente" and password == "secreto123":
        # Simulamos la entrega de un token JWT
        return {"access_token": "admin123", "token_type": "bearer"}
    else:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")

# --- ACTUALIZADO: Endpoint protegido ---
@app.get("/api/v1/precios/{moneda}")
def obtener_precio_actual(moneda: str, authorization: str = Header(None)):
    # Verificamos que el cliente envíe su token de seguridad (RBAC/JWT)
    
    if moneda.lower() not in ["bitcoin", "ethereum"]:
        raise HTTPException(status_code=400, detail="Moneda no soportada.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de base de datos")

    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
            SELECT price_usd, var_porcentual, media_movil 
            FROM crypto_history 
            WHERE coin = %s ORDER BY timestamp DESC LIMIT 1
        """
        cur.execute(query, (moneda.lower(),))
        resultado = cur.fetchone()
        cur.close()
        conn.close()

        if not resultado:
            raise HTTPException(status_code=404, detail="No hay datos.")
            
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
