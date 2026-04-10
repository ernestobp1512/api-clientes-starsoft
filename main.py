#from dotenv import load_dotenv
#load_dotenv()

import os
import json
import logging
import pyodbc
import gspread
import pandas as pd
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from google.oauth2.service_account import Credentials
from datetime import datetime

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
# Todos los valores sensibles vienen de variables de entorno (configuradas en Dokploy)

SQL_SERVER   = os.environ["SQL_SERVER"]    # IP o hostname del SQL Server
SQL_DATABASE = os.environ["SQL_DATABASE"]  # nombre de la base de datos
SQL_USER     = os.environ["SQL_USER"]      # usuario SQL
SQL_PASSWORD = os.environ["SQL_PASSWORD"]  # contraseña SQL
SQL_QUERY    = os.environ.get(             # query o nombre de tabla/vista de clientes
    "SQL_QUERY",
    "SELECT * FROM [003BDCOMUN].[dbo].[CLIACTU]"       # cambia esto por tu tabla real
)
SQL_QUERY_DIRECCIONES = os.environ.get(    # query o nombre de tabla para direcciones
    "SQL_QUERY_DIRECCIONES",
    "SELECT * FROM [003BDCOMUN].[dbo].[DIRACTU]"   # cambia esto por tu tabla de direcciones
)

SPREADSHEET_NAME = os.environ["SPREADSHEET_NAME"]  # nombre exacto del Google Sheet
SHEET_TAB_CLIENTES = os.environ.get("SHEET_TAB", "Clientes")  # pestaña destino clientes
SHEET_TAB_DIRECCIONES = os.environ.get("SHEET_TAB_DIRECCIONES", "Direcciones")  # pestaña destino direcciones
SYNC_INTERVAL    = int(os.environ.get("SYNC_INTERVAL_MINUTES", "30"))  # frecuencia

# Las credenciales de Google vienen como JSON en una variable de entorno
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

# ─── CONEXIÓN GOOGLE SHEETS ───────────────────────────────────────────────────

def get_sheet(sheet_tab):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client     = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME).worksheet(sheet_tab)

# ─── CONEXIÓN SQL SERVER ──────────────────────────────────────────────────────

def get_df_from_sql(query):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER=tcp:{SQL_SERVER},1433;"  # Forzar TCP/IP y puerto 1433
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=no;"                     # Evitar problemas de TLS con SQL 2012
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn)
    return df

# ─── SINCRONIZACIÓN ───────────────────────────────────────────────────────────

ultimo_sync = {
    "timestamp": None,
    "filas_clientes": None,
    "filas_direcciones": None,
    "error": None
}

def sincronizar_tabla(query, tab_name):
    """Extrae datos de SQL según el query y los escribe en la pestaña tab_name."""
    try:
        # 1. Extraer de SQL Server
        df = get_df_from_sql(query)
        log.info(f"SQL Server ({tab_name}): {len(df)} filas extraídas")

        # Convertir columnas datetime a string para que Google Sheets las acepte
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            df[col] = df[col].astype(str)

        # Reemplazar NaN por cadena vacía
        df = df.fillna("")

        # 2. Escribir en Google Sheets (reemplaza todo)
        sheet = get_sheet(tab_name)
        sheet.clear()
        sheet.update([df.columns.tolist()] + df.values.tolist())
        log.info(f"Google Sheets ({tab_name}) actualizado con {len(df)} filas")
        return len(df)

    except pyodbc.Error as e:
        msg = f"Error SQL Server en {tab_name}: {e}"
        log.error(msg)
        raise
    except gspread.exceptions.GSpreadException as e:
        msg = f"Error Google Sheets en {tab_name}: {e}"
        log.error(msg)
        raise
    except Exception as e:
        msg = f"Error inesperado en {tab_name}: {e}"
        log.error(msg)
        raise

def sincronizar_ambas():
    log.info("Iniciando sincronización de clientes y direcciones...")
    try:
        filas_cli = sincronizar_tabla(SQL_QUERY, SHEET_TAB_CLIENTES)
        filas_dir = sincronizar_tabla(SQL_QUERY_DIRECCIONES, SHEET_TAB_DIRECCIONES)

        ultimo_sync["timestamp"] = datetime.now().isoformat()
        ultimo_sync["filas_clientes"] = filas_cli
        ultimo_sync["filas_direcciones"] = filas_dir
        ultimo_sync["error"] = None

    except Exception as e:
        msg = f"Error general en sincronización simultánea: {e}"
        log.error(msg)
        ultimo_sync["error"] = msg
        raise

# ─── APP ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="API Sync SQL Server → Google Sheets")

@app.on_event("startup")
def startup():
    # Sincronizar ambas tablas al arrancar
    try:
        sincronizar_ambas()
    except Exception:
        log.warning("Sincronización inicial fallida, reintentará en el próximo ciclo")

    # Programar sincronización automática de ambas
    scheduler = BackgroundScheduler()
    scheduler.add_job(sincronizar_ambas, "interval", minutes=SYNC_INTERVAL)
    scheduler.start()
    log.info(f"Scheduler iniciado: sincronización doble cada {SYNC_INTERVAL} minutos")

@app.get("/")
def status():
    """Estado general de la API"""
    return {
        "status": "ok",
        "sync_interval_minutes": SYNC_INTERVAL,
        "ultimo_sync": ultimo_sync,
    }

@app.post("/sync")
def sync_manual_ambas():
    """Forzar sincronización manual de ambas tablas (clientes y direcciones)"""
    try:
        sincronizar_ambas()
        return {
            "status": "ok",
            "mensaje": "Sincronización doble completada",
            "filas_clientes": ultimo_sync["filas_clientes"],
            "filas_direcciones": ultimo_sync["filas_direcciones"],
            "timestamp": ultimo_sync["timestamp"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/clientes")
def sync_manual_clientes():
    """Forzar sincronización manual solo de clientes"""
    try:
        filas = sincronizar_tabla(SQL_QUERY, SHEET_TAB_CLIENTES)
        ultimo_sync["timestamp"] = datetime.now().isoformat()
        ultimo_sync["filas_clientes"] = filas
        return {
            "status": "ok",
            "mensaje": "Sincronización de clientes completada",
            "filas_clientes": filas,
            "timestamp": ultimo_sync["timestamp"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/direcciones")
def sync_manual_direcciones():
    """Forzar sincronización manual solo de direcciones"""
    try:
        filas = sincronizar_tabla(SQL_QUERY_DIRECCIONES, SHEET_TAB_DIRECCIONES)
        ultimo_sync["timestamp"] = datetime.now().isoformat()
        ultimo_sync["filas_direcciones"] = filas
        return {
            "status": "ok",
            "mensaje": "Sincronización de direcciones completada",
            "filas_direcciones": filas,
            "timestamp": ultimo_sync["timestamp"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
