from dotenv import load_dotenv
load_dotenv()

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
SQL_QUERY    = os.environ.get(             # query o nombre de tabla/vista
    "SQL_QUERY",
    "SELECT * FROM [003BDCOMUN].[dbo].[CLIACTU]"       # cambia esto por tu tabla real
)

SPREADSHEET_NAME = os.environ["SPREADSHEET_NAME"]  # nombre exacto del Google Sheet
SHEET_TAB        = os.environ.get("SHEET_TAB", "Clientes")  # pestaña destino
SYNC_INTERVAL    = int(os.environ.get("SYNC_INTERVAL_MINUTES", "30"))  # frecuencia

# Las credenciales de Google vienen como JSON en una variable de entorno
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

# ─── CONEXIÓN GOOGLE SHEETS ───────────────────────────────────────────────────

def get_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client     = gspread.authorize(creds)
    return client.open(SPREADSHEET_NAME).worksheet(SHEET_TAB)

# ─── CONEXIÓN SQL SERVER ──────────────────────────────────────────────────────

def get_df_from_sql():
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
        df = pd.read_sql(SQL_QUERY, conn)
    return df

# ─── SINCRONIZACIÓN ───────────────────────────────────────────────────────────

ultimo_sync = {"timestamp": None, "filas": None, "error": None}

def sincronizar():
    log.info("Iniciando sincronización...")
    try:
        # 1. Extraer de SQL Server
        df = get_df_from_sql()
        log.info(f"SQL Server: {len(df)} filas extraídas")

        # Convertir columnas datetime a string para que Google Sheets las acepte
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            df[col] = df[col].astype(str)

        # Reemplazar NaN por cadena vacía
        df = df.fillna("")

        # 2. Escribir en Google Sheets (reemplaza todo)
        sheet = get_sheet()
        sheet.clear()
        sheet.update([df.columns.tolist()] + df.values.tolist())
        log.info(f"Google Sheets actualizado con {len(df)} filas")

        ultimo_sync["timestamp"] = datetime.now().isoformat()
        ultimo_sync["filas"]     = len(df)
        ultimo_sync["error"]     = None

    except pyodbc.Error as e:
        msg = f"Error SQL Server: {e}"
        log.error(msg)
        ultimo_sync["error"] = msg
        raise

    except gspread.exceptions.GSpreadException as e:
        msg = f"Error Google Sheets: {e}"
        log.error(msg)
        ultimo_sync["error"] = msg
        raise

    except Exception as e:
        msg = f"Error inesperado: {e}"
        log.error(msg)
        ultimo_sync["error"] = msg
        raise

# ─── APP ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="API Sync SQL Server → Google Sheets")

@app.on_event("startup")
def startup():
    # Sincronizar al arrancar
    try:
        sincronizar()
    except Exception:
        log.warning("Sincronización inicial fallida, reintentará en el próximo ciclo")

    # Programar sincronización automática
    scheduler = BackgroundScheduler()
    scheduler.add_job(sincronizar, "interval", minutes=SYNC_INTERVAL)
    scheduler.start()
    log.info(f"Scheduler iniciado: sincronización cada {SYNC_INTERVAL} minutos")

@app.get("/")
def status():
    """Estado general de la API"""
    return {
        "status": "ok",
        "sync_interval_minutes": SYNC_INTERVAL,
        "ultimo_sync": ultimo_sync,
    }

@app.post("/sync")
def sync_manual():
    """Forzar sincronización manual"""
    try:
        sincronizar()
        return {
            "status": "ok",
            "mensaje": "Sincronización completada",
            "filas": ultimo_sync["filas"],
            "timestamp": ultimo_sync["timestamp"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
