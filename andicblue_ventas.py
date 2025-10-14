# andicblue_app_local.py
# AndicBlue - Versión local con respaldo CSV y sincronización controlada con Google Sheets
# Requisitos:
#  - Python >= 3.9
#  - pip install streamlit pandas gspread google-auth plotly
#  - st.secrets["gcp_service_account"] (opcional — si quieres usar Google Sheets)
#
# Instrucciones:
# 1. Crea carpeta 'data/' en el mismo directorio que este script.
# 2. Ejecuta: streamlit run andicblue_app_local.py
#
# Autor: ChatGPT adaptado a requerimientos de usuario
# Fecha: 2025-10

import streamlit as st
import pandas as pd
import os
import json
import time
import math
import logging
from typing import Any, Dict, List, Tuple
from datetime import datetime, date
from pathlib import Path

# Optional Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GS_AVAILABLE = True
except Exception:
    GS_AVAILABLE = False

# Optional plotting
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except Exception:
    PLOTLY_AVAILABLE = False

# ---------------------------
# CONFIG / CONSTANTS
# ---------------------------
APP_TITLE = "AndicBlue — Gestión de Pedidos"
APP_ICON = "🫐"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# CSV filenames
CSV_CLIENTES = DATA_DIR / "clientes.csv"
CSV_PEDIDOS = DATA_DIR / "pedidos.csv"           # header (cabecera) for orders
CSV_PEDIDOS_DETALLE = DATA_DIR / "pedidos_detalle.csv"
CSV_INVENTARIO = DATA_DIR / "inventario.csv"
CSV_FLUJO = DATA_DIR / "flujo.csv"
CSV_GASTOS = DATA_DIR / "gastos.csv"
CSV_LOG = DATA_DIR / "logs.txt"

# Google Sheet name (if using)
SHEET_NAME = "andicblue_pedidos"

DOMICILIO_COST = 3000  # COP

# Canonical products and prices
PRODUCTOS = {
    "Docena de Arándanos 125g": 52500,
    "Arandanos_125g": 5000,
    "Arandanos_250g": 10000,
    "Arandanos_500g": 20000,
    "Kilo_industrial": 30000,
    "Mermelada_azucar": 16000,
    "Mermelada_sin_azucar": 20000,
}

# HEADERS - ensure consistent ordering
HEAD_CLIENTES = ["ID Cliente", "Nombre", "Telefono", "Direccion"]
HEAD_PEDIDOS = [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
    "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Estado",
    "Medio_pago", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"
]
HEAD_PEDIDOS_DETALLE = ["ID Pedido", "Producto", "Cantidad", "Precio_unitario", "Subtotal"]
HEAD_INVENTARIO = ["Producto", "Stock"]
HEAD_FLUJO = [
    "Fecha", "ID Pedido", "Cliente", "Medio_pago",
    "Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"
]
HEAD_GASTOS = ["Fecha", "Concepto", "Monto"]

# Logging config
logging.basicConfig(
    filename=str(CSV_LOG),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ---------------------------
# UTILIDADES LOCALES
# ---------------------------

def log_info(msg: str):
    logging.info(msg)

def log_warn(msg: str):
    logging.warning(msg)

def log_error(msg: str):
    logging.error(msg)

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_csv_with_headers(path: Path, headers: List[str]):
    """Ensure CSV exists with exactly headers (if missing, create)."""
    if not path.exists():
        df = pd.DataFrame(columns=headers)
        df.to_csv(path, index=False)
        log_info(f"Creada CSV inicial: {path}")
    else:
        try:
            df = pd.read_csv(path)
            # If headers mismatched (e.g., duplicated headers row), fix — keep columns intersection
            existing = list(df.columns)
            # If first row equals headers (duplicated header as row), drop first row
            if not df.empty:
                first_row = df.iloc[0].astype(str).tolist()
                if first_row == headers:
                    df = df.drop(index=0).reset_index(drop=True)
                    df.to_csv(path, index=False)
                    log_info(f"Duplicated header row removed from {path}")
            # Ensure header columns present
            for h in headers:
                if h not in df.columns:
                    df[h] = ""
                    log_info(f"Added missing column {h} to {path}")
            # enforce column order
            df = df.reindex(columns=headers)
            df.to_csv(path, index=False)
        except Exception as e:
            log_error(f"Error asegurando CSV {path}: {e}")
            # try to recreate safely
            df = pd.DataFrame(columns=headers)
            df.to_csv(path, index=False)

# Initialize CSV files with headers if missing or corrupted
ensure_csv_with_headers(CSV_CLIENTES, HEAD_CLIENTES)
ensure_csv_with_headers(CSV_PEDIDOS, HEAD_PEDIDOS)
ensure_csv_with_headers(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
ensure_csv_with_headers(CSV_INVENTARIO, HEAD_INVENTARIO)
ensure_csv_with_headers(CSV_FLUJO, HEAD_FLUJO)
ensure_csv_with_headers(CSV_GASTOS, HEAD_GASTOS)

# ---------------------------
# GOOGLE SHEETS - optional (safe wrappers)
# ---------------------------

GS_CLIENT = None
GS_SPREADSHEET = None

def init_gs_client():
    global GS_CLIENT, GS_SPREADSHEET
    if not GS_AVAILABLE:
        log_warn("gspread/google-auth not available, Sheets functionality disabled.")
        return False
    if "gcp_service_account" not in st.secrets:
        log_warn("No st.secrets['gcp_service_account'] found. Sheets disabled until provided.")
        return False
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])
        GS_CLIENT = gspread.authorize(creds)
        # try open spreadsheet (may raise)
        try:
            GS_SPREADSHEET = GS_CLIENT.open(SHEET_NAME)
        except Exception:
            # spreadsheet missing: do not create automatically in local mode; leave None
            GS_SPREADSHEET = None
        log_info("Google Sheets client inicializado (OK).")
        return True
    except Exception as e:
        log_error(f"Error inicializando Google Sheets client: {e}")
        return False

# init once at startup
init_gs_client()

# Safe wrappers for Sheet operations with backoff and fallback to CSV
def exponential_backoff(attempt: int):
    delay = min(10, 0.5 * (2 ** attempt))
    time.sleep(delay)

def safe_get_worksheet(title: str):
    """Return a gspread worksheet or None if not available. Use retries for quota errors."""
    global GS_SPREADSHEET
    if GS_CLIENT is None:
        return None
    for attempt in range(5):
        try:
            if GS_SPREADSHEET is None:
                # try to open
                GS_SPREADSHEET = GS_CLIENT.open(SHEET_NAME)
            ws = GS_SPREADSHEET.worksheet(title)
            return ws
        except Exception as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                log_warn(f"Sheets quota exceeded when accessing {title}. Attempt {attempt+1}/5.")
                exponential_backoff(attempt)
                continue
            # if worksheet not found, try to create
            try:
                GS_SPREADSHEET.add_worksheet(title=title, rows=1000, cols=20)
                ws = GS_SPREADSHEET.worksheet(title)
                return ws
            except Exception as ex:
                log_warn(f"Error creating worksheet {title}: {ex}")
                exponential_backoff(attempt)
                continue
    log_warn(f"No pude obtener worksheet {title} de Google Sheets.")
    return None

def ensure_sheet_headers(ws, headers: List[str]):
    """Ensure the worksheet has headers in the first row and not duplicated as row values."""
    if ws is None:
        return
    try:
        first_row = ws.row_values(1)
        if first_row != headers:
            # remove first row if it's a duplicate header
            try:
                if ws.row_count >= 1 and ws.row_values(1):
                    # delete row 1 then insert headers
                    ws.delete_rows(1)
            except Exception:
                # fallback: just insert headers at top
                pass
            # try to insert headers
            try:
                ws.insert_row(headers, index=1)
            except Exception:
                try:
                    ws.append_row(headers)
                except Exception:
                    pass
    except Exception as e:
        log_warn(f"Error asegurando headers en sheet: {e}")

def safe_read_sheet_to_df(sheet_title: str, headers: List[str]) -> pd.DataFrame:
    """Attempt to read Google Sheet with retries; fall back to local CSV on failure."""
    ws = safe_get_worksheet(sheet_title)
    if ws is None:
        log_warn(f"Worksheet {sheet_title} not available, loading local CSV fallback.")
        return load_local_csv_by_sheet(sheet_title)
    for attempt in range(5):
        try:
            records = ws.get_all_records()
            df = pd.DataFrame(records)
            # ensure header alignment
            if df.empty:
                df = pd.DataFrame(columns=headers)
            else:
                # if first row accidentally equals headers, drop
                if list(df.columns) == headers:
                    pass  # ok
            return df
        except Exception as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                log_warn(f"Quota exceeded reading sheet {sheet_title}, attempt {attempt+1}")
                exponential_backoff(attempt)
                continue
            else:
                log_warn(f"Error reading sheet {sheet_title}: {e}")
                break
    log_warn(f"Failed reading sheet {sheet_title}, using local CSV fallback.")
    return load_local_csv_by_sheet(sheet_title)

def safe_write_df_to_sheet(df: pd.DataFrame, sheet_title: str, headers: List[str]) -> bool:
    """Overwrite the Google Sheet with the DataFrame (safe). Returns True if success."""
    ws = safe_get_worksheet(sheet_title)
    if ws is None:
        log_warn(f"Cannot write to sheet {sheet_title} (ws None).")
        return False
    # Normalize df columns to headers order
    try:
        df_to_write = df.copy().reindex(columns=headers)
    except Exception:
        df_to_write = df.copy()
        for h in headers:
            if h not in df_to_write.columns:
                df_to_write[h] = ""
        df_to_write = df_to_write[headers]
    # Convert NaN to empty strings
    df_to_write = df_to_write.where(pd.notnull(df_to_write), "")
    rows = [headers] + df_to_write.values.tolist()
    for attempt in range(5):
        try:
            ws.clear()
            # append rows in chunks to avoid timeouts
            for r in rows:
                ws.append_row([("" if v is None else v) for v in r])
            log_info(f"Wrote {len(df_to_write)} rows to Google Sheet {sheet_title}.")
            return True
        except Exception as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                log_warn(f"Quota exceeded writing to {sheet_title}: attempt {attempt+1}")
                exponential_backoff(attempt)
                continue
            else:
                log_warn(f"Error writing to sheet {sheet_title}: {e}")
                return False
    log_warn(f"Failed to write to sheet {sheet_title} after retries.")
    return False

# ---------------------------
# LOCAL CSV helpers (single source of truth when offline)
# ---------------------------

def load_local_csv(path: Path, headers: List[str]):
    try:
        if not path.exists():
            # create empty
            df = pd.DataFrame(columns=headers)
            df.to_csv(path, index=False)
            return df
        df = pd.read_csv(path)
        # Remove accidental duplicated header rows in data
        if not df.empty:
            first_row = df.iloc[0].astype(str).tolist()
            if first_row == headers:
                df = df.drop(index=0).reset_index(drop=True)
                df.to_csv(path, index=False)
                log_info(f"Removed duplicated header row from {path}")
        # Ensure all expected columns exist
        for h in headers:
            if h not in df.columns:
                df[h] = ""
        df = df[headers]
        return df
    except Exception as e:
        log_error(f"Error loading local CSV {path}: {e}")
        # try to return empty df
        return pd.DataFrame(columns=headers)

def save_local_csv(path: Path, df: pd.DataFrame, headers: List[str]):
    try:
        # Normalize columns
        for h in headers:
            if h not in df.columns:
                df[h] = ""
        df_to_save = df[headers]
        df_to_save.to_csv(path, index=False)
        log_info(f"Saved local CSV {path} ({len(df_to_save)} rows).")
        return True
    except Exception as e:
        log_error(f"Error saving local CSV {path}: {e}")
        return False

def load_local_csv_by_sheet(sheet_title: str) -> pd.DataFrame:
    """Return corresponding local CSV DataFrame for a sheet title."""
    if sheet_title == "Clientes":
        return load_local_csv(CSV_CLIENTES, HEAD_CLIENTES)
    elif sheet_title == "Pedidos":
        return load_local_csv(CSV_PEDIDOS, HEAD_PEDIDOS)
    elif sheet_title == "Pedidos_detalle":
        return load_local_csv(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
    elif sheet_title == "Inventario":
        return load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
    elif sheet_title == "FlujoCaja":
        return load_local_csv(CSV_FLUJO, HEAD_FLUJO)
    elif sheet_title == "Gastos":
        return load_local_csv(CSV_GASTOS, HEAD_GASTOS)
    else:
        return pd.DataFrame()

def save_local_csv_by_sheet(sheet_title: str, df: pd.DataFrame):
    if sheet_title == "Clientes":
        return save_local_csv(CSV_CLIENTES, df, HEAD_CLIENTES)
    elif sheet_title == "Pedidos":
        return save_local_csv(CSV_PEDIDOS, df, HEAD_PEDIDOS)
    elif sheet_title == "Pedidos_detalle":
        return save_local_csv(CSV_PEDIDOS_DETALLE, df, HEAD_PEDIDOS_DETALLE)
    elif sheet_title == "Inventario":
        return save_local_csv(CSV_INVENTARIO, df, HEAD_INVENTARIO)
    elif sheet_title == "FlujoCaja":
        return save_local_csv(CSV_FLUJO, df, HEAD_FLUJO)
    elif sheet_title == "Gastos":
        return save_local_csv(CSV_GASTOS, df, HEAD_GASTOS)
    else:
        log_warn(f"Unknown sheet title for saving local CSV: {sheet_title}")
        return False

# ---------------------------
# HIGH-LEVEL DATA LOAD/STORE (cache to reduce FS/Sheets calls)
# ---------------------------

@st.cache_data(ttl=30, show_spinner=False)
def load_df(sheet_title: str) -> pd.DataFrame:
    """Load from Google Sheets if available, else from local CSV. Cached to reduce repeated reads."""
    mapping = {
        "Clientes": (safe_read_sheet_to_df, HEAD_CLIENTES),
        "Pedidos": (safe_read_sheet_to_df, HEAD_PEDIDOS),
        "Pedidos_detalle": (safe_read_sheet_to_df, HEAD_PEDIDOS_DETALLE),
        "Inventario": (safe_read_sheet_to_df, HEAD_INVENTARIO),
        "FlujoCaja": (safe_read_sheet_to_df, HEAD_FLUJO),
        "Gastos": (safe_read_sheet_to_df, HEAD_GASTOS)
    }
    if sheet_title not in mapping:
        return pd.DataFrame()
    func, headers = mapping[sheet_title]
    # try sheets first
    try:
        df = func(sheet_title, headers)
        if df is None or df.empty:
            # fallback to local CSV
            df_local = load_local_csv_by_sheet(sheet_title)
            return df_local
        # ensure columns
        for h in headers:
            if h not in df.columns:
                df[h] = ""
        df = df[headers]
        return df
    except Exception as e:
        log_warn(f"Error loading {sheet_title} from sheets: {e}. Loading local CSV.")
        return load_local_csv_by_sheet(sheet_title)

def flush_cache():
    # Clear st.cache_data for load_df by incrementing a token in session_state and reloading
    st.cache_data.clear()
    log_info("Cleared st.cache_data")

# ---------------------------
# BUSINESS LOGIC: CRUD Orders, Inventory adjustments, Payments, Flow
# ---------------------------

def canonical_product_name(name: str) -> str:
    if not isinstance(name, str):
        return name
    s = name.strip()
    if s in PRODUCTOS:
        return s
    def norm(x): return x.lower().replace(" ", "").replace("_","").replace("-","")
    ns = norm(s)
    for k in PRODUCTOS.keys():
        if norm(k) == ns:
            return k
    for k in PRODUCTOS.keys():
        if ns in norm(k) or norm(k) in ns:
            return k
    return s

def next_id_for(df: pd.DataFrame, col: str) -> int:
    if df is None or df.empty or col not in df.columns:
        return 1
    try:
        vals = pd.to_numeric(df[col], errors='coerce').dropna().astype(int).tolist()
        return max(vals) + 1 if vals else 1
    except Exception:
        # fallback
        return len(df) + 1

def create_client(nombre: str, telefono: str="", direccion: str="") -> int:
    dfc = load_df("Clientes")
    cid = next_id_for(dfc, "ID Cliente")
    new_row = {"ID Cliente": cid, "Nombre": nombre, "Telefono": telefono, "Direccion": direccion}
    dfc = pd.concat([dfc, pd.DataFrame([new_row])], ignore_index=True)
    save_local_csv_by_sheet("Clientes", dfc)
    # try writing to sheets
    safe_write_df_to_sheet(dfc, "Clientes", HEAD_CLIENTES)
    flush_cache()
    log_info(f"Cliente creado: {cid} - {nombre}")
    return cid

def create_order_with_details(cliente_id: int, items: Dict[str,int], domicilio_bool: bool=False, fecha_entrega: date=None) -> int:
    """
    Create order: write header to Pedidos and lines to Pedidos_detalle.
    Adjust inventory (allow negative).
    """
    dfc = load_df("Clientes")
    if dfc.empty or cliente_id not in dfc["ID Cliente"].astype(int).tolist():
        raise ValueError("ID cliente no encontrado")
    cliente_nombre = dfc.loc[dfc["ID Cliente"].astype(int) == int(cliente_id), "Nombre"].values[0]

    df_ped = load_df("Pedidos")
    df_det = load_df("Pedidos_detalle")
    df_inv = load_df("Inventario")

    subtotal = 0
    for p,q in items.items():
        prod = canonical_product_name(p)
        price = PRODUCTOS.get(prod, 0)
        subtotal += price * int(q)

    domicilio_monto = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio_monto
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    semana_entrega = int(pd.to_datetime(fecha_entrega).isocalendar().week) if fecha_entrega else datetime.now().isocalendar().week

    pid = next_id_for(df_ped, "ID Pedido")
    header_row = {
        "ID Pedido": pid, "Fecha": fecha_actual, "ID Cliente": cliente_id, "Nombre Cliente": cliente_nombre,
        "Subtotal_productos": subtotal, "Monto_domicilio": domicilio_monto, "Total_pedido": total, "Estado": "Pendiente",
        "Medio_pago": "", "Monto_pagado": 0, "Saldo_pendiente": total, "Semana_entrega": semana_entrega
    }
    df_ped = pd.concat([df_ped, pd.DataFrame([header_row])], ignore_index=True)

    # detalle lines
    for prod_raw, qty in items.items():
        prod = canonical_product_name(prod_raw)
        price = PRODUCTOS.get(prod, 0)
        subtotal_line = int(qty) * int(price)
        line = {"ID Pedido": pid, "Producto": prod, "Cantidad": int(qty), "Precio_unitario": int(price), "Subtotal": subtotal_line}
        df_det = pd.concat([df_det, pd.DataFrame([line])], ignore_index=True)

        # update inventory
        if df_inv is None or df_inv.empty:
            df_inv = pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)
        else:
            # normalize product names
            df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
            if prod in df_inv["Producto"].values:
                idx = df_inv.index[df_inv["Producto"] == prod][0]
                df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
            else:
                df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # aggregate inventory to avoid duplicates
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})

    # persist local csv
    save_local_csv_by_sheet("Pedidos", df_ped)
    save_local_csv_by_sheet("Pedidos_detalle", df_det)
    save_local_csv_by_sheet("Inventario", df_inv)

    # try sheet writes (best-effort)
    try:
        safe_write_df_to_sheet(df_ped, "Pedidos", HEAD_PEDIDOS)
        safe_write_df_to_sheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        safe_write_df_to_sheet(df_inv, "Inventario", HEAD_INVENTARIO)
    except Exception as e:
        log_warn(f"Best-effort sync to sheets failed for new order {pid}: {e}")

    flush_cache()
    log_info(f"Created order {pid} for client {cliente_id} with items {items}")
    return pid

def get_order_details(order_id: int) -> pd.DataFrame:
    df_det = load_df("Pedidos_detalle")
    if df_det.empty:
        return pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    return df_det[df_det["ID Pedido"].astype(int) == int(order_id)].copy()

def edit_order(order_id: int, new_items: Dict[str,int], new_domic_bool: bool=None, new_week: int=None):
    """
    Replace detail lines for order_id with new_items.
    Revert old inventory and apply new adjustments.
    """
    df_ped = load_df("Pedidos")
    df_det = load_df("Pedidos_detalle")
    df_inv = load_df("Inventario")

    if df_ped.empty or order_id not in df_ped["ID Pedido"].astype(int).tolist():
        raise ValueError("Pedido no encontrado")

    # Revert inventory from old lines
    old_lines = df_det[df_det["ID Pedido"].astype(int) == int(order_id)]
    for _, r in old_lines.iterrows():
        prod = canonical_product_name(r["Producto"])
        qty = int(r["Cantidad"])
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) + qty
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, qty]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # remove old detail rows
    df_det = df_det[df_det["ID Pedido"].astype(int) != int(order_id)].reset_index(drop=True)

    # add new detail rows and subtract inventory
    for prod_raw, qty in new_items.items():
        prod = canonical_product_name(prod_raw)
        price = PRODUCTOS.get(prod, 0)
        subtotal = int(qty) * int(price)
        df_det = pd.concat([df_det, pd.DataFrame([[order_id, prod, int(qty), int(price), int(subtotal)]], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # update header totals
    subtotal_new = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in new_items.items())
    idx_h = df_ped.index[df_ped["ID Pedido"].astype(int) == int(order_id)][0]
    domicilio = float(df_ped.at[idx_h, "Monto_domicilio"]) if new_domic_bool is None else (DOMICILIO_COST if new_domic_bool else 0)
    total_new = subtotal_new + domicilio
    monto_pagado = float(df_ped.at[idx_h, "Monto_pagado"])
    saldo_new = total_new - monto_pagado

    df_ped.at[idx_h, "Subtotal_productos"] = subtotal_new
    df_ped.at[idx_h, "Monto_domicilio"] = domicilio
    df_ped.at[idx_h, "Total_pedido"] = total_new
    df_ped.at[idx_h, "Saldo_pendiente"] = saldo_new
    if new_week:
        df_ped.at[idx_h, "Semana_entrega"] = int(new_week)

    # aggregate inventory
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})

    # persist local and try sheets
    save_local_csv_by_sheet("Pedidos", df_ped)
    save_local_csv_by_sheet("Pedidos_detalle", df_det)
    save_local_csv_by_sheet("Inventario", df_inv)
    try:
        safe_write_df_to_sheet(df_ped, "Pedidos", HEAD_PEDIDOS)
        safe_write_df_to_sheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        safe_write_df_to_sheet(df_inv, "Inventario", HEAD_INVENTARIO)
    except Exception as e:
        log_warn(f"Best-effort sync failed on edit_order {order_id}: {e}")

    flush_cache()
    log_info(f"Edited order {order_id}")

def delete_order(order_id: int):
    df_ped = load_df("Pedidos")
    df_det = load_df("Pedidos_detalle")
    df_inv = load_df("Inventario")

    if df_ped.empty or order_id not in df_ped["ID Pedido"].astype(int).tolist():
        raise ValueError("Pedido no encontrado")
    # revert inventory
    detalle = df_det[df_det["ID Pedido"].astype(int) == int(order_id)]
    for _, r in detalle.iterrows():
        prod = canonical_product_name(r["Producto"])
        qty = int(r["Cantidad"])
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) + qty
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, qty]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # remove rows
    df_det = df_det[df_det["ID Pedido"].astype(int) != int(order_id)].reset_index(drop=True)
    df_ped = df_ped[df_ped["ID Pedido"].astype(int) != int(order_id)].reset_index(drop=True)
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})

    save_local_csv_by_sheet("Pedidos", df_ped)
    save_local_csv_by_sheet("Pedidos_detalle", df_det)
    save_local_csv_by_sheet("Inventario", df_inv)
    try:
        safe_write_df_to_sheet(df_ped, "Pedidos", HEAD_PEDIDOS)
        safe_write_df_to_sheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        safe_write_df_to_sheet(df_inv, "Inventario", HEAD_INVENTARIO)
    except Exception as e:
        log_warn(f"Best-effort sync failed on delete_order {order_id}: {e}")

    flush_cache()
    log_info(f"Deleted order {order_id}")

# Payments & Flow
def register_payment(order_id: int, medio_pago: str, monto: float) -> Dict[str, float]:
    df_ped = load_df("Pedidos")
    df_flu = load_df("FlujoCaja")
    if df_ped.empty or order_id not in df_ped["ID Pedido"].astype(int).tolist():
        raise ValueError("Pedido no encontrado")
    idx = df_ped.index[df_ped["ID Pedido"].astype(int) == int(order_id)][0]
    subtotal_products = float(df_ped.at[idx, "Subtotal_productos"])
    domicilio_monto = float(df_ped.at[idx, "Monto_domicilio"])
    monto_anterior = float(df_ped.at[idx, "Monto_pagado"])
    monto = float(monto)
    nuevo_total = monto_anterior + monto

    prod_total_acum = min(nuevo_total, subtotal_products)
    dom_total_acum = min(max(0, nuevo_total - subtotal_products), domicilio_monto)

    prod_pagado_antes = min(monto_anterior, subtotal_products)
    dom_pagado_antes = max(0, monto_anterior - subtotal_products)

    prod_now = max(0, prod_total_acum - prod_pagado_antes)
    domicilio_now = max(0, dom_total_acum - dom_pagado_antes)

    saldo_total = (subtotal_products - prod_total_acum) + (domicilio_monto - dom_total_acum)
    monto_total_reg = prod_total_acum + dom_total_acum

    # update header
    df_ped.at[idx, "Monto_pagado"] = monto_total_reg
    df_ped.at[idx, "Saldo_pendiente"] = saldo_total
    df_ped.at[idx, "Medio_pago"] = medio_pago
    df_ped.at[idx, "Estado"] = "Entregado" if saldo_total == 0 else "Pendiente"

    # append flujo
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_flow = {
        "Fecha": fecha, "ID Pedido": int(order_id), "Cliente": df_ped.at[idx, "Nombre Cliente"],
        "Medio_pago": medio_pago, "Ingreso_productos_recibido": prod_now, "Ingreso_domicilio_recibido": domicilio_now,
        "Saldo_pendiente_total": saldo_total
    }
    if df_flu.empty:
        df_flu = pd.DataFrame([new_flow], columns=HEAD_FLUJO)
    else:
        df_flu = pd.concat([df_flu, pd.DataFrame([new_flow])], ignore_index=True)

    # persist
    save_local_csv_by_sheet("Pedidos", df_ped)
    save_local_csv_by_sheet("FlujoCaja", df_flu)
    try:
        safe_write_df_to_sheet(df_ped, "Pedidos", HEAD_PEDIDOS)
        safe_write_df_to_sheet(df_flu, "FlujoCaja", HEAD_FLUJO)
    except Exception as e:
        log_warn(f"Best-effort sync failed on register_payment for order {order_id}: {e}")

    flush_cache()
    log_info(f"Payment registered for order {order_id}: amount={monto}, medio={medio_pago}")
    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def totals_by_payment_method() -> Dict[str, float]:
    df_f = load_df("FlujoCaja")
    if df_f.empty:
        return {}
    coerce_cols = ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"]
    for c in coerce_cols:
        if c in df_f.columns:
            df_f[c] = pd.to_numeric(df_f[c], errors='coerce').fillna(0)
    df_f["total"] = df_f["Ingreso_productos_recibido"].fillna(0) + df_f["Ingreso_domicilio_recibido"].fillna(0)
    grouped = df_f.groupby("Medio_pago")["total"].sum().to_dict()
    return {k: float(v) for k,v in grouped.items()}

def flow_summaries() -> Tuple[float, float, float, float]:
    df_f = load_df("FlujoCaja")
    df_g = load_df("Gastos")
    if not df_f.empty:
        df_f["Ingreso_productos_recibido"] = pd.to_numeric(df_f["Ingreso_productos_recibido"], errors='coerce').fillna(0)
        df_f["Ingreso_domicilio_recibido"] = pd.to_numeric(df_f["Ingreso_domicilio_recibido"], errors='coerce').fillna(0)
    total_prod = df_f["Ingreso_productos_recibido"].sum() if not df_f.empty else 0
    total_dom = df_f["Ingreso_domicilio_recibido"].sum() if not df_f.empty else 0
    total_gastos = df_g["Monto"].sum() if not df_g.empty else 0
    saldo = total_prod + total_dom - total_gastos
    return total_prod, total_dom, total_gastos, saldo

def add_expense(concepto: str, monto: float):
    df_g = load_df("Gastos")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_row = {"Fecha": fecha, "Concepto": concepto, "Monto": monto}
    if df_g.empty:
        df_g = pd.DataFrame([new_row], columns=HEAD_GASTOS)
    else:
        df_g = pd.concat([df_g, pd.DataFrame([new_row])], ignore_index=True)
    save_local_csv_by_sheet("Gastos", df_g)
    try:
        safe_write_df_to_sheet(df_g, "Gastos", HEAD_GASTOS)
    except Exception as e:
        log_warn(f"Best-effort sync failed on add_expense: {e}")
    flush_cache()

def move_funds(amount: float, from_method: str, to_method: str, note: str="Movimiento interno"):
    df_f = load_df("FlujoCaja")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    neg = {"Fecha": fecha, "ID Pedido": 0, "Cliente": note + f" ({from_method} -> {to_method})", "Medio_pago": from_method, "Ingreso_productos_recibido": -float(amount), "Ingreso_domicilio_recibido": 0, "Saldo_pendiente_total": 0}
    pos = {"Fecha": fecha, "ID Pedido": 0, "Cliente": note + f" ({from_method} -> {to_method})", "Medio_pago": to_method, "Ingreso_productos_recibido": float(amount), "Ingreso_domicilio_recibido": 0, "Saldo_pendiente_total": 0}
    df_new = pd.DataFrame([neg, pos], columns=HEAD_FLUJO)
    if df_f.empty:
        df_f = df_new
    else:
        df_f = pd.concat([df_f, df_new], ignore_index=True)
    save_local_csv_by_sheet("FlujoCaja", df_f)
    try:
        safe_write_df_to_sheet(df_f, "FlujoCaja", HEAD_FLUJO)
    except Exception as e:
        log_warn(f"Best-effort sync failed on move_funds: {e}")
    flush_cache()

# ---------------------------
# REPORTS HELPERS
# ---------------------------

def unidades_vendidas_por_producto(df_det: pd.DataFrame = None) -> Dict[str, int]:
    if df_det is None or df_det.empty:
        return {p: 0 for p in PRODUCTOS.keys()}
    res = {}
    for _, r in df_det.iterrows():
        prod = r.get("Producto")
        qty = int(r.get("Cantidad", 0))
        res[prod] = res.get(prod, 0) + qty
    # ensure all canonical products present
    for p in PRODUCTOS.keys():
        res.setdefault(p, 0)
    return res

def ventas_por_semana(df_ped: pd.DataFrame) -> pd.DataFrame:
    if df_ped is None or df_ped.empty:
        return pd.DataFrame(columns=["Semana","Total"])
    df_local = df_ped.copy()
    df_local["Semana_entrega"] = pd.to_numeric(df_local["Semana_entrega"], errors='coerce').fillna(0).astype(int)
    df_local["Total_pedido"] = pd.to_numeric(df_local["Total_pedido"], errors='coerce').fillna(0)
    df = df_local.groupby("Semana_entrega")["Total_pedido"].sum().reset_index().rename(columns={"Semana_entrega":"Semana","Total_pedido":"Total"})
    return df.sort_values("Semana")

# ---------------------------
# STREAMLIT UI
# ---------------------------

st.set_page_config(page_title="AndicBlue — Gestión", page_icon=APP_ICON, layout="wide")
st.title("🫐 AndicBlue — Gestión de Pedidos, Inventario y Flujo (Local + Sync)")

# Top control bar
col1, col2, col3, col4 = st.columns([3,2,2,1])
with col1:
    st.markdown("#### Estado de sincronización")
with col2:
    sheets_status = "Disponible" if GS_CLIENT and GS_SPREADSHEET else "No conectado"
    st.info(f"Google Sheets: **{sheets_status}**")
with col3:
    st.button("Forzar recarga caché", on_click=flush_cache)
with col4:
    st.write(" ")

# Sidebar menu
st.sidebar.header("Menú")
menu = st.sidebar.selectbox("Selecciona módulo", ["Dashboard", "Clientes", "Pedidos", "Entregas/Pagos", "Inventario", "Flujo & Gastos", "Reportes", "Sincronización"])

# Quick helper - reload data
if st.sidebar.button("🔁 Sincronizar local -> Sheets (manual)"):
    # Try upload all local CSVs to sheets (best effort)
    try:
        df_clients = load_local_csv(CSV_CLIENTES, HEAD_CLIENTES)
        df_ped = load_local_csv(CSV_PEDIDOS, HEAD_PEDIDOS)
        df_det = load_local_csv(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
        df_inv = load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
        df_flu = load_local_csv(CSV_FLUJO, HEAD_FLUJO)
        df_gas = load_local_csv(CSV_GASTOS, HEAD_GASTOS)
        ok_clients = safe_write_df_to_sheet(df_clients, "Clientes", HEAD_CLIENTES)
        ok_ped = safe_write_df_to_sheet(df_ped, "Pedidos", HEAD_PEDIDOS)
        ok_det = safe_write_df_to_sheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        ok_inv = safe_write_df_to_sheet(df_inv, "Inventario", HEAD_INVENTARIO)
        ok_flu = safe_write_df_to_sheet(df_flu, "FlujoCaja", HEAD_FLUJO)
        ok_gas = safe_write_df_to_sheet(df_gas, "Gastos", HEAD_GASTOS)
        st.success("Intento de sincronización iniciado (revisa logs para detalles).")
        log_info("Manual sync local->sheets requested by user.")
    except Exception as e:
        st.error(f"Error al sincronizar manualmente: {e}")
        log_error(f"Manual sync failed: {e}")

# ---------------------------
# DASHBOARD
# ---------------------------
if menu == "Dashboard":
    st.header("📊 Dashboard — Resumen")
    df_ped = load_df("Pedidos")
    df_det = load_df("Pedidos_detalle")
    df_flu = load_df("FlujoCaja")
    df_gas = load_df("Gastos")
    df_inv = load_df("Inventario")
    df_clients = load_df("Clientes")

    # KPIs
    total_orders = 0 if df_ped.empty else len(df_ped)
    total_clients = 0 if df_clients.empty else df_clients["ID Cliente"].nunique()
    total_revenue = 0
    if not df_flu.empty:
        df_flu["Ingreso_productos_recibido"] = pd.to_numeric(df_flu["Ingreso_productos_recibido"], errors='coerce').fillna(0)
        df_flu["Ingreso_domicilio_recibido"] = pd.to_numeric(df_flu["Ingreso_domicilio_recibido"], errors='coerce').fillna(0)
        total_revenue = int(df_flu["Ingreso_productos_recibido"].sum() + df_flu["Ingreso_domicilio_recibido"].sum())
    total_expenses = 0 if df_gas.empty else int(pd.to_numeric(df_gas["Monto"], errors='coerce').sum())
    balance = total_revenue - total_expenses

    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Pedidos", f"{int(total_orders):,}")
    k2.metric("Clientes", f"{int(total_clients):,}")
    k3.metric("Ingresos registrados", f"{int(total_revenue):,} COP")
    k4.metric("Saldo neto", f"{int(balance):,} COP")

    st.markdown("---")
    st.subheader("Ventas por producto")
    if not df_det.empty and PLOTLY_AVAILABLE:
        df_det_local = df_det.copy()
        df_det_local["Subtotal"] = pd.to_numeric(df_det_local["Subtotal"], errors='coerce').fillna(0)
        ventas_prod = df_det_local.groupby("Producto")["Subtotal"].sum().reset_index().sort_values("Subtotal", ascending=False)
        fig = px.bar(ventas_prod, x="Producto", y="Subtotal", title="Ingresos por producto (COP)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay detalle de pedidos para graficar.")

    st.markdown("---")
    st.subheader("Stock actual")
    if not df_inv.empty:
        df_inv_local = df_inv.copy()
        df_inv_local["Stock"] = pd.to_numeric(df_inv_local["Stock"], errors='coerce').fillna(0)
        st.dataframe(df_inv_local.sort_values("Stock"), use_container_width=True)
    else:
        st.info("Inventario vacío.")

# ---------------------------
# CLIENTES
# ---------------------------
elif menu == "Clientes":
    st.header("👥 Clientes")
    df_clients = load_df("Clientes")
    st.dataframe(df_clients, use_container_width=True)
    with st.form("form_add_client"):
        st.subheader("Agregar nuevo cliente")
        nombre = st.text_input("Nombre completo")
        telefono = st.text_input("Teléfono")
        direccion = st.text_input("Dirección")
        submitted = st.form_submit_button("Agregar cliente")
        if submitted:
            if not nombre:
                st.error("Nombre obligatorio")
            else:
                cid = create_client(nombre, telefono, direccion)
                st.success(f"Cliente agregado con ID {cid}")

# ---------------------------
# PEDIDOS
# ---------------------------
elif menu == "Pedidos":
    st.header("📦 Pedidos — Crear / Editar / Eliminar")
    df_clients = load_df("Clientes")
    df_ped = load_df("Pedidos")
    df_det = load_df("Pedidos_detalle")
    df_inv = load_df("Inventario")

    # Create order section
    with st.expander("➕ Registrar nuevo pedido"):
        if df_clients.empty:
            st.warning("No hay clientes registrados. Agrega clientes en la sección de Clientes.")
        else:
            client_options = df_clients["ID Cliente"].astype(int).astype(str) + " - " + df_clients["Nombre"]
            client_options = client_options.tolist()
            client_select = st.selectbox("Cliente", ["Seleccionar..."] + client_options)
            if client_select == "Seleccionar...":
                st.info("Selecciona un cliente válido")
                new_cliente_id = None
            else:
                try:
                    new_cliente_id = int(client_select.split(" - ")[0])
                except Exception:
                    new_cliente_id = None
                    st.error("Formato de cliente inválido. Selecciona de la lista.")
            num_lines = st.number_input("Número de líneas", min_value=1, max_value=12, value=3)
            new_items = {}
            cols = st.columns(2)
            for i in range(int(num_lines)):
                with cols[i % 2]:
                    prod = st.selectbox(f"Producto {i+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"np_{i}")
                    qty = st.number_input(f"Cantidad {i+1}", min_value=0, step=1, value=0, key=f"nq_{i}")
                if prod and prod != "-- Ninguno --" and qty > 0:
                    new_items[prod] = new_items.get(prod, 0) + int(qty)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada entrega", value=datetime.now().date())
            if st.button("Crear pedido"):
                if new_cliente_id is None:
                    st.error("Selecciona un cliente válido")
                elif not new_items:
                    st.warning("No hay líneas definidas")
                else:
                    try:
                        pid = create_order_with_details(new_cliente_id, new_items, domicilio_bool=domicilio, fecha_entrega=fecha_entrega)
                        st.success(f"Pedido registrado con ID {pid}")
                    except Exception as e:
                        st.error(f"Error creando pedido: {e}")

    st.markdown("---")
    # Filter & edit existing
    if df_ped.empty:
        st.info("No hay pedidos registrados.")
    else:
        st.subheader("Listado de pedidos")
        coerce_week = pd.to_numeric(df_ped["Semana_entrega"], errors='coerce').fillna(0).astype(int)
        weeks = sorted(list(set(coerce_week.tolist())))
        week_opts = ["Todas"] + [str(w) for w in weeks if w > 0]
        week_filter = st.selectbox("Filtrar por semana (ISO)", week_opts)
        estado_filter = st.selectbox("Filtrar por estado", ["Todos", "Pendiente", "Entregado"])
        df_view = df_ped.copy()
        if estado_filter != "Todos":
            df_view = df_view[df_view["Estado"] == estado_filter]
        if week_filter != "Todas":
            df_view = df_view[df_view["Semana_entrega"].astype(int) == int(week_filter)]
        st.dataframe(df_view.reset_index(drop=True), use_container_width=True)

        if not df_view.empty:
            sel_id = st.selectbox("Selecciona ID Pedido para editar/eliminar", df_view["ID Pedido"].astype(int).tolist())
            if sel_id:
                header = df_ped[df_ped["ID Pedido"].astype(int) == int(sel_id)].iloc[0].to_dict()
                detalle = get_order_details(sel_id)
                st.markdown("### Detalle del pedido")
                st.write(f"Cliente: **{header.get('Nombre Cliente','')}**")
                st.write(f"Fecha: {header.get('Fecha','')}")
                st.write(f"Subtotal productos: {int(header.get('Subtotal_productos',0)):,} COP")
                st.write(f"Total pedido: {int(header.get('Total_pedido',0)):,} COP")
                st.write(f"Domicilio: {int(header.get('Monto_domicilio',0)):,} COP")
                st.write(f"Saldo pendiente: {int(header.get('Saldo_pendiente',0)):,} COP")

                st.markdown("#### Líneas (editar)")
                edited_items = {}
                if detalle.empty:
                    st.info("No hay líneas de detalle para este pedido.")
                else:
                    for i, r in detalle.iterrows():
                        cols = st.columns([4,2,1])
                        prod = cols[0].selectbox(f"Producto {i+1}", list(PRODUCTOS.keys()), index=list(PRODUCTOS.keys()).index(r["Producto"]) if r["Producto"] in PRODUCTOS else 0, key=f"ep_{i}")
                        qty = cols[1].number_input(f"Cantidad {i+1}", min_value=0, step=1, value=int(r["Cantidad"]), key=f"eq_{i}")
                        remove = cols[2].checkbox("Eliminar", key=f"er_{i}")
                        if not remove:
                            edited_items[prod] = edited_items.get(prod, 0) + int(qty)

                add_lines = st.number_input("Agregar nuevas líneas", min_value=0, max_value=8, value=0)
                if add_lines > 0:
                    for j in range(int(add_lines)):
                        a1,a2 = st.columns([4,2])
                        pnew = a1.selectbox(f"Nuevo producto {j+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"np2_{j}")
                        qnew = a2.number_input(f"Nueva cantidad {j+1}", min_value=0, step=1, key=f"nq2_{j}")
                        if pnew and pnew != "-- Ninguno --" and qnew > 0:
                            edited_items[pnew] = edited_items.get(pnew, 0) + int(qnew)

                domic_opt = st.selectbox("Domicilio", ["No", f"Sí ({DOMICILIO_COST} COP)"], index=0 if int(header.get("Monto_domicilio",0)) == 0 else 1)
                week_val = int(header.get("Semana_entrega", datetime.now().isocalendar().week))
                new_week = st.number_input("Semana entrega (ISO)", min_value=1, max_value=53, value=week_val)
                new_state = st.selectbox("Estado", ["Pendiente","Entregado"], index=0 if header.get("Estado","Pendiente")!="Entregado" else 1)

                if st.button("Guardar cambios en pedido"):
                    try:
                        if not edited_items:
                            st.warning("No hay líneas definidas. Si deseas eliminar el pedido, utiliza la opción eliminar.")
                        else:
                            new_domic = True if "Sí" in domic_opt else False
                            edit_order(sel_id, edited_items, new_domic_bool=new_domic, new_week=new_week)
                            # update estado
                            dfh = load_df("Pedidos")
                            idxh = dfh.index[dfh["ID Pedido"].astype(int) == int(sel_id)][0]
                            dfh.at[idxh, "Estado"] = new_state
                            save_local_csv_by_sheet("Pedidos", dfh)
                            try:
                                safe_write_df_to_sheet(dfh, "Pedidos", HEAD_PEDIDOS)
                            except Exception:
                                pass
                            flush_cache()
                            st.success("Pedido actualizado correctamente.")
                    except Exception as e:
                        st.error(f"Error actualizando pedido: {e}")

                if st.button("Eliminar pedido (revertir inventario)"):
                    try:
                        delete_order(sel_id)
                        st.success("Pedido eliminado y stock revertido.")
                    except Exception as e:
                        st.error(f"Error eliminando pedido: {e}")

# ---------------------------
# ENTREGAS / PAGOS
# ---------------------------
elif menu == "Entregas/Pagos":
    st.header("🚚 Entregas y Pagos")
    df_ped = load_df("Pedidos")
    if df_ped.empty:
        st.info("No hay pedidos.")
    else:
        estado_choice = st.selectbox("Estado", ["Todos","Pendiente","Entregado"])
        weeks = sorted(df_ped["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_ped.empty else []
        week_opts = ["Todas"] + [str(w) for w in weeks]
        week_filter = st.selectbox("Semana (ISO)", week_opts)
        df_view = df_ped.copy()
        if estado_choice != "Todos":
            df_view = df_view[df_view["Estado"] == estado_choice]
        if week_filter != "Todas":
            df_view = df_view[df_view["Semana_entrega"] == int(week_filter)]
        st.dataframe(df_view.reset_index(drop=True), use_container_width=True)

        if not df_view.empty:
            ids = df_view["ID Pedido"].astype(int).tolist()
            selection = st.selectbox("Selecciona ID Pedido", ids)
            idx = df_ped.index[df_ped["ID Pedido"].astype(int) == int(selection)][0]
            row = df_ped.loc[idx]
            st.markdown(f"**Cliente:** {row['Nombre Cliente']}")
            st.markdown(f"**Total:** {int(row['Total_pedido']):,} COP  •  **Pagado:** {int(row['Monto_pagado']):,} COP  •  **Saldo:** {int(row['Saldo_pendiente']):,} COP")
            detalle = get_order_details(selection)
            if not detalle.empty:
                st.table(detalle[["Producto","Cantidad","Precio_unitario","Subtotal"]].set_index(pd.Index(range(1,len(detalle)+1))))
            with st.form("form_payment"):
                amount = st.number_input("Monto a pagar (COP)", min_value=0, step=1000, value=int(row.get("Saldo_pendiente",0)))
                medio = st.selectbox("Medio de pago", ["Efectivo","Transferencia","Nequi","Daviplata"])
                submit_payment = st.form_submit_button("Registrar pago")
                if submit_payment:
                    try:
                        res = register_payment(int(selection), medio, amount)
                        st.success(f"Pago registrado: productos {res['prod_paid']} COP, domicilio {res['domicilio_paid']} COP. Saldo restante: {res['saldo_total']} COP")
                    except Exception as e:
                        st.error(f"Error registrando pago: {e}")

# ---------------------------
# INVENTARIO
# ---------------------------
elif menu == "Inventario":
    st.header("📦 Inventario")
    df_inv = load_df("Inventario")
    if df_inv.empty:
        st.info("Inventario vacío.")
    else:
        df_inv["Stock"] = pd.to_numeric(df_inv["Stock"], errors='coerce').fillna(0).astype(int)
        st.dataframe(df_inv, use_container_width=True)

    st.markdown("### Ajuste manual de stock (permite negativo)")
    df_inv_local = load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
    df_inv_local["Stock"] = pd.to_numeric(df_inv_local["Stock"], errors='coerce').fillna(0).astype(int)
    prod_list = sorted(df_inv_local["Producto"].astype(str).unique().tolist()) if not df_inv_local.empty else list(PRODUCTOS.keys())
    prod_sel = st.selectbox("Producto", prod_list)
    delta = st.number_input("Cantidad a sumar/restar (negativo para restar)", value=0, step=1)
    reason = st.text_input("Motivo (opcional)")

    if st.button("Aplicar ajuste"):
        try:
            if prod_sel in df_inv_local["Producto"].values:
                idx = df_inv_local.index[df_inv_local["Producto"] == prod_sel][0]
                df_inv_local.at[idx, "Stock"] = int(df_inv_local.at[idx, "Stock"]) + int(delta)
            else:
                df_inv_local = pd.concat([df_inv_local, pd.DataFrame([[prod_sel, int(delta)]], columns=HEAD_INVENTARIO)], ignore_index=True)
            # aggregate and save
            df_inv_local["Producto"] = df_inv_local["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
            df_inv_local = df_inv_local.groupby("Producto", as_index=False).agg({"Stock":"sum"})
            save_local_csv_by_sheet("Inventario", df_inv_local)
            try:
                safe_write_df_to_sheet(df_inv_local, "Inventario", HEAD_INVENTARIO)
            except Exception:
                pass
            flush_cache()
            st.success("Ajuste aplicado al inventario.")
            log_info(f"Inventory adjusted: {prod_sel} -> delta {delta} reason: {reason}")
        except Exception as e:
            st.error(f"Error aplicando ajuste de inventario: {e}")

# ---------------------------
# FLUJO & GASTOS
# ---------------------------
elif menu == "Flujo & Gastos":
    st.header("💰 Flujo de caja y Gastos")
    total_prod, total_dom, total_gastos, saldo = flow_summaries()
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Ingresos productos", f"{int(total_prod):,} COP".replace(",","."))
    c2.metric("Ingresos domicilios", f"{int(total_dom):,} COP".replace(",","."))
    c3.metric("Gastos", f"-{int(total_gastos):,} COP".replace(",","."))
    c4.metric("Saldo disponible", f"{int(saldo):,} COP".replace(",","."))

    st.markdown("---")
    st.subheader("Registro de movimientos entre medios (retiros, transferencias internas)")
    with st.form("form_move"):
        amt = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        from_m = st.selectbox("De (medio)", ["Transferencia","Efectivo","Nequi","Daviplata"])
        to_m = st.selectbox("A (medio)", ["Efectivo","Transferencia","Nequi","Daviplata"])
        note = st.text_input("Nota (opcional)", value="Movimiento interno")
        submit_move = st.form_submit_button("Registrar movimiento")
        if submit_move:
            if amt <= 0:
                st.error("Monto debe ser mayor a 0")
            elif from_m == to_m:
                st.error("Los medios deben ser diferentes")
            else:
                try:
                    move_funds(amt, from_m, to_m, note)
                    st.success("Movimiento registrado")
                except Exception as e:
                    st.error(f"Error registrando movimiento: {e}")

    st.markdown("---")
    st.subheader("Agregar gasto")
    with st.form("form_gasto"):
        concepto = st.text_input("Concepto")
        monto_g = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        add_gasto = st.form_submit_button("Agregar gasto")
        if add_gasto:
            try:
                add_expense(concepto, float(monto_g))
                st.success("Gasto agregado")
            except Exception as e:
                st.error(f"Error agregando gasto: {e}")

    st.markdown("---")
    st.subheader("Movimientos recientes")
    df_flu = load_df("FlujoCaja")
    if not df_flu.empty:
        st.dataframe(df_flu.tail(200), use_container_width=True)
    df_g = load_df("Gastos")
    if not df_g.empty:
        st.dataframe(df_g.tail(200), use_container_width=True)

# ---------------------------
# REPORTES
# ---------------------------
elif menu == "Reportes":
    st.header("📈 Reportes y Exportes")
    df_p = load_df("Pedidos")
    df_det = load_df("Pedidos_detalle")
    df_f = load_df("FlujoCaja")
    df_g = load_df("Gastos")
    df_inv = load_df("Inventario")

    st.subheader("Pedidos (cabecera)")
    st.dataframe(df_p, use_container_width=True)
    st.subheader("Detalle Pedidos")
    st.dataframe(df_det, use_container_width=True)
    st.subheader("Flujo caja")
    st.dataframe(df_f, use_container_width=True)
    st.subheader("Gastos")
    st.dataframe(df_g, use_container_width=True)
    st.subheader("Inventario")
    if not df_inv.empty:
        st.dataframe(df_inv, use_container_width=True)

    st.markdown("---")
    st.subheader("Exportar CSV locales")
    for path in [CSV_CLIENTES, CSV_PEDIDOS, CSV_PEDIDOS_DETALLE, CSV_INVENTARIO, CSV_FLUJO, CSV_GASTOS]:
        if path.exists():
            with open(path, "rb") as f:
                st.download_button(f"Descargar {path.name}", f.read(), file_name=path.name, mime="text/csv")
        else:
            st.write(f"{path.name} no existe aún.")

# ---------------------------
# SINCRONIZACIÓN & CONFIG
# ---------------------------
elif menu == "Sincronización":
    st.header("🔄 Sincronización con Google Sheets (manual / diagnóstico)")
    st.write("Estado actual del cliente Google Sheets y del Spreadsheet.")
    st.write(f"gspread disponible: {GS_AVAILABLE}")
    st.write(f"Cliente inicializado: {'Sí' if GS_CLIENT else 'No'}")
    st.write(f"Spreadsheet detectado: {'Sí' if GS_SPREADSHEET else 'No'}")
    st.write("Puedes realizar sincronizaciones manuales desde aquí.")

    if st.button("Sincronizar local -> Google Sheets (todo)"):
        # upload all CSVs to corresponding sheets (best effort)
        try:
            df_clients = load_local_csv(CSV_CLIENTES, HEAD_CLIENTES)
            df_ped = load_local_csv(CSV_PEDIDOS, HEAD_PEDIDOS)
            df_det = load_local_csv(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
            df_inv = load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
            df_flu = load_local_csv(CSV_FLUJO, HEAD_FLUJO)
            df_gas = load_local_csv(CSV_GASTOS, HEAD_GASTOS)
            ok1 = safe_write_df_to_sheet(df_clients, "Clientes", HEAD_CLIENTES)
            ok2 = safe_write_df_to_sheet(df_ped, "Pedidos", HEAD_PEDIDOS)
            ok3 = safe_write_df_to_sheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
            ok4 = safe_write_df_to_sheet(df_inv, "Inventario", HEAD_INVENTARIO)
            ok5 = safe_write_df_to_sheet(df_flu, "FlujoCaja", HEAD_FLUJO)
            ok6 = safe_write_df_to_sheet(df_gas, "Gastos", HEAD_GASTOS)
            st.success("Sincronización iniciada (best-effort). Revisa logs para resultados.")
        except Exception as e:
            st.error(f"Error al sincronizar: {e}")
            log_error(f"Sync error: {e}")

    st.markdown("---")
    st.subheader("Logs recientes")
    if CSV_LOG.exists():
        with open(CSV_LOG, "r") as lf:
            logs = lf.read().splitlines()[-200:]
            st.text("\n".join(logs[-200:]))
    else:
        st.info("No hay logs todavía.")

# Footer
st.markdown("---")
st.caption("AndicBlue — App local con respaldo CSV y sincronización controlada a Google Sheets. Diseñado para operar localmente y evitar errores por cuota de la API.")

# ---------------------------
# FIN DEL ARCHIVO
# ---------------------------

