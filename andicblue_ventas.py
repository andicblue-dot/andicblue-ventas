# andicblue_ventas_full.py
# AndicBlue — Versión final integrada
# - Local CSV backup + optional Google Sheets sync (best-effort)
# - Pedidos con detalle, inventario editable, flujo & gastos, reportes
# - UI Azul moderno, logo: ./andicblue_logo.png
# Requisitos:
# pip install streamlit pandas gspread google-auth plotly openpyxl
# Optional: configure st.secrets["gcp_service_account"] with service account JSON to enable Sheets sync
# Run: streamlit run andicblue_ventas_full.py

import streamlit as st
import pandas as pd
import numpy as np
import os
import time
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple

# Optional libs
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GS_AVAILABLE = True
except Exception:
    GS_AVAILABLE = False

try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except Exception:
    PLOTLY_AVAILABLE = False

# ---------------------------
# CONFIG
# ---------------------------
APP_TITLE = "AndicBlue — Gestión de Pedidos"
APP_ICON = "🫐"
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)
LOG_FILE = DATA_DIR / "andicblue_logs.txt"

# CSV files
CSV_CLIENTES = DATA_DIR / "clientes.csv"
CSV_PEDIDOS = DATA_DIR / "pedidos.csv"               # header (cabecera)
CSV_PEDIDOS_DETALLE = DATA_DIR / "pedidos_detalle.csv"
CSV_INVENTARIO = DATA_DIR / "inventario.csv"
CSV_FLUJO = DATA_DIR / "flujo.csv"
CSV_GASTOS = DATA_DIR / "gastos.csv"

# Price list (canonical)
PRODUCTOS = {
    "Docena de Arándanos 125g": 52500,
    "Arandanos_125g": 5000,
    "Arandanos_250g": 10000,
    "Arandanos_500g": 20000,
    "Kilo_industrial": 30000,
    "Mermelada_azucar": 16000,
    "Mermelada_sin_azucar": 20000,
}

DOMICILIO_COST = 3000  # COP

# HEADERS
HEAD_CLIENTES = ["ID Cliente", "Nombre", "Telefono", "Direccion"]
HEAD_PEDIDOS = [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
    "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Estado",
    "Medio_pago", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"
]
HEAD_PEDIDOS_DETALLE = ["ID Pedido", "Producto", "Cantidad", "Precio_unitario", "Subtotal"]
HEAD_INVENTARIO = ["Producto", "Stock", "Precio Unitario"]
HEAD_FLUJO = [
    "Fecha", "ID Pedido", "Cliente", "Medio_pago",
    "Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"
]
HEAD_GASTOS = ["Fecha", "Concepto", "Monto"]

# Spreadsheet name for optional sync
SHEET_NAME = "andicblue_pedidos"

# Logging config
logging.basicConfig(filename=str(LOG_FILE), level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def log_info(msg): logging.info(msg)
def log_warn(msg): logging.warning(msg)
def log_error(msg): logging.error(msg)

# ---------------------------
# UTIL: ensure CSVs and headers (avoid duplicate header rows)
# ---------------------------
def ensure_csv(path: Path, headers: List[str]):
    if not path.exists():
        pd.DataFrame(columns=headers).to_csv(path, index=False)
        log_info(f"Created {path}")
        return
    try:
        df = pd.read_csv(path)
    except Exception as e:
        log_warn(f"Error reading {path}: {e}. Recreating.")
        pd.DataFrame(columns=headers).to_csv(path, index=False)
        return
    # If first row equals headers (duplicate header row), drop it
    if not df.empty:
        first = df.iloc[0].astype(str).tolist()
        if first == headers:
            df = df.drop(index=0).reset_index(drop=True)
            df.to_csv(path, index=False)
            log_info(f"Removed duplicated header row from {path}")
    # Ensure all expected columns exist
    for h in headers:
        if h not in df.columns:
            df[h] = ""
            log_info(f"Added missing column {h} to {path}")
    # reorder
    df = df.reindex(columns=headers)
    df.to_csv(path, index=False)

# Ensure all CSVs exist with proper headers
ensure_csv(CSV_CLIENTES, HEAD_CLIENTES)
ensure_csv(CSV_PEDIDOS, HEAD_PEDIDOS)
ensure_csv(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
ensure_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
ensure_csv(CSV_FLUJO, HEAD_FLUJO)
ensure_csv(CSV_GASTOS, HEAD_GASTOS)

# ---------------------------
# GOOGLE SHEETS (optional, safe)
# ---------------------------
GS_CLIENT = None
GS_SHEET = None
def init_gs():
    global GS_CLIENT, GS_SHEET
    if not GS_AVAILABLE:
        log_warn("gspread not available.")
        return False
    if "gcp_service_account" not in st.secrets:
        log_warn("No gcp_service_account in st.secrets.")
        return False
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])
        GS_CLIENT = gspread.authorize(creds)
        try:
            GS_SHEET = GS_CLIENT.open(SHEET_NAME)
        except Exception:
            GS_SHEET = None
        log_info("Google Sheets client initialized")
        return True
    except Exception as e:
        log_warn(f"Error init GS: {e}")
        return False

# Attempt init once
init_gs()

# Safe wrapper functions for sheet ops with retries and fallback
def exponential_backoff(attempt:int):
    time.sleep(min(5, 0.5*(2**attempt)))

def safe_get_worksheet(title):
    if GS_CLIENT is None:
        return None
    global GS_SHEET
    for attempt in range(4):
        try:
            if GS_SHEET is None:
                GS_SHEET = GS_CLIENT.open(SHEET_NAME)
            ws = GS_SHEET.worksheet(title)
            return ws
        except Exception as e:
            msg=str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                log_warn(f"Sheets quota error reading {title}, attempt {attempt+1}")
                exponential_backoff(attempt)
                continue
            try:
                # try create worksheet
                GS_SHEET.add_worksheet(title=title, rows="1000", cols="20")
                ws = GS_SHEET.worksheet(title)
                return ws
            except Exception as ex:
                log_warn(f"Could not create worksheet {title}: {ex}")
                exponential_backoff(attempt)
                continue
    log_warn(f"Unable to access worksheet {title}")
    return None

def safe_read_sheet(title, headers):
    ws = safe_get_worksheet(title)
    if ws is None:
        return None
    for attempt in range(4):
        try:
            records = ws.get_all_records()
            df = pd.DataFrame(records)
            # ensure columns
            for h in headers:
                if h not in df.columns:
                    df[h] = ""
            df = df[headers]
            return df
        except Exception as e:
            log_warn(f"Error reading sheet {title}: {e}")
            exponential_backoff(attempt)
            continue
    log_warn(f"Failed reading sheet {title}")
    return None

def safe_write_sheet(df: pd.DataFrame, title:str, headers:List[str]) -> bool:
    ws = safe_get_worksheet(title)
    if ws is None:
        return False
    try:
        # reorder and replace NaNs
        df2 = df.copy()
        for h in headers:
            if h not in df2.columns:
                df2[h] = ""
        df2 = df2[headers].where(pd.notnull(df2), "")
        # clear then write rows
        ws.clear()
        ws.append_row(headers)
        # append rows in batch if supported
        rows = df2.values.tolist()
        for r in rows:
            ws.append_row([("" if v is None else v) for v in r])
        log_info(f"Wrote {len(rows)} rows to sheet {title}")
        return True
    except Exception as e:
        log_warn(f"Error writing to sheet {title}: {e}")
        return False

# ---------------------------
# LOCAL CSV helpers
# ---------------------------
def load_local_csv(path:Path, headers:List[str]) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.DataFrame(columns=headers)
        df.to_csv(path, index=False)
        return df
    # remove accidental duplicated header row
    if not df.empty:
        first = df.iloc[0].astype(str).tolist()
        if first == headers:
            df = df.drop(index=0).reset_index(drop=True)
            df.to_csv(path, index=False)
            log_info(f"Removed duplicated header row from {path}")
    # ensure columns
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    return df[headers]

def save_local_csv(path:Path, df:pd.DataFrame, headers:List[str]):
    try:
        for h in headers:
            if h not in df.columns:
                df[h] = ""
        df2 = df[headers]
        df2.to_csv(path, index=False)
        log_info(f"Saved {path} with {len(df2)} rows")
        return True
    except Exception as e:
        log_warn(f"Error saving {path}: {e}")
        return False

# ---------------------------
# Higher level load (use sheets when available else local)
# ---------------------------
@st.cache_data(ttl=30)
def load_df(name:str) -> pd.DataFrame:
    mapping = {
        "Clientes": (CSV_CLIENTES, HEAD_CLIENTES),
        "Pedidos": (CSV_PEDIDOS, HEAD_PEDIDOS),
        "Pedidos_detalle": (CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE),
        "Inventario": (CSV_INVENTARIO, HEAD_INVENTARIO),
        "FlujoCaja": (CSV_FLUJO, HEAD_FLUJO),
        "Gastos": (CSV_GASTOS, HEAD_GASTOS)
    }
    if name not in mapping:
        return pd.DataFrame()
    path, headers = mapping[name]
    # try sheets first
    try:
        df_sheet = safe_read_sheet(name, headers)
        if df_sheet is not None:
            # persist to local for fallback
            save_local_csv(path, df_sheet, headers)
            return df_sheet
    except Exception as e:
        log_warn(f"safe_read_sheet failed for {name}: {e}")
    # fallback local
    df_local = load_local_csv(path, headers)
    return df_local

def flush_caches():
    st.cache_data.clear()
    log_info("Cleared st.cache_data")

# ---------------------------
# Business logic: orders, detalle, inventory adjustments, payments
# ---------------------------
def canonical_prod(name:str)->str:
    if not isinstance(name,str): return name
    s=name.strip()
    if s=="" or s.lower()=="producto": return None
    # match by normalized forms
    def norm(x): return x.lower().replace(" ", "").replace("_","").replace("-","")
    ns=norm(s)
    for k in PRODUCTOS.keys():
        if norm(k)==ns or ns in norm(k) or norm(k) in ns:
            return k
    return s

def next_id(df:pd.DataFrame, col:str):
    if df is None or df.empty or col not in df.columns:
        return 1
    try:
        vals = pd.to_numeric(df[col], errors='coerce').dropna().astype(int).tolist()
        return max(vals)+1 if vals else 1
    except:
        return len(df)+1

def ensure_inventory_has_products():
    df_inv = load_df("Inventario")
    if df_inv.empty:
        # seed from PRODUCTOS
        rows=[]
        for p,pr in PRODUCTOS.items():
            rows.append({"Producto":p,"Stock":0,"Precio Unitario":pr})
        df_inv = pd.DataFrame(rows, columns=HEAD_INVENTARIO)
        save_local_csv(CSV_INVENTARIO, df_inv, HEAD_INVENTARIO)
    return df_inv

# create client
def add_cliente(nombre, telefono="", direccion=""):
    dfc = load_df("Clientes")
    cid = next_id(dfc,"ID Cliente")
    new = {"ID Cliente":cid,"Nombre":nombre,"Telefono":telefono,"Direccion":direccion}
    dfc = pd.concat([dfc,pd.DataFrame([new])], ignore_index=True)
    save_local_csv(CSV_CLIENTES, dfc, HEAD_CLIENTES)
    try:
        safe_write_sheet(dfc, "Clientes", HEAD_CLIENTES)
    except: pass
    flush_caches()
    return cid

def build_detalle_rows(pid:int, items:Dict[str,int]) -> List[List]:
    rows=[]
    for prod,qty in items.items():
        p = canonical_prod(prod)
        if p is None:
            continue
        price = PRODUCTOS.get(p,0)
        subtotal = int(qty)*int(price)
        rows.append([pid,p,int(qty),int(price),int(subtotal)])
    return rows

def create_order(cliente_id:int, items:Dict[str,int], domicilio_bool:bool=False, fecha_entrega:date=None):
    # validate cliente
    dfc = load_df("Clientes")
    if dfc.empty or int(cliente_id) not in dfc["ID Cliente"].astype(int).tolist():
        raise ValueError("ID cliente no encontrado")
    name = dfc.loc[dfc["ID Cliente"].astype(int)==int(cliente_id),"Nombre"].values[0]
    dfped = load_df("Pedidos")
    dfdet = load_df("Pedidos_detalle")
    dfinv = load_df("Inventario")
    if dfinv is None or dfinv.empty:
        dfinv = ensure_inventory_has_products()
    # compute totals
    subtotal=0
    for k,v in items.items():
        p = canonical_prod(k)
        if p is None: continue
        price = PRODUCTOS.get(p,0)
        subtotal += int(v)*int(price)
    domicilio = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio
    fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    semana = int(pd.to_datetime(fecha_entrega).isocalendar().week) if fecha_entrega else datetime.now().isocalendar().week
    pid = next_id(dfped, "ID Pedido")
    header = {"ID Pedido":pid,"Fecha":fecha_str,"ID Cliente":int(cliente_id),"Nombre Cliente":name,
              "Subtotal_productos":subtotal,"Monto_domicilio":domicilio,"Total_pedido":total,
              "Estado":"Pendiente","Medio_pago":"","Monto_pagado":0,"Saldo_pendiente":total,"Semana_entrega":semana}
    dfped = pd.concat([dfped,pd.DataFrame([header])], ignore_index=True)
    # add detalle lines
    for prod, qty in items.items():
        p=canonical_prod(prod)
        if p is None: continue
        price = PRODUCTOS.get(p,0)
        subtotal_line = int(qty)*int(price)
        dfdet = pd.concat([dfdet,pd.DataFrame([[pid,p,int(qty),int(price),int(subtotal_line)]], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
        # subtract inventory (allow negative)
        if p in dfinv["Producto"].values:
            idx = dfinv.index[dfinv["Producto"]==p][0]
            dfinv.at[idx,"Stock"] = int(dfinv.at[idx,"Stock"]) - int(qty)
        else:
            dfinv = pd.concat([dfinv,pd.DataFrame([[p,-int(qty), PRODUCTOS.get(p,0)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # aggregate inventory
    dfinv["Producto"]=dfinv["Producto"].astype(str)
    dfinv = dfinv.groupby("Producto", as_index=False).agg({"Stock":"sum","Precio Unitario":"first"})
    # persist local
    save_local_csv(CSV_PEDIDOS, dfped, HEAD_PEDIDOS)
    save_local_csv(CSV_PEDIDOS_DETALLE, dfdet, HEAD_PEDIDOS_DETALLE)
    save_local_csv(CSV_INVENTARIO, dfinv, HEAD_INVENTARIO)
    # try sheets best effort
    try:
        safe_write_sheet(dfped, "Pedidos", HEAD_PEDIDOS)
        safe_write_sheet(dfdet, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        safe_write_sheet(dfinv, "Inventario", HEAD_INVENTARIO)
    except: pass
    flush_caches()
    log_info(f"Created order {pid} for client {cliente_id}")
    return pid

def get_order_details(pid:int) -> pd.DataFrame:
    dfdet = load_df("Pedidos_detalle")
    if dfdet.empty:
        return pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    return dfdet[dfdet["ID Pedido"].astype(int)==int(pid)].copy()

def edit_order(pid:int, new_items:Dict[str,int], new_domic=None, new_week=None):
    dfped = load_df("Pedidos")
    dfdet = load_df("Pedidos_detalle")
    dfinv = load_df("Inventario")
    if dfped.empty or int(pid) not in dfped["ID Pedido"].astype(int).tolist():
        raise ValueError("Pedido no encontrado")
    # revert inventory from existing lines
    old = dfdet[dfdet["ID Pedido"].astype(int)==int(pid)]
    for _,r in old.iterrows():
        prod = canonical_prod(r["Producto"])
        qty = int(r["Cantidad"])
        if prod in dfinv["Producto"].values:
            idx = dfinv.index[dfinv["Producto"]==prod][0]
            dfinv.at[idx,"Stock"] = int(dfinv.at[idx,"Stock"]) + qty
        else:
            dfinv = pd.concat([dfinv,pd.DataFrame([[prod,qty,PRODUCTOS.get(prod,0)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # remove old lines
    dfdet = dfdet[dfdet["ID Pedido"].astype(int)!=int(pid)].reset_index(drop=True)
    # add new lines and subtract inventory
    for prod,qty in new_items.items():
        p = canonical_prod(prod)
        price = PRODUCTOS.get(p,0)
        subtotal = int(qty)*int(price)
        dfdet = pd.concat([dfdet,pd.DataFrame([[pid,p,int(qty),int(price),int(subtotal)]], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
        if p in dfinv["Producto"].values:
            idx = dfinv.index[dfinv["Producto"]==p][0]
            dfinv.at[idx,"Stock"] = int(dfinv.at[idx,"Stock"]) - int(qty)
        else:
            dfinv = pd.concat([dfinv,pd.DataFrame([[p,-int(qty),PRODUCTOS.get(p,0)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # update header totals
    subtotal_new = sum(PRODUCTOS.get(canonical_prod(p),0)*int(q) for p,q in new_items.items())
    idxh = dfped.index[dfped["ID Pedido"].astype(int)==int(pid)][0]
    domicilio = dfped.at[idxh,"Monto_domicilio"] if new_domic is None else (DOMICILIO_COST if new_domic else 0)
    total_new = subtotal_new + domicilio
    pagado = float(dfped.at[idxh,"Monto_pagado"])
    saldo_new = total_new - pagado
    dfped.at[idxh,"Subtotal_productos"]=subtotal_new
    dfped.at[idxh,"Monto_domicilio"]=domicilio
    dfped.at[idxh,"Total_pedido"]=total_new
    dfped.at[idxh,"Saldo_pendiente"]=saldo_new
    if new_week:
        dfped.at[idxh,"Semana_entrega"]=int(new_week)
    # aggregate inventory
    dfinv["Producto"]=dfinv["Producto"].astype(str)
    dfinv = dfinv.groupby("Producto", as_index=False).agg({"Stock":"sum","Precio Unitario":"first"})
    # persist
    save_local_csv(CSV_PEDIDOS, dfped, HEAD_PEDIDOS)
    save_local_csv(CSV_PEDIDOS_DETALLE, dfdet, HEAD_PEDIDOS_DETALLE)
    save_local_csv(CSV_INVENTARIO, dfinv, HEAD_INVENTARIO)
    try:
        safe_write_sheet(dfped, "Pedidos", HEAD_PEDIDOS)
        safe_write_sheet(dfdet, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        safe_write_sheet(dfinv, "Inventario", HEAD_INVENTARIO)
    except: pass
    flush_caches()
    log_info(f"Edited order {pid}")

def delete_order(pid:int):
    dfped = load_df("Pedidos")
    dfdet = load_df("Pedidos_detalle")
    dfinv = load_df("Inventario")
    if dfped.empty or int(pid) not in dfped["ID Pedido"].astype(int).tolist():
        raise ValueError("Pedido no encontrado")
    detalle = dfdet[dfdet["ID Pedido"].astype(int)==int(pid)]
    for _,r in detalle.iterrows():
        prod = canonical_prod(r["Producto"])
        qty = int(r["Cantidad"])
        if prod in dfinv["Producto"].values:
            idx = dfinv.index[dfinv["Producto"]==prod][0]
            dfinv.at[idx,"Stock"] = int(dfinv.at[idx,"Stock"]) + qty
        else:
            dfinv = pd.concat([dfinv,pd.DataFrame([[prod,qty,PRODUCTOS.get(prod,0)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    dfdet = dfdet[dfdet["ID Pedido"].astype(int)!=int(pid)].reset_index(drop=True)
    dfped = dfped[dfped["ID Pedido"].astype(int)!=int(pid)].reset_index(drop=True)
    dfinv["Producto"]=dfinv["Producto"].astype(str)
    dfinv = dfinv.groupby("Producto", as_index=False).agg({"Stock":"sum","Precio Unitario":"first"})
    save_local_csv(CSV_PEDIDOS, dfped, HEAD_PEDIDOS)
    save_local_csv(CSV_PEDIDOS_DETALLE, dfdet, HEAD_PEDIDOS_DETALLE)
    save_local_csv(CSV_INVENTARIO, dfinv, HEAD_INVENTARIO)
    try:
        safe_write_sheet(dfped, "Pedidos", HEAD_PEDIDOS)
        safe_write_sheet(dfdet, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
        safe_write_sheet(dfinv, "Inventario", HEAD_INVENTARIO)
    except: pass
    flush_caches()
    log_info(f"Deleted order {pid}")

# payments and flow
def register_payment(pid:int, medio:str, monto:float) -> Dict[str,float]:
    dfped = load_df("Pedidos")
    dffl = load_df("FlujoCaja")
    if dfped.empty or int(pid) not in dfped["ID Pedido"].astype(int).tolist():
        raise ValueError("Pedido no encontrado")
    idx = dfped.index[dfped["ID Pedido"].astype(int)==int(pid)][0]
    subtotal_products = float(dfped.at[idx,"Subtotal_productos"])
    domicilio_monto = float(dfped.at[idx,"Monto_domicilio"])
    monto_anterior = float(dfped.at[idx,"Monto_pagado"])
    monto_total = float(monto_anterior) + float(monto)
    prod_total_acum = min(monto_total, subtotal_products)
    dom_total_acum = min(max(0, monto_total - subtotal_products), domicilio_monto)
    prod_pagado_antes = min(monto_anterior, subtotal_products)
    dom_pagado_antes = max(0, monto_anterior - subtotal_products)
    prod_now = max(0, prod_total_acum - prod_pagado_antes)
    domicilio_now = max(0, dom_total_acum - dom_pagado_antes)
    saldo_total = (subtotal_products - prod_total_acum) + (domicilio_monto - dom_total_acum)
    monto_total_reg = prod_total_acum + dom_total_acum
    # update header
    dfped.at[idx,"Monto_pagado"] = monto_total_reg
    dfped.at[idx,"Saldo_pendiente"] = saldo_total
    dfped.at[idx,"Medio_pago"] = medio
    dfped.at[idx,"Estado"] = "Entregado" if saldo_total==0 else "Pendiente"
    # append flujo row with amounts actually received in this transaction
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_flow = {"Fecha":fecha,"ID Pedido":int(pid),"Cliente":dfped.at[idx,"Nombre Cliente"],"Medio_pago":medio,
                "Ingreso_productos_recibido":prod_now,"Ingreso_domicilio_recibido":domicilio_now,"Saldo_pendiente_total":saldo_total}
    if dffl.empty:
        dffl = pd.DataFrame([new_flow], columns=HEAD_FLUJO)
    else:
        dffl = pd.concat([dffl,pd.DataFrame([new_flow])], ignore_index=True)
    save_local_csv(CSV_PEDIDOS, dfped, HEAD_PEDIDOS)
    save_local_csv(CSV_FLUJO, dffl, HEAD_FLUJO)
    try:
        safe_write_sheet(dfped, "Pedidos", HEAD_PEDIDOS)
        safe_write_sheet(dffl, "FlujoCaja", HEAD_FLUJO)
    except: pass
    flush_caches()
    log_info(f"Registered payment for order {pid}: {monto} via {medio}")
    return {"prod_paid":prod_now,"domicilio_paid":domicilio_now,"saldo_total":saldo_total}

def totals_by_payment_method() -> Dict[str,float]:
    dff = load_df("FlujoCaja")
    if dff.empty:
        return {}
    dff["Ingreso_productos_recibido"] = pd.to_numeric(dff["Ingreso_productos_recibido"], errors='coerce').fillna(0)
    dff["Ingreso_domicilio_recibido"] = pd.to_numeric(dff["Ingreso_domicilio_recibido"], errors='coerce').fillna(0)
    dff["total"] = dff["Ingreso_productos_recibido"] + dff["Ingreso_domicilio_recibido"]
    grouped = dff.groupby("Medio_pago")["total"].sum().to_dict()
    return {str(k):float(v) for k,v in grouped.items()}

def flow_summaries() -> Tuple[float,float,float,float]:
    dff = load_df("FlujoCaja")
    dfg = load_df("Gastos")
    if not dff.empty:
        dff["Ingreso_productos_recibido"] = pd.to_numeric(dff["Ingreso_productos_recibido"], errors='coerce').fillna(0)
        dff["Ingreso_domicilio_recibido"] = pd.to_numeric(dff["Ingreso_domicilio_recibido"], errors='coerce').fillna(0)
    total_prod = dff["Ingreso_productos_recibido"].sum() if not dff.empty else 0
    total_dom = dff["Ingreso_domicilio_recibido"].sum() if not dff.empty else 0
    total_gastos = dfg["Monto"].sum() if not dfg.empty else 0
    saldo = total_prod + total_dom - total_gastos
    return total_prod, total_dom, total_gastos, saldo

def add_expense(concepto:str, monto:float):
    dfg = load_df("Gastos")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new = {"Fecha":fecha,"Concepto":concepto,"Monto":monto}
    if dfg.empty:
        dfg = pd.DataFrame([new], columns=HEAD_GASTOS)
    else:
        dfg = pd.concat([dfg,pd.DataFrame([new])], ignore_index=True)
    save_local_csv(CSV_GASTOS, dfg, HEAD_GASTOS)
    try:
        safe_write_sheet(dfg, "Gastos", HEAD_GASTOS)
    except: pass
    flush_caches()

def move_funds(amount:float, from_method:str, to_method:str, note:str="Movimiento interno"):
    dff = load_df("FlujoCaja")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    neg = {"Fecha":fecha,"ID Pedido":0,"Cliente":note+f" ({from_method} -> {to_method})","Medio_pago":from_method,
           "Ingreso_productos_recibido":-float(amount),"Ingreso_domicilio_recibido":0,"Saldo_pendiente_total":0}
    pos = {"Fecha":fecha,"ID Pedido":0,"Cliente":note+f" ({from_method} -> {to_method})","Medio_pago":to_method,
           "Ingreso_productos_recibido":float(amount),"Ingreso_domicilio_recibido":0,"Saldo_pendiente_total":0}
    dd = pd.DataFrame([neg,pos], columns=HEAD_FLUJO)
    if dff.empty:
        dff = dd
    else:
        dff = pd.concat([dff, dd], ignore_index=True)
    save_local_csv(CSV_FLUJO, dff, HEAD_FLUJO)
    try:
        safe_write_sheet(dff, "FlujoCaja", HEAD_FLUJO)
    except: pass
    flush_caches()
    log_info(f"Moved funds {amount} from {from_method} to {to_method}")

# report helpers
def unidades_vendidas_por_producto(df_det=None) -> Dict[str,int]:
    if df_det is None or df_det.empty:
        return {p:0 for p in PRODUCTOS.keys()}
    res={}
    for _,r in df_det.iterrows():
        prod = r["Producto"]; qty = int(r.get("Cantidad",0))
        res[prod] = res.get(prod,0)+qty
    for p in PRODUCTOS.keys(): res.setdefault(p,0)
    return res

# ---------------------------
# STREAMLIT UI
# ---------------------------
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON, layout="wide")
# Apply some CSS for blue dashboard
st.markdown("""
<style>
/* Sidebar */
[data-testid="stSidebar"] { background: linear-gradient(180deg,#003e6b,#0050a0); color: white; }
/* Buttons */
div.stButton > button { background-color: #0050A0; color: white; border-radius:8px; padding:6px 12px; }
div.stButton > button:hover { background-color:#3CA6FF; color:white; }
/* Cards */
.card { background: white; border-radius:12px; padding:12px; box-shadow:0 4px 12px rgba(0,0,0,0.08); }
.metric { border-left:6px solid #0050A0; padding:10px; border-radius:8px; background:white; }
</style>
""", unsafe_allow_html=True)

# Header with logo and contact
col1,col2 = st.columns([1,4])
with col1:
    if os.path.exists("andicblue_logo.png"):
        st.image("andicblue_logo.png", width=140)
with col2:
    st.markdown("<h1 style='color:#003e6b'>🫐 AndicBlue — Gestión de Pedidos & Flujo</h1>", unsafe_allow_html=True)
    st.markdown("**Cultivo orgullosamente nariñense**  •  📍 Nariño  •  📞 +57 300 000 0000  •  ✉ contacto@andicblue.com")

st.markdown("---")
# Sidebar quick navigation & status
st.sidebar.title("AndicBlue | Menú")
menu = st.sidebar.radio("Ir a:", ["Dashboard","Clientes","Pedidos","Entregas/Pagos","Inventario","Flujo & Gastos","Reportes","Sincronización"], index=2)
st.sidebar.markdown("---")
# Week info
current_week = datetime.now().isocalendar().week
st.sidebar.markdown(f"### Semana actual: **{current_week}**")
# Sync controls
st.sidebar.markdown("---")
st.sidebar.markdown("#### Sincronización y backups")
if GS_AVAILABLE and "gcp_service_account" in st.secrets:
    st.sidebar.success("Google Sheets: configurado")
else:
    st.sidebar.info("Google Sheets: no configurado (opcional)")
if st.sidebar.button("Forzar sincronización local -> Sheets"):
    # Best-effort: upload all local CSVs to sheets
    try:
        dfc = load_local_csv(CSV_CLIENTES, HEAD_CLIENTES)
        dfp = load_local_csv(CSV_PEDIDOS, HEAD_PEDIDOS)
        dfd = load_local_csv(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
        dfi = load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
        dff = load_local_csv(CSV_FLUJO, HEAD_FLUJO)
        dfg = load_local_csv(CSV_GASTOS, HEAD_GASTOS)
        ok1 = safe_write_sheet(dfc,"Clientes",HEAD_CLIENTES)
        ok2 = safe_write_sheet(dfp,"Pedidos",HEAD_PEDIDOS)
        ok3 = safe_write_sheet(dfd,"Pedidos_detalle",HEAD_PEDIDOS_DETALLE)
        ok4 = safe_write_sheet(dfi,"Inventario",HEAD_INVENTARIO)
        ok5 = safe_write_sheet(dff,"FlujoCaja",HEAD_FLUJO)
        ok6 = safe_write_sheet(dfg,"Gastos",HEAD_GASTOS)
        st.success("Sincronización iniciada. Revisar logs para resultados.")
    except Exception as e:
        st.error(f"Error sincronizando: {e}")

if st.sidebar.button("Forzar recarga caché"):
    flush_caches()
    st.experimental_rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("Contacto: contacto@andicblue.com")

# ---------------------------
# DASHBOARD
# ---------------------------
if menu == "Dashboard":
    st.header("📊 Resumen - Dashboard")
    dfp = load_df("Pedidos")
    dff = load_df("FlujoCaja")
    dfi = load_df("Inventario")
    dfdet = load_df("Pedidos_detalle")
    dfc = load_df("Clientes")
    # KPIs
    total_orders = 0 if dfp.empty else len(dfp)
    total_clients = 0 if dfc.empty else dfc["ID Cliente"].nunique()
    total_revenue = 0 if dff.empty else int(dff["Ingreso_productos_recibido"].sum() + dff["Ingreso_domicilio_recibido"].sum())
    total_expenses = 0 if load_df("Gastos").empty else int(load_df("Gastos")["Monto"].sum())
    balance = total_revenue - total_expenses
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Pedidos totales", f"{total_orders}")
    c2.metric("Clientes", f"{total_clients}")
    c3.metric("Ingresos (registrados)", f"{total_revenue:,} COP")
    c4.metric("Saldo neto", f"{balance:,} COP")
    st.markdown("### Ventas por producto")
    unidades = unidades_vendidas_por_producto(dfdet)
    df_un = pd.DataFrame(list(unidades.items()), columns=["Producto","Unidades"]).sort_values("Unidades",ascending=False)
    if PLOTLY_AVAILABLE and not df_un.empty:
        fig = px.bar(df_un, x="Producto", y="Unidades", title="Unidades vendidas por producto")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(df_un)

# ---------------------------
# CLIENTES
# ---------------------------
elif menu == "Clientes":
    st.header("👥 Clientes")
    dfc = load_df("Clientes")
    st.dataframe(dfc, use_container_width=True)
    with st.form("form_add_cliente"):
        st.subheader("Agregar cliente")
        n = st.text_input("Nombre")
        t = st.text_input("Teléfono")
        d = st.text_input("Dirección")
        if st.form_submit_button("Agregar"):
            if not n:
                st.error("Nombre requerido")
            else:
                cid = add_cliente(n,t,d)
                st.success(f"Cliente {n} agregado (ID {cid})")
                st.experimental_rerun()

# ---------------------------
# PEDIDOS
# ---------------------------
elif menu == "Pedidos":
    st.header("📦 Pedidos - Crear / Editar / Eliminar")
    dfc = load_df("Clientes")
    if dfc.empty:
        st.warning("No hay clientes. Agrega clientes en la sección 'Clientes'.")
    else:
        with st.expander("➕ Registrar nuevo pedido"):
            client_opts = dfc["ID Cliente"].astype(int).astype(str) + " - " + dfc["Nombre"]
            client_opts = client_opts.tolist()
            client_sel = st.selectbox("Cliente", ["Seleccionar..."] + client_opts, key="new_client")
            if client_sel == "Seleccionar...":
                st.info("Selecciona un cliente")
                new_client_id=None
            else:
                try:
                    new_client_id = int(client_sel.split(" - ")[0])
                except:
                    st.error("Formato cliente inválido")
                    new_client_id=None
            # build product lines
            dfi = load_df("Inventario")
            # filter out bad entries like header text 'Producto'
            prod_list = [p for p in dfi["Producto"].astype(str).unique() if str(p).strip().lower()!="producto" and str(p).strip()!=""]
            num_lines = st.number_input("Número de líneas", min_value=1, max_value=12, value=3)
            new_items={}
            cols = st.columns(2)
            for i in range(int(num_lines)):
                with cols[i%2]:
                    pr = st.selectbox(f"Producto {i+1}", ["-- Ninguno --"] + prod_list, key=f"np_{i}")
                    q = st.number_input(f"Cantidad {i+1}", min_value=0, step=1, key=f"nq_{i}")
                if pr and pr!="-- Ninguno --" and q>0:
                    new_items[pr]=new_items.get(pr,0)+int(q)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada de entrega", value=date.today())
            if st.button("Crear pedido"):
                if new_client_id is None:
                    st.error("Selecciona cliente válido")
                elif not new_items:
                    st.warning("No hay líneas definidas")
                else:
                    try:
                        pid = create_order(new_client_id, new_items, domicilio_bool=domicilio, fecha_entrega=fecha_entrega)
                        st.success(f"Pedido creado (ID {pid})")
                    except Exception as e:
                        st.error(f"Error creando pedido: {e}")

    st.markdown("---")
    dfp = load_df("Pedidos")
    if dfp.empty:
        st.info("No hay pedidos registrados.")
    else:
        st.subheader("Listado de pedidos")
        estado_filter = st.selectbox("Filtrar por estado", ["Todos","Pendiente","Entregado"])
        weeks = sorted(dfp["Semana_entrega"].dropna().astype(int).unique().tolist()) if not dfp.empty else []
        week_opts = ["Todas"] + [str(w) for w in weeks]
        week_sel = st.selectbox("Filtrar por semana (ISO)", week_opts)
        df_view = dfp.copy()
        if estado_filter!="Todos":
            df_view = df_view[df_view["Estado"]==estado_filter]
        if week_sel!="Todas":
            df_view = df_view[df_view["Semana_entrega"]==int(week_sel)]
        st.dataframe(df_view.reset_index(drop=True), use_container_width=True)
        if not df_view.empty:
            sel_id = st.selectbox("Seleccionar ID Pedido", df_view["ID Pedido"].astype(int).tolist())
            header = dfp[dfp["ID Pedido"].astype(int)==int(sel_id)].iloc[0].to_dict()
            det = get_order_details(sel_id)
            # visual card
            st.markdown("### Detalle del pedido")
            c1,c2,c3 = st.columns([3,2,2])
            with c1:
                st.markdown(f"**Cliente:** {header.get('Nombre Cliente','')}")
                st.markdown(f"**Fecha:** {header.get('Fecha','')}")
                st.markdown(f"**Semana entrega:** {header.get('Semana_entrega', '')}")
            with c2:
                st.metric("Total pedido", f"{int(header.get('Total_pedido',0)):,} COP")
                st.metric("Subtotal productos", f"{int(header.get('Subtotal_productos',0)):,} COP")
            with c3:
                st.metric("Domicilio", f"{int(header.get('Monto_domicilio',0)):,} COP")
                st.metric("Saldo pendiente", f"{int(header.get('Saldo_pendiente',0)):,} COP")
            st.markdown("#### Líneas")
            if det.empty:
                st.info("No hay líneas de detalle.")
            else:
                st.table(det[["Producto","Cantidad","Precio_unitario","Subtotal"]].set_index(pd.Index(range(1,len(det)+1))))
            # edit section
            st.markdown("#### Editar pedido")
            edited_items = {}
            if not det.empty:
                for i,r in det.iterrows():
                    cols = st.columns([4,2,1])
                    prod = cols[0].selectbox(f"Producto {i+1}", det["Producto"].astype(str).unique(), index=0, key=f"ep_{i}")
                    qty = cols[1].number_input(f"Cantidad {i+1}", min_value=0, value=int(r["Cantidad"]), key=f"eq_{i}")
                    remove = cols[2].checkbox("Eliminar", key=f"er_{i}")
                    if not remove:
                        edited_items[prod] = edited_items.get(prod,0)+int(qty)
            add_lines = st.number_input("Agregar líneas", min_value=0, max_value=8, value=0)
            if add_lines>0:
                for j in range(int(add_lines)):
                    a1,a2 = st.columns([4,2])
                    pnew = a1.selectbox(f"Nuevo producto {j+1}", ["-- Ninguno --"] + prod_list, key=f"np_new_{j}")
                    qnew = a2.number_input(f"Nueva cantidad {j+1}", min_value=0, step=1, key=f"nq_new_{j}")
                    if pnew and pnew!="-- Ninguno --" and qnew>0:
                        edited_items[pnew]=edited_items.get(pnew,0)+int(qnew)
            domic_opt = st.selectbox("Domicilio", ["No", f"Sí ({DOMICILIO_COST} COP)"], index=0 if header.get("Monto_domicilio",0)==0 else 1)
            new_week = st.number_input("Semana entrega (ISO)", min_value=1, max_value=53, value=int(header.get("Semana_entrega", current_week)))
            new_state = st.selectbox("Estado", ["Pendiente","Entregado"], index=0 if header.get("Estado","Pendiente")!="Entregado" else 1)
            if st.button("Guardar cambios en pedido"):
                try:
                    if not edited_items:
                        st.warning("No hay líneas definidas.")
                    else:
                        new_domic = True if "Sí" in domic_opt else False
                        edit_order(sel_id, edited_items, new_domic, new_week)
                        # update state
                        dfh = load_df("Pedidos")
                        idx = dfh.index[dfh["ID Pedido"].astype(int)==int(sel_id)][0]
                        dfh.at[idx,"Estado"]=new_state
                        save_local_csv(CSV_PEDIDOS, dfh, HEAD_PEDIDOS)
                        try: safe_write_sheet(dfh,"Pedidos",HEAD_PEDIDOS)
                        except: pass
                        flush_caches()
                        st.success("Pedido actualizado.")
                except Exception as e:
                    st.error(f"Error: {e}")
            if st.button("Eliminar pedido (revertir inventario)"):
                try:
                    delete_order(sel_id)
                    st.success("Pedido eliminado.")
                except Exception as e:
                    st.error(f"Error eliminando pedido: {e}")

# ---------------------------
# ENTREGAS / PAGOS
# ---------------------------
elif menu == "Entregas/Pagos":
    st.header("🚚 Entregas y Pagos")
    dfp = load_df("Pedidos")
    if dfp.empty:
        st.info("No hay pedidos")
    else:
        estado_choice = st.selectbox("Estado", ["Todos","Pendiente","Entregado"])
        weeks = sorted(dfp["Semana_entrega"].dropna().astype(int).unique().tolist()) if not dfp.empty else []
        week_opts = ["Todas"] + [str(w) for w in weeks]
        week_filter = st.selectbox("Semana (ISO)", week_opts)
        df_view = dfp.copy()
        if estado_choice!="Todos":
            df_view = df_view[df_view["Estado"]==estado_choice]
        if week_filter!="Todas":
            df_view = df_view[df_view["Semana_entrega"]==int(week_filter)]
        st.dataframe(df_view.reset_index(drop=True), use_container_width=True)
        if not df_view.empty:
            ids = df_view["ID Pedido"].astype(int).tolist()
            sel = st.selectbox("Selecciona ID Pedido", ids)
            idx = dfp.index[dfp["ID Pedido"].astype(int)==int(sel)][0]
            row = dfp.loc[idx]
            st.markdown(f"**Cliente:** {row['Nombre Cliente']}  •  **Total:** {int(row['Total_pedido']):,} COP  •  **Pagado:** {int(row['Monto_pagado']):,} COP  •  **Saldo:** {int(row['Saldo_pendiente']):,} COP")
            det = get_order_details(sel)
            if not det.empty:
                st.markdown("**Líneas:**")
                st.table(det[["Producto","Cantidad","Precio_unitario","Subtotal"]].set_index(pd.Index(range(1,len(det)+1))))
            with st.form("form_payment"):
                amount = st.number_input("Monto a pagar (COP)", min_value=0, step=1000, value=int(row.get("Saldo_pendiente",0)))
                medio = st.selectbox("Medio de pago", ["Efectivo","Transferencia","Nequi","Daviplata","Bancolombia"])
                submit = st.form_submit_button("Registrar pago")
                if submit:
                    try:
                        res = register_payment(int(sel), medio, float(amount))
                        st.success(f"Pago registrado. Productos: {res['prod_paid']} COP, Domicilio: {res['domicilio_paid']} COP. Saldo: {res['saldo_total']} COP")
                    except Exception as e:
                        st.error(f"Error registrando pago: {e}")

# ---------------------------
# INVENTARIO
# ---------------------------
elif menu == "Inventario":
    st.header("📦 Inventario")
    dfi = load_df("Inventario")
    if dfi.empty:
        st.info("Inventario vacío. Agrega productos.")
    else:
        dfi["Stock"]=pd.to_numeric(dfi["Stock"], errors='coerce').fillna(0).astype(int)
        st.dataframe(dfi, use_container_width=True)
    st.markdown("### Ajuste manual de stock (permite negativo)")
    dfi_local = load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
    prod_list = sorted([p for p in dfi_local["Producto"].astype(str).unique() if str(p).strip()!=""])
    prod_sel = st.selectbox("Producto", prod_list if prod_list else list(PRODUCTOS.keys()))
    delta = st.number_input("Cantidad a sumar/restar (negativo para restar)", value=0, step=1)
    reason = st.text_input("Motivo (opcional)")
    if st.button("Aplicar ajuste"):
        try:
            if prod_sel in dfi_local["Producto"].values:
                idx = dfi_local.index[dfi_local["Producto"]==prod_sel][0]
                dfi_local.at[idx,"Stock"] = int(dfi_local.at[idx,"Stock"]) + int(delta)
            else:
                dfi_local = pd.concat([dfi_local,pd.DataFrame([[prod_sel,int(delta),PRODUCTOS.get(prod_sel,0)]], columns=HEAD_INVENTARIO)], ignore_index=True)
            dfi_local["Producto"]=dfi_local["Producto"].astype(str)
            dfi_local = dfi_local.groupby("Producto", as_index=False).agg({"Stock":"sum","Precio Unitario":"first"})
            save_local_csv(CSV_INVENTARIO, dfi_local, HEAD_INVENTARIO)
            try: safe_write_sheet(dfi_local,"Inventario",HEAD_INVENTARIO)
            except: pass
            flush_caches()
            st.success("Ajuste aplicado.")
            log_info(f"Stock adjusted {prod_sel} by {delta} reason:{reason}")
        except Exception as e:
            st.error(f"Error ajustando stock: {e}")

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
    st.subheader("Totales por medio de pago")
    by_method = totals_by_payment_method()
    if not by_method:
        st.info("No hay movimientos registrados")
    else:
        dfm = pd.DataFrame(list(by_method.items()), columns=["Medio_pago","Total"]).set_index("Medio_pago")
        st.dataframe(dfm)

    st.markdown("---")
    st.subheader("Registrar movimiento entre medios (ej. retiro: Transferencia -> Efectivo)")
    with st.form("form_move"):
        amt = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        from_m = st.selectbox("De (medio)", ["Transferencia","Efectivo","Nequi","Daviplata","Bancolombia"])
        to_m = st.selectbox("A (medio)", ["Efectivo","Transferencia","Nequi","Daviplata","Bancolombia"])
        note = st.text_input("Nota (opcional)", value="Movimiento interno / Retiro")
        if st.form_submit_button("Registrar movimiento"):
            if amt<=0:
                st.error("Monto debe ser mayor a 0")
            elif from_m==to_m:
                st.error("Los medios deben ser diferentes")
            else:
                try:
                    move_funds(amt, from_m, to_m, note)
                    st.success("Movimiento registrado")
                except Exception as e:
                    st.error(f"Error registrando movimiento: {e}")

    st.markdown("---")
    st.subheader("Registrar gasto")
    with st.form("form_gasto"):
        c = st.text_input("Concepto")
        m = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        if st.form_submit_button("Agregar gasto"):
            try:
                add_expense(c,float(m))
                st.success("Gasto agregado")
            except Exception as e:
                st.error(f"Error agregando gasto: {e}")

    st.markdown("---")
    st.subheader("Movimientos recientes")
    dff = load_df("FlujoCaja")
    if not dff.empty:
        st.dataframe(dff.tail(200), use_container_width=True)
    dg = load_df("Gastos")
    if not dg.empty:
        st.dataframe(dg.tail(200), use_container_width=True)

# ---------------------------
# REPORTES
# ---------------------------
elif menu == "Reportes":
    st.header("📈 Reportes")
    dfp = load_df("Pedidos")
    dfdet = load_df("Pedidos_detalle")
    dff = load_df("FlujoCaja")
    dg = load_df("Gastos")
    dfi = load_df("Inventario")
    st.subheader("Cabecera Pedidos")
    st.dataframe(dfp, use_container_width=True)
    st.subheader("Detalle Pedidos")
    st.dataframe(dfdet, use_container_width=True)
    st.subheader("Flujo completo")
    st.dataframe(dff, use_container_width=True)
    st.subheader("Gastos")
    st.dataframe(dg, use_container_width=True)
    st.subheader("Inventario")
    st.dataframe(dfi, use_container_width=True)
    st.markdown("---")
    st.subheader("Exportar CSV locales")
    for path in [CSV_CLIENTES, CSV_PEDIDOS, CSV_PEDIDOS_DETALLE, CSV_INVENTARIO, CSV_FLUJO, CSV_GASTOS]:
        if path.exists():
            with open(path,"rb") as f:
                st.download_button(f"Descargar {path.name}", f.read(), file_name=path.name, mime="text/csv")

# ---------------------------
# SINCRONIZACIÓN & LOGS
# ---------------------------
elif menu == "Sincronización":
    st.header("🔄 Sincronización y logs")
    st.write(f"gspread disponible: {GS_AVAILABLE}")
    st.write(f"Google Sheets inicializado: {'Sí' if GS_CLIENT else 'No'}")
    st.write(f"Spreadsheet detectado: {'Sí' if GS_SHEET else 'No'}")
    if st.button("Subir todos los CSV locales a Google Sheets (best-effort)"):
        try:
            dfc = load_local_csv(CSV_CLIENTES, HEAD_CLIENTES)
            dfp = load_local_csv(CSV_PEDIDOS, HEAD_PEDIDOS)
            dfd = load_local_csv(CSV_PEDIDOS_DETALLE, HEAD_PEDIDOS_DETALLE)
            dfi = load_local_csv(CSV_INVENTARIO, HEAD_INVENTARIO)
            dff = load_local_csv(CSV_FLUJO, HEAD_FLUJO)
            dfg = load_local_csv(CSV_GASTOS, HEAD_GASTOS)
            ok = safe_write_sheet(dfc,"Clientes",HEAD_CLIENTES)
            ok = ok and safe_write_sheet(dfp,"Pedidos",HEAD_PEDIDOS)
            ok = ok and safe_write_sheet(dfd,"Pedidos_detalle",HEAD_PEDIDOS_DETALLE)
            ok = ok and safe_write_sheet(dfi,"Inventario",HEAD_INVENTARIO)
            ok = ok and safe_write_sheet(dff,"FlujoCaja",HEAD_FLUJO)
            ok = ok and safe_write_sheet(dfg,"Gastos",HEAD_GASTOS)
            if ok:
                st.success("Sincronización completa (best-effort).")
            else:
                st.warning("Sincronización parcial o fallida. Revisa logs.")
        except Exception as e:
            st.error(f"Error sincronizando: {e}")
    st.markdown("---")
    st.subheader("Logs recientes")
    if LOG_FILE.exists():
        with open(LOG_FILE,"r") as lf:
            logs = lf.read().splitlines()[-400:]
            st.text("\n".join(logs))
    else:
        st.info("No hay logs todavía.")

st.markdown("---")
st.caption("AndicBlue — App local con respaldo CSV y sincronización opcional a Google Sheets. Diseñada para operar aún cuando Sheets limite lecturas (quota).")
