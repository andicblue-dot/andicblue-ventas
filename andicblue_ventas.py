# andicblue_app_full.py
# AndicBlue - App completa para Streamlit Cloud
# Requisitos:
#  - st.secrets["gcp_service_account"] con JSON de la cuenta de servicio
#  - Google Sheet name: andicblue_pedidos (compartido con el service account)
#
# Funcionalidades completas:
#  - Clientes, Pedidos (cabecera), Pedidos_detalle (líneas)
#  - Inventario editable (permite stock negativo, evita duplicados)
#  - Entregas / Pagos (pagos parciales, desglose prod vs domicilio)
#  - FlujoCaja, Gastos, movimiento entre medios
#  - CRUD pedidos: crear / editar / eliminar (revertir inventario)
#  - Dashboard: métricas y gráficos (ventas por producto, ventas por día, ingresos por medio, rentabilidad)
#  - Protección contra errores 429 y reintentos con backoff
#  - Caché para minimizar lecturas (st.cache_data)
#  - Guardados seguros con incrementos de token para bust cache
#
# Autor: Generado por ChatGPT (adaptado a requisitos de usuario)
# Fecha: 2025-10-11

import streamlit as st
import pandas as pd
import gspread
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
from typing import Dict, List, Any, Tuple

# -----------------------------------------------------------
# CONFIGURACIÓN GLOBAL
# -----------------------------------------------------------
st.set_page_config(page_title="AndicBlue - Gestión Integral", page_icon="🫐", layout="wide")
st.title("🫐 AndicBlue — Gestión de Pedidos, Inventario y Flujo")

SHEET_NAME = "andicblue_pedidos"
DOMICILIO_COST = 3000  # COP fijo
PRODUCTOS = {
    "Docena de Arándanos 125g": 52500,
    "Arandanos_125g": 5000,
    "Arandanos_250g": 10000,
    "Arandanos_500g": 20000,
    "Kilo_industrial": 30000,
    "Mermelada_azucar": 16000,
    "Mermelada_sin_azucar": 20000,
}

# HEADERS esperados
HEAD_CLIENTES = ["ID Cliente", "Nombre", "Telefono", "Direccion"]
HEAD_PEDIDOS = [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
    "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Estado",
    "Medio_pago", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"
]
HEAD_PEDIDOS_DETALLE = ["ID Pedido", "Producto", "Cantidad", "Precio_unitario", "Subtotal"]
HEAD_INVENTARIO = ["Producto", "Stock"]
HEAD_FLUJO = ["Fecha", "ID Pedido", "Cliente", "Medio_pago", "Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"]
HEAD_GASTOS = ["Fecha", "Concepto", "Monto"]

# Scopes para Google API
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# -----------------------------------------------------------
# AUTENTICACIÓN: st.secrets["gcp_service_account"]
# -----------------------------------------------------------
if "gcp_service_account" not in st.secrets:
    st.error("❌ Falta st.secrets['gcp_service_account'] con JSON de la cuenta de servicio. Añádelo y recarga.")
    st.stop()

try:
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    gc = gspread.authorize(creds)
except Exception as e:
    st.error(f"❌ Error autenticando la cuenta de servicio: {e}")
    st.stop()

# -----------------------------------------------------------
# UTILIDADES: reintentos / backoff / cache bust token
# -----------------------------------------------------------
def exponential_backoff_sleep(attempt: int):
    # sleep bounded to avoid long waits: 1,2,4,8,10...
    time.sleep(min(10, 1 * (2 ** attempt)))

def safe_open_spreadsheet(name: str, retries: int = 4):
    """Abrir el spreadsheet con reintentos en caso de APIError (p.ej. quota exceeded)."""
    last_exc = None
    for attempt in range(retries):
        try:
            ss = gc.open(name)
            return ss
        except gspread.exceptions.APIError as e:
            last_exc = e
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "User Rate Limit" in msg:
                st.warning("⚠️ Límite de lectura de Google Sheets alcanzado. Reintentando...")
                exponential_backoff_sleep(attempt)
                continue
            else:
                st.error(f"APIError abriendo spreadsheet: {e}")
                raise
        except Exception as e:
            last_exc = e
            st.error(f"Error abriendo spreadsheet: {e}")
            exponential_backoff_sleep(attempt)
    st.error("❌ No se pudo abrir el spreadsheet tras varios intentos. Revisa permisos y cuota.")
    if last_exc:
        raise last_exc
    else:
        st.stop()

def safe_get_worksheet(ss, title: str):
    """Obtener worksheet; si no existe lo crea con tamaño base."""
    try:
        ws = ss.worksheet(title)
        return ws
    except Exception:
        try:
            ss.add_worksheet(title=title, rows="1000", cols="20")
            return ss.worksheet(title)
        except Exception as e:
            st.error(f"❌ No se pudo crear/abrir la hoja '{title}': {e}")
            st.stop()

# cache bust token en session_state
if "cache_bust" not in st.session_state:
    st.session_state["cache_bust"] = 0

# -----------------------------------------------------------
# LECTURAS CACHÉADAS (reduce requests y previene 429)
# -----------------------------------------------------------
@st.cache_data(ttl=120, show_spinner=False)
def load_sheet_to_df(sheet_title: str, cache_bust: int = 0) -> pd.DataFrame:
    """
    Carga una hoja en DataFrame con reintentos. cache_bust se usa para forzar recarga tras escrituras.
    """
    ss = safe_open_spreadsheet(SHEET_NAME)
    ws = safe_get_worksheet(ss, sheet_title)
    for attempt in range(4):
        try:
            records = ws.get_all_records()
            df = pd.DataFrame(records)
            if df.empty:
                # retornar DataFrame con columnas esperadas si la hoja está vacía
                if sheet_title == "Clientes":
                    return pd.DataFrame(columns=HEAD_CLIENTES)
                if sheet_title == "Pedidos":
                    return pd.DataFrame(columns=HEAD_PEDIDOS)
                if sheet_title == "Pedidos_detalle":
                    return pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
                if sheet_title == "Inventario":
                    return pd.DataFrame(columns=HEAD_INVENTARIO)
                if sheet_title == "FlujoCaja":
                    return pd.DataFrame(columns=HEAD_FLUJO)
                if sheet_title == "Gastos":
                    return pd.DataFrame(columns=HEAD_GASTOS)
            return df
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "User Rate Limit" in msg:
                exponential_backoff_sleep(attempt)
                continue
            # other API error: return empty DataFrame but warn
            st.warning(f"APIError leyendo hoja '{sheet_title}': {e}")
            return pd.DataFrame()
        except Exception as e:
            st.warning(f"Error leyendo hoja '{sheet_title}': {e}")
            return pd.DataFrame()
    st.warning(f"No se pudo leer la hoja '{sheet_title}' tras varios intentos.")
    return pd.DataFrame()

# -----------------------------------------------------------
# ESCRITURAS SEGURAS
# -----------------------------------------------------------
def save_df_to_worksheet(df: pd.DataFrame, sheet_title: str, headers: List[str], cache_bust_key: str = "cache_bust"):
    """
    Sobrescribe la hoja con df. No lanza excepción al usuario; muestra warning en caso de fallo.
    Incrementa cache_bust token para forzar recarga.
    """
    try:
        ss = safe_open_spreadsheet(SHEET_NAME)
        ws = safe_get_worksheet(ss, sheet_title)
        ws.clear()
        # Guardar encabezados
        ws.append_row(headers)
        # Guardar filas (append row para evitar issues con formatos)
        for _, row in df.iterrows():
            vals = [("" if pd.isna(v) else v) for v in row.tolist()]
            ws.append_row(vals)
        # Bust cache
        st.session_state[cache_bust_key] = st.session_state.get(cache_bust_key, 0) + 1
    except gspread.exceptions.APIError as e:
        st.warning(f"⚠️ No se pudo guardar '{sheet_title}' (APIError): {e}")
    except Exception as e:
        st.warning(f"⚠️ No se pudo guardar '{sheet_title}': {e}")

def safe_append_row(sheet_title: str, row: List[Any], cache_bust_key: str = "cache_bust"):
    """Anexa una fila simple en la hoja."""
    try:
        ss = safe_open_spreadsheet(SHEET_NAME)
        ws = safe_get_worksheet(ss, sheet_title)
        ws.append_row(row)
        st.session_state[cache_bust_key] = st.session_state.get(cache_bust_key, 0) + 1
    except Exception as e:
        st.warning(f"⚠️ No se pudo anexar fila a '{sheet_title}': {e}")

# -----------------------------------------------------------
# NORMALIZACIONES Y HELPERS
# -----------------------------------------------------------
def coerce_numeric(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

def canonical_product_name(name: str) -> str:
    """Mapea nombres arbitrarios a claves canónicas en PRODUCTOS cuando sea posible."""
    if not isinstance(name, str):
        return name
    s = name.strip()
    # Direct match
    if s in PRODUCTOS:
        return s
    # Normalize (lower, remove spaces/underscores/dashes)
    def norm(x): return x.lower().replace(" ", "").replace("_", "").replace("-", "")
    ns = norm(s)
    for k in PRODUCTOS.keys():
        if norm(k) == ns:
            return k
    for k in PRODUCTOS.keys():
        # partial match
        if ns in norm(k) or norm(k) in ns:
            return k
    # fallback: return original trimmed string
    return s

def parse_legacy_product_cell(cell_text: str) -> Dict[str, int]:
    """
    Parsea formatos legacy en una sola celda, ejemplo:
    "Arandanos_250g x11 (@10000) | Mermelada_azucar x2 (@16000)"
    -> {'Arandanos_250g': 11, 'Mermelada_azucar': 2}
    """
    items = {}
    if not cell_text or pd.isna(cell_text):
        return items
    parts = str(cell_text).split(" | ")
    for p in parts:
        try:
            left = p.split(" x")
            name = left[0].strip()
            qty = int(left[1].split(" ")[0])
            items[canonical_product_name(name)] = items.get(canonical_product_name(name), 0) + qty
        except Exception:
            continue
    return items

def build_detalle_rows_from_dict(order_id: int, items: Dict[str,int]) -> List[List[Any]]:
    rows = []
    for prod_raw, qty in items.items():
        prod = canonical_product_name(prod_raw)
        price = PRODUCTOS.get(prod, 0)
        subtotal = int(qty) * int(price)
        rows.append([order_id, prod, int(qty), int(price), int(subtotal)])
    return rows

def next_id(df: pd.DataFrame, col: str) -> int:
    if df is None or df.empty or col not in df.columns:
        return 1
    vals = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
    return max(vals) + 1 if vals else 1

# -----------------------------------------------------------
# CORE BUSINESS: Pedidos, Detalle, Inventario, Pagos, Flujo
# -----------------------------------------------------------
def initialize_sheets_if_missing():
    """
    Asegura que todas las hojas existan en el spreadsheet y con encabezados.
    Se crea si faltan; no modifica si ya existen.
    """
    ss = safe_open_spreadsheet(SHEET_NAME)
    # mapping sheet->headers
    mapping = {
        "Clientes": HEAD_CLIENTES,
        "Pedidos": HEAD_PEDIDOS,
        "Pedidos_detalle": HEAD_PEDIDOS_DETALLE,
        "Inventario": HEAD_INVENTARIO,
        "FlujoCaja": HEAD_FLUJO,
        "Gastos": HEAD_GASTOS
    }
    for title, headers in mapping.items():
        try:
            ws = ss.worksheet(title)
            # check headers
            try:
                current = ws.row_values(1)
            except Exception:
                current = []
            if not current or current[:len(headers)] != headers:
                # try to replace first row with headers
                try:
                    if ws.row_count >= 1 and any(ws.row_values(1)):
                        ws.delete_rows(1)
                except Exception:
                    pass
                try:
                    ws.insert_row(headers, index=1)
                except Exception:
                    try:
                        ws.append_row(headers)
                    except Exception:
                        pass
        except Exception:
            try:
                ss.add_worksheet(title=title, rows="1000", cols="20")
                ws = ss.worksheet(title)
                try:
                    ws.insert_row(headers, index=1)
                except Exception:
                    ws.append_row(headers)
            except Exception as e:
                st.warning(f"No se pudo crear hoja '{title}': {e}")

# Actions to initialize sheets at startup (safe)
try:
    initialize_sheets_if_missing()
except Exception:
    # don't block startup if initialization fails
    pass

# -----------------------------------------------------------
# Create / Edit / Delete Orders with inventory updates (no duplicates)
# -----------------------------------------------------------
def create_order(cliente_id: int, items: Dict[str,int], domicilio: bool=False, fecha_entrega=None) -> int:
    """
    Crea encabezado en Pedidos y filas en Pedidos_detalle. Actualiza Inventario sin duplicados.
    Permite stock negativo.
    """
    # load data
    df_clients = load_sheet_to_df("Clientes", st.session_state["cache_bust"])
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    # validate client
    client_name = ""
    if not df_clients.empty and "ID Cliente" in df_clients.columns:
        try:
            client_name = df_clients.loc[df_clients["ID Cliente"] == cliente_id, "Nombre"].values[0]
        except Exception:
            client_name = ""
    # totals
    subtotal = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in items.items())
    monto_dom = DOMICILIO_COST if domicilio else 0
    total = subtotal + monto_dom
    # fecha
    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = int(fecha_dt.isocalendar().week)
    pid = next_id(df_ped, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header_row = [pid, fecha_actual, cliente_id, client_name, subtotal, monto_dom, total, "Pendiente", "", 0, total, semana_entrega]
    # append header
    safe_append_row("Pedidos", header_row)
    # append detalle rows
    detalle_rows = build_detalle_rows_from_dict(pid, items)
    for r in detalle_rows:
        safe_append_row("Pedidos_detalle", r)
    # update inventory: read full inventory, update canonical names and sums
    if df_inv.empty:
        # initialize default inventory with canonical products
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
    # normalize Producto column
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])
    # apply adjustments
    for prod_raw, qty in items.items():
        prod = canonical_product_name(prod_raw)
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
        else:
            # add new product row with negative stock allowed
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # aggregate duplicates and persist
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")
    return pid

def edit_order(order_id: int, new_items: Dict[str,int], new_domic_bool: bool=None, new_week: int=None):
    """
    Reemplaza detalle del pedido y ajusta inventario (revirtiendo cantidades previas).
    Recalcula totales y persiste.
    """
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    if df_ped.empty:
        raise ValueError("No hay pedidos registrados.")

    if order_id not in df_ped["ID Pedido"].values:
        raise ValueError("Pedido no encontrado.")

    if df_det.empty:
        df_det = pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)

    # normalize
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])

    # compute old counts and revert inventory
    old_lines = df_det[df_det["ID Pedido"] == order_id]
    old_counts: Dict[str,int] = {}
    for _, r in old_lines.iterrows():
        prod = canonical_product_name(r["Producto"])
        qty = int(r["Cantidad"])
        old_counts[prod] = old_counts.get(prod, 0) + qty
    for prod, qty in old_counts.items():
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) + qty
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # remove old detalle rows
    df_det = df_det[df_det["ID Pedido"] != order_id].reset_index(drop=True)

    # add new detalle rows and subtract from inventory
    for prod_raw, qty in new_items.items():
        prod = canonical_product_name(prod_raw)
        price = PRODUCTOS.get(prod, 0)
        new_row = [order_id, prod, int(qty), int(price), int(qty) * int(price)]
        df_det = pd.concat([df_det, pd.DataFrame([new_row], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # update header totals
    subtotal = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in new_items.items())
    idx_header = df_ped.index[df_ped["ID Pedido"] == order_id][0]
    domicilio = float(df_ped.at[idx_header, "Monto_domicilio"]) if new_domic_bool is None else (DOMICILIO_COST if new_domic_bool else 0)
    total = subtotal + domicilio
    monto_pagado = float(df_ped.at[idx_header, "Monto_pagado"])
    saldo = total - monto_pagado
    df_ped.at[idx_header, "Subtotal_productos"] = subtotal
    df_ped.at[idx_header, "Monto_domicilio"] = domicilio
    df_ped.at[idx_header, "Total_pedido"] = total
    df_ped.at[idx_header, "Saldo_pendiente"] = saldo
    if new_week:
        df_ped.at[idx_header, "Semana_entrega"] = int(new_week)

    # normalize and persist inventory and detalle and header
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")

def delete_order(order_id: int):
    """
    Elimina pedido y detalle, revierte inventario.
    """
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    if df_det.empty:
        df_det = pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)

    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])

    detalle = df_det[df_det["ID Pedido"] == order_id]
    for _, r in detalle.iterrows():
        prod = canonical_product_name(r["Producto"])
        qty = int(r["Cantidad"])
        if prod in df_inv["Producto"].values:
            i = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[i, "Stock"] = int(df_inv.at[i, "Stock"]) + qty
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, qty]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # remove rows
    df_det = df_det[df_det["ID Pedido"] != order_id].reset_index(drop=True)
    df_ped = df_ped[df_ped["ID Pedido"] != order_id].reset_index(drop=True)
    # aggregate inv and save
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")

# -----------------------------------------------------------
# PAYMENTS & FLOW
# -----------------------------------------------------------
def register_payment(order_id: int, medio_pago: str, monto: float) -> Dict[str, float]:
    """
    Registra un pago para un pedido, discriminando entre productos y domicilio.
    Calcula cuanto del pago se aplica a productos y cuanto a domicilio (solo DOMICILIO_COST).
    Agrega fila a FlujoCaja con los montos efectivamente recibidos en esta transacción.
    """
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_flu = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    if df_ped.empty:
        raise ValueError("No hay pedidos registrados.")
    if order_id not in df_ped["ID Pedido"].values:
        raise ValueError("Pedido no encontrado.")
    idx = df_ped.index[df_ped["ID Pedido"] == order_id][0]
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

    # append to flujo
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_flow = [fecha, order_id, df_ped.at[idx, "Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]
    if df_flu.empty:
        df_flu = pd.DataFrame([new_flow], columns=HEAD_FLUJO)
    else:
        df_flu = pd.concat([df_flu, pd.DataFrame([new_flow], columns=HEAD_FLUJO)], ignore_index=True)

    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_flu, "FlujoCaja", HEAD_FLUJO, cache_bust_key="cache_bust")
    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def totals_by_payment_method() -> Dict[str,float]:
    df_f = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    if df_f.empty:
        return {}
    coerce_numeric(df_f, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    df_f["total_ingreso"] = df_f["Ingreso_productos_recibido"].fillna(0) + df_f["Ingreso_domicilio_recibido"].fillna(0)
    grouped = df_f.groupby("Medio_pago")["total_ingreso"].sum().to_dict()
    return {str(k): float(v) for k,v in grouped.items() if str(k).strip() != ""}

def add_expense(concepto: str, monto: float):
    df_g = load_sheet_to_df("Gastos", st.session_state["cache_bust"])
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_row = [fecha, concepto, monto]
    if df_g.empty:
        df_g = pd.DataFrame([new_row], columns=HEAD_GASTOS)
    else:
        df_g = pd.concat([df_g, pd.DataFrame([new_row], columns=HEAD_GASTOS)], ignore_index=True)
    save_df_to_worksheet(df_g, "Gastos", HEAD_GASTOS, cache_bust_key="cache_bust")

def move_funds(amount: float, from_method: str, to_method: str, note: str = "Movimiento interno"):
    df_f = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    neg = [fecha, 0, note + f" ({from_method} -> {to_method})", from_method, -float(amount), 0, 0]
    pos = [fecha, 0, note + f" ({from_method} -> {to_method})", to_method, float(amount), 0, 0]
    rows = [neg, pos]
    df_new = pd.DataFrame(rows, columns=HEAD_FLUJO)
    if df_f.empty:
        df_f = df_new
    else:
        df_f = pd.concat([df_f, df_new], ignore_index=True)
    save_df_to_worksheet(df_f, "FlujoCaja", HEAD_FLUJO, cache_bust_key="cache_bust")

def flow_summaries() -> Tuple[float,float,float,float]:
    df_f = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    df_g = load_sheet_to_df("Gastos", st.session_state["cache_bust"])
    coerce_numeric(df_f, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    coerce_numeric(df_g, ["Monto"])
    total_prod = df_f["Ingreso_productos_recibido"].sum() if not df_f.empty else 0
    total_dom = df_f["Ingreso_domicilio_recibido"].sum() if not df_f.empty else 0
    total_gastos = df_g["Monto"].sum() if not df_g.empty else 0
    saldo = total_prod + total_dom - total_gastos
    return total_prod, total_dom, total_gastos, saldo

# -----------------------------------------------------------
# REPORTS HELPERS
# -----------------------------------------------------------
def unidades_vendidas(df_det: pd.DataFrame = None) -> Dict[str,int]:
    if df_det is None or df_det.empty:
        return {k:0 for k in PRODUCTOS.keys()}
    res = {}
    for _, r in df_det.iterrows():
        prod = r.get("Producto")
        qty = int(r.get("Cantidad", 0))
        res[prod] = res.get(prod, 0) + qty
    # ensure all canonical products exist in dict
    for p in PRODUCTOS.keys():
        res.setdefault(p, 0)
    return res

def ventas_por_semana(df_ped: pd.DataFrame) -> pd.DataFrame:
    if df_ped is None or df_ped.empty:
        return pd.DataFrame(columns=["Semana", "Total"])
    coerce_numeric(df_ped, ["Semana_entrega", "Total_pedido"])
    df = df_ped.groupby("Semana_entrega")["Total_pedido"].sum().reset_index().rename(columns={"Semana_entrega":"Semana","Total_pedido":"Total"})
    return df.sort_values("Semana")

# -----------------------------------------------------------
# UI: Sidebar & Menu
# -----------------------------------------------------------
st.sidebar.markdown("### Menú")
menu = st.sidebar.selectbox("", ["Dashboard", "Clientes", "Pedidos", "Entregas/Pagos", "Inventario", "Flujo & Gastos", "Reportes", "Configuración"])

# Quick helper to reload caches manually
if st.sidebar.button("🔁 Forzar recarga de datos (bust cache)"):
    st.session_state["cache_bust"] = st.session_state.get("cache_bust", 0) + 1
    st.experimental_rerun()

# -----------------------------------------------------------
# DASHBOARD
# -----------------------------------------------------------
if menu == "Dashboard":
    st.header("📊 Dashboard — Resumen y métricas")
    # load necessary data (cached)
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_flu = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    df_gas = load_sheet_to_df("Gastos", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    # Basic metrics
    st.subheader("KPI's Rápidos")
    total_orders = 0 if df_ped.empty else len(df_ped)
    total_clients = 0
    df_clients = load_sheet_to_df("Clientes", st.session_state["cache_bust"])
    if not df_clients.empty and "ID Cliente" in df_clients.columns:
        total_clients = df_clients["ID Cliente"].nunique()
    total_revenue = 0
    if not df_flu.empty:
        coerce_numeric(df_flu, ["Ingreso_productos_recibido","Ingreso_domicilio_recibido"])
        total_revenue = df_flu["Ingreso_productos_recibido"].sum() + df_flu["Ingreso_domicilio_recibido"].sum()
    total_gastos = 0 if df_gas.empty else df_gas["Monto"].sum()
    saldo = total_revenue - total_gastos

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Pedidos registrados", f"{int(total_orders):,}")
    c2.metric("Clientes", f"{int(total_clients):,}")
    c3.metric("Ingresos (registrados)", f"{int(total_revenue):,} COP")
    c4.metric("Saldo neto", f"{int(saldo):,} COP")

    # Sales by product (from detalle aggregated)
    st.markdown("---")
    st.subheader("Ventas por producto (desde Pedidos_detalle)")
    if not df_det.empty:
        df_det_local = df_det.copy()
        coerce_numeric(df_det_local, ["Subtotal","Cantidad"])
        ventas_prod = df_det_local.groupby("Producto")["Subtotal"].sum().reset_index().sort_values("Subtotal", ascending=False)
        fig1 = px.bar(ventas_prod, x="Producto", y="Subtotal", title="Ingresos por producto (COP)", labels={"Subtotal":"Ingresos (COP)"})
        st.plotly_chart(fig1, use_container_width=True)
        # top products table
        st.dataframe(ventas_prod.head(20), use_container_width=True)
    else:
        st.info("No hay líneas de detalle registradas.")

    # Sales per week
    st.markdown("---")
    st.subheader("Ventas por semana (Pedidos)")
    if not df_ped.empty:
        df_weeks = ventas_por_semana(df_ped)
        if not df_weeks.empty:
            fig_week = px.line(df_weeks, x="Semana", y="Total", markers=True, title="Ventas por semana (Total COP)")
            st.plotly_chart(fig_week, use_container_width=True)
            st.dataframe(df_weeks, use_container_width=True)
        else:
            st.info("No hay datos de semanas.")
    else:
        st.info("No hay pedidos registrados.")

    # Ingresos por medio de pago
    st.markdown("---")
    st.subheader("Ingresos por medio de pago (Flujo)")
    if not df_flu.empty:
        coerce_numeric(df_flu, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
        df_flu["total"] = df_flu["Ingreso_productos_recibido"].fillna(0) + df_flu["Ingreso_domicilio_recibido"].fillna(0)
        medios = df_flu.groupby("Medio_pago")["total"].sum().reset_index().sort_values("total", ascending=False)
        fig_medios = px.pie(medios, names="Medio_pago", values="total", title="Distribución por medio de pago")
        st.plotly_chart(fig_medios, use_container_width=True)
        st.dataframe(medios, use_container_width=True)
    else:
        st.info("No hay movimientos registrados en Flujo.")

    # Stock overview
    st.markdown("---")
    st.subheader("Inventario: stock actual y alertas")
    if not df_inv.empty:
        df_inv_local = df_inv.copy()
        df_inv_local["Producto"] = df_inv_local["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
        coerce_numeric(df_inv_local, ["Stock"])
        df_inv_local = df_inv_local.groupby("Producto", as_index=False).agg({"Stock":"sum"}).sort_values("Stock")
        fig_inv = px.bar(df_inv_local, x="Producto", y="Stock", title="Stock por producto")
        st.plotly_chart(fig_inv, use_container_width=True)
        st.dataframe(df_inv_local, use_container_width=True)
        neg = df_inv_local[df_inv_local["Stock"] < 0]
        if not neg.empty:
            st.warning("Productos con stock negativo:")
            st.dataframe(neg)
    else:
        st.info("Inventario vacío.")

# -----------------------------------------------------------
# CLIENTES
# -----------------------------------------------------------
elif menu == "Clientes":
    st.header("📋 Clientes")
    df_clients = load_sheet_to_df("Clientes", st.session_state["cache_bust"])
    if df_clients.empty:
        st.info("No hay clientes registrados.")
    else:
        st.dataframe(df_clients, use_container_width=True)
    with st.form("form_add_cliente"):
        st.subheader("Agregar nuevo cliente")
        name = st.text_input("Nombre completo")
        phone = st.text_input("Teléfono")
        addr = st.text_input("Dirección")
        submitted = st.form_submit_button("Agregar cliente")
    if submitted:
        if not name:
            st.error("Nombre requerido")
        else:
            df_clients = load_sheet_to_df("Clientes", st.session_state["cache_bust"])
            cid = next_id(df_clients, "ID Cliente")
            safe_append_row("Clientes", [cid, name, phone, addr], cache_bust_key="cache_bust")
            st.success(f"Cliente agregado con ID {cid}")
            st.session_state["cache_bust"] += 1

# -----------------------------------------------------------
# PEDIDOS (crear / listar / editar / eliminar)
# -----------------------------------------------------------
elif menu == "Pedidos":
    st.header("📦 Pedidos — Crear / Editar / Eliminar")
    df_clients = load_sheet_to_df("Clientes", st.session_state["cache_bust"])
    if df_clients.empty:
        st.warning("No hay clientes registrados. Agrega clientes primero.")
    else:
        with st.expander("➕ Registrar nuevo pedido"):
            client_select = st.selectbox("Cliente", df_clients["ID Cliente"].astype(str) + " - " + df_clients["Nombre"])
            cliente_id = int(client_select.split(" - ")[0])
            # dynamic number of lines
            lines = st.number_input("Número de líneas de producto", min_value=1, max_value=12, value=3)
            new_items: Dict[str,int] = {}
            cols = st.columns(2)
            for i in range(int(lines)):
                with cols[i % 2]:
                    p = st.selectbox(f"Producto {i+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"newp_{i}")
                    q = st.number_input(f"Cantidad {i+1}", min_value=0, step=1, key=f"newq_{i}")
                if p and p != "-- Ninguno --" and q > 0:
                    new_items[p] = new_items.get(p, 0) + int(q)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada de entrega", value=datetime.now())
            if st.button("Crear pedido"):
                if not new_items:
                    st.warning("No hay líneas definidas")
                else:
                    try:
                        pid = create_order(cliente_id, new_items, domicilio, fecha_entrega)
                        st.success(f"Pedido creado con ID {pid}")
                    except Exception as e:
                        st.error(f"Error creando pedido: {e}")

    st.markdown("---")
    st.subheader("Buscar / Filtrar pedidos")
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    if df_ped.empty:
        st.info("No hay pedidos")
    else:
        coerce_numeric(df_ped, ["Semana_entrega", "ID Pedido"])
        week_values = sorted(df_ped["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_ped.empty else []
        week_opts = ["Todas"] + [str(w) for w in week_values]
        week_sel = st.selectbox("Filtrar por semana (ISO)", week_opts, index=0)
        estado_filter = st.selectbox("Filtrar por estado", ["Todos","Pendiente","Entregado"], index=0)
        df_view = df_ped.copy()
        if estado_filter != "Todos":
            df_view = df_view[df_view["Estado"] == estado_filter]
        if week_sel != "Todas":
            df_view = df_view[df_view["Semana_entrega"] == int(week_sel)]
        st.dataframe(df_view.reset_index(drop=True), use_container_width=True)

        if not df_view.empty:
            sel = st.selectbox("Selecciona ID Pedido para ver / editar", df_view["ID Pedido"].astype(int).tolist())
            header_idx = df_ped.index[df_ped["ID Pedido"] == sel][0]
            header = df_ped.loc[header_idx].to_dict()
            detalle_df = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
            detalle_df = detalle_df[detalle_df["ID Pedido"] == sel].reset_index(drop=True)
            # Card
            st.markdown("### Detalle del pedido")
            cc1, cc2, cc3 = st.columns([2,1,1])
            with cc1:
                st.markdown(f"**Cliente:** {header.get('Nombre Cliente','')}")
                st.markdown(f"**Fecha:** {header.get('Fecha','')}")
                st.markdown(f"**Semana (ISO):** {int(header.get('Semana_entrega', datetime.now().isocalendar().week))}")
            with cc2:
                st.markdown(f"**Subtotal productos:** {int(header.get('Subtotal_productos',0)):,} COP")
                st.markdown(f"**Total:** {int(header.get('Total_pedido',0)):,} COP")
            with cc3:
                st.markdown(f"**Domicilio:** {int(header.get('Monto_domicilio',0)):,} COP")
                st.markdown(f"**Saldo pendiente:** {int(header.get('Saldo_pendiente',0)):,} COP")

            st.markdown("---")
            st.markdown("#### Productos (editar línea por línea)")
            edited_items: Dict[str,int] = {}
            if detalle_df.empty:
                st.info("No hay líneas de detalle para este pedido.")
            else:
                for i, row in detalle_df.iterrows():
                    rcols = st.columns([4,2,2,1])
                    prod = rcols[0].selectbox(f"Producto {i+1}", list(PRODUCTOS.keys()), index=list(PRODUCTOS.keys()).index(row["Producto"]) if row["Producto"] in PRODUCTOS else 0, key=f"edit_prod_{i}")
                    qty = rcols[1].number_input(f"Cantidad {i+1}", min_value=0, step=1, value=int(row["Cantidad"]), key=f"edit_qty_{i}")
                    rcols[2].markdown(f"Unit: {int(row['Precio_unitario']):,}".replace(",","."))
                    remove = rcols[3].checkbox("Eliminar", key=f"remove_{i}")
                    if not remove:
                        edited_items[prod] = edited_items.get(prod, 0) + int(qty)

            st.markdown("Añadir nuevas líneas")
            add_lines = st.number_input("Agregar N líneas", min_value=0, max_value=8, value=0)
            if int(add_lines) > 0:
                for j in range(int(add_lines)):
                    a1,a2 = st.columns([4,2])
                    p_new = a1.selectbox(f"Nuevo producto {j+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"addprod_{j}")
                    q_new = a2.number_input(f"Nueva cantidad {j+1}", min_value=0, step=1, key=f"addqty_{j}")
                    if p_new and p_new != "-- Ninguno --" and q_new > 0:
                        edited_items[p_new] = edited_items.get(p_new, 0) + int(q_new)

            domic_opt = st.selectbox("Domicilio", ["No", f"Sí ({DOMICILIO_COST} COP)"], index=0 if header.get("Monto_domicilio",0) == 0 else 1)
            try:
                week_val = int(header.get("Semana_entrega", datetime.now().isocalendar().week))
                if week_val < 1 or week_val > 53:
                    week_val = datetime.now().isocalendar().week
            except Exception:
                week_val = datetime.now().isocalendar().week
            new_week = st.number_input("Semana entrega (ISO)", min_value=1, max_value=53, value=week_val)
            new_state = st.selectbox("Estado", ["Pendiente","Entregado"], index=0 if header.get("Estado","Pendiente")!="Entregado" else 1)
            if st.button("Guardar cambios en pedido"):
                try:
                    if not edited_items:
                        st.warning("No hay líneas definidas; si quieres borrar el pedido usa Eliminar.")
                    else:
                        new_domic = True if "Sí" in domic_opt else False
                        edit_order(sel, edited_items, new_domic_bool=new_domic, new_week=new_week)
                        # update header state if changed
                        dfp = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
                        idxh = dfp.index[dfp["ID Pedido"] == sel][0]
                        dfp.at[idxh, "Estado"] = new_state
                        save_df_to_worksheet(dfp, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
                        st.success("Pedido actualizado correctamente.")
                except Exception as e:
                    st.error(f"Error actualizando pedido: {e}")

            if st.button("Eliminar pedido (revertir inventario)"):
                try:
                    delete_order(sel)
                    st.success("Pedido eliminado y stock revertido.")
                except Exception as e:
                    st.error(f"Error eliminando pedido: {e}")

# -----------------------------------------------------------
# ENTREGAS / PAGOS
# -----------------------------------------------------------
elif menu == "Entregas/Pagos":
    st.header("🚚 Entregas y Pagos")
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
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
            idx = df_ped.index[df_ped["ID Pedido"] == selection][0]
            row = df_ped.loc[idx]
            st.markdown(f"**Cliente:** {row['Nombre Cliente']}")
            st.markdown(f"**Total:** {int(row['Total_pedido']):,} COP  •  **Pagado:** {int(row['Monto_pagado']):,} COP  •  **Saldo:** {int(row['Saldo_pendiente']):,} COP")
            detalle = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
            detalle_sel = detalle[detalle["ID Pedido"] == selection] if not detalle.empty else pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
            if not detalle_sel.empty:
                st.table(detalle_sel[["Producto","Cantidad","Precio_unitario","Subtotal"]].set_index(pd.Index(range(1,len(detalle_sel)+1))))
            with st.form("form_payment"):
                amount = st.number_input("Monto a pagar (COP)", min_value=0, step=1000, value=int(row.get("Saldo_pendiente",0)))
                medio = st.selectbox("Medio de pago", ["Efectivo","Transferencia","Nequi","Daviplata"])
                submit = st.form_submit_button("Registrar pago")
                if submit:
                    try:
                        res = register_payment(int(selection), medio, amount)
                        st.success(f"Pago registrado: productos {res['prod_paid']} COP, domicilio {res['domicilio_paid']} COP. Saldo restante: {res['saldo_total']} COP")
                    except Exception as e:
                        st.error(f"Error registrando pago: {e}")

# -----------------------------------------------------------
# INVENTARIO (mejorado: editar sumar/restar y CSV)
# -----------------------------------------------------------
elif menu == "Inventario":
    st.header("📦 Inventario — ver / editar")
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])
    if df_inv.empty:
        st.info("Inventario vacío. Puedes agregar productos desde 'Pedidos' (se inicializa) o desde 'Configuración'.")
        df_inv = pd.DataFrame(columns=HEAD_INVENTARIO)

    # Normalize names and aggregate
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"}).sort_values("Producto").reset_index(drop=True)

    st.subheader("Stock actual")
    st.dataframe(df_inv, use_container_width=True)

    st.markdown("---")
    st.subheader("🔧 Ajuste manual de stock (puede ser negativo)")

    colA, colB, colC = st.columns([3,2,2])
    with colA:
        prod_sel = st.selectbox("Producto a ajustar", df_inv["Producto"].tolist() if not df_inv.empty else list(PRODUCTOS.keys()))
    with colB:
        delta = st.number_input("Cantidad a agregar / restar (ej: -3 o 5)", value=0, step=1)
    with colC:
        reason = st.text_input("Motivo (opcional)")

    if st.button("Aplicar ajuste"):
        try:
            # fetch latest inventory, apply change, persist (no dedup)
            df_inv_latest = load_sheet_to_df("Inventario", st.session_state["cache_bust"])
            if df_inv_latest.empty:
                # create canonical list
                df_inv_latest = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
            df_inv_latest["Producto"] = df_inv_latest["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
            coerce_numeric(df_inv_latest, ["Stock"])
            # update aggregate: find product
            if prod_sel in df_inv_latest["Producto"].values:
                idxp = df_inv_latest.index[df_inv_latest["Producto"] == prod_sel][0]
                df_inv_latest.at[idxp, "Stock"] = int(df_inv_latest.at[idxp, "Stock"]) + int(delta)
            else:
                df_inv_latest = pd.concat([df_inv_latest, pd.DataFrame([[prod_sel, int(delta)]], columns=HEAD_INVENTARIO)], ignore_index=True)
            # aggregate and save
            df_inv_latest["Producto"] = df_inv_latest["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
            df_inv_latest = df_inv_latest.groupby("Producto", as_index=False).agg({"Stock":"sum"})
            save_df_to_worksheet(df_inv_latest, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")
            st.success(f"Stock actualizado: {prod_sel} -> {int(df_inv_latest[df_inv_latest['Producto']==prod_sel]['Stock'].values[0])} unidades")
        except Exception as e:
            st.error(f"Error aplicando ajuste: {e}")

    st.markdown("---")
    st.subheader("Exportar inventario")
    csv = df_inv.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Descargar inventario (CSV)", csv, f"inventario_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

# -----------------------------------------------------------
# FLUJO & GASTOS
# -----------------------------------------------------------
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
        df_methods = pd.DataFrame(list(by_method.items()), columns=["Medio_pago","Total_ingresos"]).set_index("Medio_pago")
        st.dataframe(df_methods)

    st.markdown("---")
    st.subheader("Registrar movimiento entre medios (ej. retiro)")
    with st.form("form_move"):
        amt = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        from_m = st.selectbox("De (medio)", ["Transferencia","Efectivo","Nequi","Daviplata"])
        to_m = st.selectbox("A (medio)", ["Efectivo","Transferencia","Nequi","Daviplata"])
        note = st.text_input("Nota", value="Movimiento interno / Retiro")
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
    st.subheader("Registrar gasto")
    with st.form("form_gasto"):
        concept = st.text_input("Concepto")
        m = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        submit_g = st.form_submit_button("Agregar gasto")
        if submit_g:
            try:
                add_expense(concept, m)
                st.success("Gasto agregado")
            except Exception as e:
                st.error(f"Error agregando gasto: {e}")

    st.markdown("---")
    st.subheader("Movimientos recientes")
    df_flu = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    if not df_flu.empty:
        st.dataframe(df_flu.tail(200), use_container_width=True)
    df_g = load_sheet_to_df("Gastos", st.session_state["cache_bust"])
    if not df_g.empty:
        st.dataframe(df_g.tail(200), use_container_width=True)

# -----------------------------------------------------------
# REPORTES (completo)
# -----------------------------------------------------------
elif menu == "Reportes":
    st.header("📈 Reportes completos")
    df_p = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_f = load_sheet_to_df("FlujoCaja", st.session_state["cache_bust"])
    df_g = load_sheet_to_df("Gastos", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    st.subheader("Pedidos (cabecera)")
    st.dataframe(df_p, use_container_width=True)
    st.subheader("Detalle Pedidos")
    st.dataframe(df_det, use_container_width=True)
    st.subheader("Flujo Caja")
    st.dataframe(df_f, use_container_width=True)
    st.subheader("Gastos")
    st.dataframe(df_g, use_container_width=True)
    st.subheader("Inventario")
    if not df_inv.empty:
        df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
        coerce_numeric(df_inv, ["Stock"])
        df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
        st.dataframe(df_inv, use_container_width=True)
    else:
        st.info("Inventario vacío.")

# -----------------------------------------------------------
# CONFIGURACIÓN: opciones administrativas
# -----------------------------------------------------------
elif menu == "Configuración":
    st.header("⚙️ Configuración y utilidades")
    st.markdown("**Acciones administrativas**")
    if st.button("Inicializar hojas (crear si faltan)"):
        try:
            initialize_sheets_if_missing()
            st.success("Hojas inicializadas / verificadas.")
        except Exception as e:
            st.error(f"Error inicializando hojas: {e}")

    st.markdown("---")
    if st.button("Recalcular inventario agregando productos faltantes"):
        try:
            # ensure inventory contains canonical product rows
            df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])
            if df_inv.empty:
                df_inv = pd.DataFrame(columns=HEAD_INVENTARIO)
            existing = set(df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x)).tolist())
            to_add = [p for p in PRODUCTOS.keys() if p not in existing]
            for p in to_add:
                df_inv = pd.concat([df_inv, pd.DataFrame([[p, 0]], columns=HEAD_INVENTARIO)], ignore_index=True)
            df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
            df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
            save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")
            st.success("Inventario normalizado.")
        except Exception as e:
            st.error(f"Error normalizando inventario: {e}")

    st.markdown("---")
    st.write("Token cache_bust:", st.session_state.get("cache_bust", 0))
    st.write("Recuerda compartir el Sheet 'andicblue_pedidos' con la cuenta de servicio (client_email) como Editor.")

# End of file
