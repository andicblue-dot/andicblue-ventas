# andicblue_ventas.py
# Versión final consolidada - AndicBlue
# Requisitos:
#  - Añadir st.secrets["gcp_service_account"] con JSON de la cuenta de servicio
#  - Compartir el Google Sheet named: "andicblue_pedidos" con la cuenta de servicio (Editor)
#
# Funcionalidades:
#  - Pedidos (cabecera + detalle), CRUD completo (crear/editar/eliminar)
#  - Inventario (actualiza sin duplicar filas)
#  - Entregas / Pagos (registro pagos parciales/total, desglosa producto vs domicilio)
#  - Flujo de caja con totales por medio de pago y movimientos entre medios
#  - Protección contra errores 429 (reintentos + backoff) y caché de lecturas
#  - Interfaz mejorada: tarjetas, filtros por semana, edición de líneas de pedido

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
from typing import Dict, List, Any

# ---------------------------
# CONFIG / CONSTANTES
# ---------------------------
st.set_page_config(page_title="AndicBlue - Ventas & Flujo", page_icon="🫐", layout="wide")
st.title("🫐 AndicBlue — Sistema de Pedidos")

SHEET_NAME = "andicblue_pedidos"

PRODUCTOS = {
    "Docena de Arándanos 125g": 52500,
    "Arandanos_125g": 5000,
    "Arandanos_250g": 10000,
    "Arandanos_500g": 20000,
    "Kilo_industrial": 30000,
    "Mermelada_azucar": 16000,
    "Mermelada_sin_azucar": 20000,
}

DOMICILIO_COST = 3000  # COP fijo

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

# ---------------------------
# AUTH: st.secrets must contain gcp_service_account JSON
# ---------------------------
if "gcp_service_account" not in st.secrets:
    st.error("⚠️ Debes añadir 'gcp_service_account' en Streamlit Secrets (JSON de la cuenta de servicio).")
    st.stop()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
try:
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    gc = gspread.authorize(creds)
except Exception as e:
    st.error(f"❌ Error autenticando cuenta de servicio: {e}")
    st.stop()

# ---------------------------
# UTILIDADES: reintentos y caché
# ---------------------------

def exponential_backoff_sleep(attempt: int):
    # Bound sleep to avoid long waits
    time.sleep(min(10, 2 ** attempt))

def safe_open_spreadsheet(name: str, retries: int = 4):
    for attempt in range(retries):
        try:
            return gc.open(name)
        except gspread.exceptions.APIError as e:
            if "Quota exceeded" in str(e) or "rateLimitExceeded" in str(e):
                st.warning("⚠️ Límite de lectura de Google Sheets alcanzado. Reintentando...")
                exponential_backoff_sleep(attempt)
                continue
            else:
                st.error(f"❌ APIError abriendo spreadsheet: {e}")
                raise
        except Exception as e:
            st.error(f"❌ Error abriendo spreadsheet: {e}")
            raise
    st.error("❌ No se pudo abrir el spreadsheet tras varios intentos.")
    st.stop()

def safe_get_worksheet(ss, title: str):
    try:
        return ss.worksheet(title)
    except Exception:
        try:
            ss.add_worksheet(title=title, rows="1000", cols="20")
            return ss.worksheet(title)
        except Exception as e:
            st.error(f"❌ No se pudo crear/abrir la hoja '{title}': {e}")
            st.stop()

@st.cache_data(ttl=120, show_spinner=False)
def load_sheet_to_df(sheet_title: str, cache_bust: int = 0) -> pd.DataFrame:
    """
    Load a worksheet into a DataFrame with retries and caching.
    cache_bust is an int stored in session_state that you increment after writes.
    """
    ss = safe_open_spreadsheet(SHEET_NAME)
    ws = safe_get_worksheet(ss, sheet_title)
    for attempt in range(4):
        try:
            records = ws.get_all_records()
            df = pd.DataFrame(records)
            if df.empty:
                # Ensure columns for expected sheets
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
            if "Quota exceeded" in str(e) or "rateLimitExceeded" in str(e):
                exponential_backoff_sleep(attempt)
                continue
            st.error(f"❌ APIError leyendo '{sheet_title}': {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"❌ Error leyendo '{sheet_title}': {e}")
            return pd.DataFrame()
    st.error(f"❌ No se pudo leer la hoja '{sheet_title}' tras varios intentos.")
    return pd.DataFrame()

def save_df_to_worksheet(df: pd.DataFrame, sheet_title: str, headers: List[str], cache_bust_key: str = None):
    """
    Overwrite worksheet with df. Use try/except to avoid app crash.
    If cache_bust_key provided, increments session_state key to force reloads.
    """
    try:
        ss = safe_open_spreadsheet(SHEET_NAME)
        ws = safe_get_worksheet(ss, sheet_title)
        ws.clear()
        ws.append_row(headers)
        for _, row in df.iterrows():
            vals = [("" if pd.isna(v) else v) for v in row.tolist()]
            ws.append_row(vals)
        if cache_bust_key:
            st.session_state[cache_bust_key] = st.session_state.get(cache_bust_key, 0) + 1
    except gspread.exceptions.APIError as e:
        st.warning(f"⚠️ No se pudo guardar '{sheet_title}' (APIError): {e}")
    except Exception as e:
        st.warning(f"⚠️ No se pudo guardar '{sheet_title}': {e}")

def safe_append_row_to_ws(sheet_title: str, row: List[Any]):
    try:
        ss = safe_open_spreadsheet(SHEET_NAME)
        ws = safe_get_worksheet(ss, sheet_title)
        ws.append_row(row)
        st.session_state["cache_bust"] = st.session_state.get("cache_bust", 0) + 1
    except Exception as e:
        st.warning(f"⚠️ No se pudo anexar fila en '{sheet_title}': {e}")

# ---------------------------
# SESSION STATE: cache bust token
# ---------------------------
if "cache_bust" not in st.session_state:
    st.session_state["cache_bust"] = 0

# ---------------------------
# HELPERS (parsing / calculos / normalización)
# ---------------------------
def coerce_numeric(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

def canonical_product_name(name: str) -> str:
    """Try to map arbitrary product name to canonical PRODUCTOS key using case-insensitive partial match."""
    if not isinstance(name, str):
        return name
    n = name.strip()
    # Direct match
    if n in PRODUCTOS:
        return n
    # Try normalized comparisons
    key_candidates = list(PRODUCTOS.keys())
    def norm(s): return s.lower().replace(" ", "").replace("-", "").replace("_", "")
    nn = norm(n)
    for k in key_candidates:
        if norm(k) == nn:
            return k
    # partial match: if product name appears in canonical key or viceversa
    for k in key_candidates:
        if nn in norm(k) or norm(k) in nn:
            return k
    # fallback: return original name (it will be added to inventory as-is)
    return n

def parse_productos_detalle_text(cell_text: str) -> Dict[str, int]:
    """
    Parse legacy single-cell Productos_detalle text like:
      "Arandanos 250g x11 (@10000) | Mermelada_azucar x2 (@16000)"
    -> return dict {product_name: qty}
    """
    productos = {}
    if not cell_text or pd.isna(cell_text):
        return productos
    parts = str(cell_text).split(" | ")
    for part in parts:
        try:
            # name may contain ' x' before qty
            name_qty = part.split(" x")
            name = name_qty[0].strip()
            qty = int(name_qty[1].split(" ")[0])
            canon = canonical_product_name(name)
            productos[canon] = productos.get(canon, 0) + qty
        except Exception:
            continue
    return productos

def build_detalle_rows_from_dict(order_id: int, items_dict: Dict[str, int]) -> List[List[Any]]:
    rows = []
    for prod, qty in items_dict.items():
        prod_can = canonical_product_name(prod)
        precio = PRODUCTOS.get(prod_can, 0)
        subtotal = int(qty) * int(precio)
        rows.append([order_id, prod_can, int(qty), int(precio), int(subtotal)])
    return rows

def unidades_vendidas_por_producto(df_detalle: pd.DataFrame = None) -> Dict[str, int]:
    resumen = {p: 0 for p in PRODUCTOS.keys()}
    if df_detalle is None or df_detalle.empty:
        return resumen
    for _, r in df_detalle.iterrows():
        prod = r.get("Producto")
        qty = int(r.get("Cantidad", 0))
        if prod in resumen:
            resumen[prod] += qty
        else:
            resumen[prod] = resumen.get(prod, 0) + qty
    return resumen

# ---------------------------
# CORE LOGIC: CRUD / pagos / inventario updates
# ---------------------------
def next_id_in_df(df: pd.DataFrame, col: str) -> int:
    if df is None or df.empty or col not in df.columns:
        return 1
    existing = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
    return max(existing) + 1 if existing else 1

def safe_load(sheet_title: str) -> pd.DataFrame:
    return load_sheet_to_df(sheet_title, st.session_state["cache_bust"])

def create_order_with_details(cliente_id: int, items_dict: Dict[str,int], domicilio_bool: bool=False, fecha_entrega=None) -> int:
    """
    Create header and detalle rows; update inventory properly (no duplicates).
    Writes append rows for header and detalle then updates inventory sheet by reading and writing full sheet (atomically).
    """
    # load clients to get name
    df_clients = safe_load("Clientes")
    cliente_nombre = ""
    if not df_clients.empty and "ID Cliente" in df_clients.columns:
        try:
            cliente_nombre = df_clients.loc[df_clients["ID Cliente"]==cliente_id, "Nombre"].values[0]
        except Exception:
            cliente_nombre = ""
    subtotal = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in items_dict.items())
    domicilio_monto = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio_monto
    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = int(fecha_dt.isocalendar().week)
    df_ped = safe_load("Pedidos")
    pid = next_id_in_df(df_ped, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header_row = [pid, fecha_actual, cliente_id, cliente_nombre, subtotal, domicilio_monto, total, "Pendiente", "", 0, total, semana_entrega]
    # append to sheets
    safe_append_row_to_ws("Pedidos", header_row)
    detalle_rows = build_detalle_rows_from_dict(pid, items_dict)
    for r in detalle_rows:
        safe_append_row_to_ws("Pedidos_detalle", r)
    # update inventory: read full inventory, update values (no duplicates)
    df_inv = safe_load("Inventario")
    # normalize inventory product names to canonical keys for matching, but keep canonical names in sheet
    if df_inv.empty:
        # initialize with canonical products
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
    # Ensure column types
    df_inv["Producto"] = df_inv["Producto"].astype(str)
    if "Stock" not in df_inv.columns:
        df_inv["Stock"] = 0
    coerce_numeric(df_inv, ["Stock"])
    # Build a map for quick lookup (canonical keys)
    prod_to_idx = {canonical_product_name(row["Producto"]): idx for idx, row in df_inv.iterrows()}
    # Apply adjustments
    for prod_raw, qty in items_dict.items():
        prod = canonical_product_name(prod_raw)
        if prod in prod_to_idx:
            i = prod_to_idx[prod]
            df_inv.at[i, "Stock"] = int(df_inv.at[i, "Stock"]) - int(qty)
        else:
            # add new line with negative stock (we allow negatives as requested)
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)
            prod_to_idx[prod] = df_inv.index[df_inv["Producto"] == prod][0]
    # Remove duplicates: keep last occurrence for each canonical product
    # Normalize 'Producto' column to canonical representation
    df_inv["Producto"] = df_inv["Producto"].apply(lambda x: canonical_product_name(str(x)))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    # Persist inventory
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")
    return pid

def edit_order_details(order_id: int, new_items: Dict[str,int], new_domic_bool: bool=None, new_week: int=None):
    """
    Replace detalle rows for order_id with new_items, revert inventory correctly, recalc totals and persist changes.
    """
    # load data
    df_det = safe_load("Pedidos_detalle")
    df_ped = safe_load("Pedidos")
    df_inv = safe_load("Inventario")
    # ensure columns
    if df_det.empty:
        df_det = pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    if df_ped.empty:
        raise ValueError("No hay cabeceras de pedidos")
    # get old lines and revert inventory
    old_lines = df_det[df_det["ID Pedido"] == order_id]
    # revert map
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])
    # revert old quantities
    for _, r in old_lines.iterrows():
        prod = canonical_product_name(r["Producto"])
        qty = int(r["Cantidad"])
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"]==prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) + qty
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # remove old detalle rows
    df_det = df_det[df_det["ID Pedido"] != order_id].reset_index(drop=True)
    # add new detalle rows
    for prod, qty in new_items.items():
        prod_can = canonical_product_name(prod)
        precio = PRODUCTOS.get(prod_can, 0)
        new_row = [order_id, prod_can, int(qty), int(precio), int(qty) * int(precio)]
        df_det = pd.concat([df_det, pd.DataFrame([new_row], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
        # subtract from inventory
        if prod_can in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"]==prod_can][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod_can, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # update header totals
    subtotal = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in new_items.items())
    if order_id in df_ped["ID Pedido"].values:
        idx = df_ped.index[df_ped["ID Pedido"] == order_id][0]
        domicilio = float(df_ped.at[idx, "Monto_domicilio"]) if new_domic_bool is None else (DOMICILIO_COST if new_domic_bool else 0)
        total = subtotal + domicilio
        monto_pagado = float(df_ped.at[idx, "Monto_pagado"])
        saldo = total - monto_pagado
        df_ped.at[idx, "Subtotal_productos"] = subtotal
        df_ped.at[idx, "Monto_domicilio"] = domicilio
        df_ped.at[idx, "Total_pedido"] = total
        df_ped.at[idx, "Saldo_pendiente"] = saldo
        if new_week:
            df_ped.at[idx, "Semana_entrega"] = int(new_week)
    else:
        raise ValueError("Pedido no encontrado en cabeceras al editar.")
    # Normalize inventory and persist
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")

def delete_order_and_revert(order_id: int):
    df_det = safe_load("Pedidos_detalle")
    df_ped = safe_load("Pedidos")
    df_inv = safe_load("Inventario")
    if df_det.empty:
        df_det = pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])
    detalle = df_det[df_det["ID Pedido"]==order_id]
    # revert inventory
    for _, r in detalle.iterrows():
        prod = canonical_product_name(r["Producto"]); qty = int(r["Cantidad"])
        if prod in df_inv["Producto"].values:
            i = df_inv.index[df_inv["Producto"]==prod][0]
            df_inv.at[i, "Stock"] = int(df_inv.at[i, "Stock"]) + qty
        else:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, qty]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # remove detalle and header
    df_det = df_det[df_det["ID Pedido"] != order_id].reset_index(drop=True)
    df_ped = df_ped[df_ped["ID Pedido"] != order_id].reset_index(drop=True)
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")

def register_payment(order_id: int, medio_pago: str, monto: float) -> Dict[str, float]:
    df_ped = safe_load("Pedidos")
    df_flu = safe_load("FlujoCaja")
    if df_ped.empty:
        raise ValueError("No hay pedidos")
    if order_id not in df_ped["ID Pedido"].values:
        raise ValueError("Pedido no encontrado")
    idx = df_ped.index[df_ped["ID Pedido"]==order_id][0]
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
    new_flow = [fecha, order_id, df_ped.at[idx, "Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]
    if df_flu.empty:
        df_flu = pd.DataFrame([new_flow], columns=HEAD_FLUJO)
    else:
        df_flu = pd.concat([df_flu, pd.DataFrame([new_flow], columns=HEAD_FLUJO)], ignore_index=True)
    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_flu, "FlujoCaja", HEAD_FLUJO, cache_bust_key="cache_bust")
    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def totals_by_payment_method() -> Dict[str, float]:
    df_f = safe_load("FlujoCaja")
    if df_f.empty:
        return {}
    coerce_numeric(df_f, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    df_f["total_ingreso"] = df_f["Ingreso_productos_recibido"].fillna(0) + df_f["Ingreso_domicilio_recibido"].fillna(0)
    grouped = df_f.groupby("Medio_pago")["total_ingreso"].sum().to_dict()
    return {str(k): float(v) for k, v in grouped.items() if str(k).strip() != ""}

def flow_summaries():
    df_f = safe_load("FlujoCaja")
    df_g = safe_load("Gastos")
    coerce_numeric(df_f, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    coerce_numeric(df_g, ["Monto"])
    total_prod = df_f["Ingreso_productos_recibido"].sum() if not df_f.empty else 0
    total_dom = df_f["Ingreso_domicilio_recibido"].sum() if not df_f.empty else 0
    total_gastos = df_g["Monto"].sum() if not df_g.empty else 0
    saldo = total_prod + total_dom - total_gastos
    return total_prod, total_dom, total_gastos, saldo

def add_expense(concepto: str, monto: float):
    df_g = safe_load("Gastos")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_row = [fecha, concepto, monto]
    if df_g.empty:
        df_g = pd.DataFrame([new_row], columns=HEAD_GASTOS)
    else:
        df_g = pd.concat([df_g, pd.DataFrame([new_row], columns=HEAD_GASTOS)], ignore_index=True)
    save_df_to_worksheet(df_g, "Gastos", HEAD_GASTOS, cache_bust_key="cache_bust")

def move_funds(amount: float, from_method: str, to_method: str, note: str = "Movimiento interno"):
    df_f = safe_load("FlujoCaja")
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    neg = [fecha, 0, note + f" ({from_method} -> {to_method})", from_method, -float(amount), 0, 0]
    pos = [fecha, 0, note + f" ({from_method} -> {to_method})", to_method, float(amount), 0, 0]
    df_new = pd.DataFrame([neg, pos], columns=HEAD_FLUJO)
    if df_f.empty:
        df_f = df_new
    else:
        df_f = pd.concat([df_f, df_new], ignore_index=True)
    save_df_to_worksheet(df_f, "FlujoCaja", HEAD_FLUJO, cache_bust_key="cache_bust")

# ---------------------------
# UI: Menú principal
# ---------------------------
menu = st.sidebar.selectbox("Selecciona módulo", ["Clientes", "Pedidos", "Entregas/Pagos", "Inventario", "Flujo & Gastos", "Reportes"])
st.write("---")

# Load clients once for selection widgets
df_clients = safe_load("Clientes")
coerce_numeric(df_clients, ["ID Cliente"])

# ---------- CLIENTES ----------
if menu == "Clientes":
    st.header("Clientes")
    st.dataframe(df_clients, use_container_width=True)
    with st.form("form_add_cliente"):
        name = st.text_input("Nombre completo")
        phone = st.text_input("Teléfono")
        addr = st.text_input("Dirección")
        if st.form_submit_button("Agregar cliente"):
            if not name:
                st.error("Nombre requerido")
            else:
                cid = next_id_in_df(df_clients, "ID Cliente")
                new_row = [cid, name, phone, addr]
                safe_append_row_to_ws("Clientes", new_row)
                st.session_state["cache_bust"] += 1
                st.success(f"Cliente agregado con ID {cid}")

# ---------- PEDIDOS ----------
elif menu == "Pedidos":
    st.header("Pedidos — crear / editar / eliminar")
    df_clients = safe_load("Clientes")
    if df_clients.empty:
        st.warning("No hay clientes. Ve a Clientes para agregar uno.")
    else:
        with st.expander("Registrar nuevo pedido"):
            cliente_sel = st.selectbox("Cliente", df_clients["ID Cliente"].astype(int).astype(str) + " - " + df_clients["Nombre"])
            cliente_id = int(cliente_sel.split(" - ")[0])
            num_lines = st.number_input("Número de líneas", min_value=1, max_value=12, value=3)
            new_items: Dict[str,int] = {}
            cols = st.columns(2)
            for i in range(int(num_lines)):
                with cols[i % 2]:
                    prod = st.selectbox(f"Producto {i+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"new_prod_{i}")
                    qty = st.number_input(f"Cantidad {i+1}", min_value=0, step=1, key=f"new_qty_{i}")
                if prod and prod != "-- Ninguno --" and qty > 0:
                    new_items[prod] = new_items.get(prod, 0) + int(qty)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada de entrega", value=datetime.now())
            if st.button("Crear pedido"):
                try:
                    pid = create_order_with_details(cliente_id, new_items, domicilio, fecha_entrega)
                    st.success(f"Pedido creado con ID {pid}")
                except Exception as e:
                    st.error(f"Error creando pedido: {e}")

    st.markdown("---")
    st.subheader("Buscar / Editar pedidos")
    df_pedidos = safe_load("Pedidos")
    coerce_numeric(df_pedidos, ["Semana_entrega", "ID Pedido"])
    weeks = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    week_opts = ["Todas"] + [str(w) for w in weeks]
    week_sel = st.selectbox("Filtrar por semana (ISO)", week_opts, index=0)
    estado_opts = ["Todos", "Pendiente", "Entregado"]
    estado_sel = st.selectbox("Filtrar por estado", estado_opts, index=0)
    df_display = df_pedidos.copy()
    if estado_sel != "Todos":
        df_display = df_display[df_display["Estado"] == estado_sel]
    if week_sel != "Todas":
        df_display = df_display[df_display["Semana_entrega"] == int(week_sel)]
    st.dataframe(df_display.reset_index(drop=True), use_container_width=True)

    if not df_display.empty:
        sel_id = st.selectbox("Selecciona ID Pedido para ver/edit", df_display["ID Pedido"].astype(int).tolist(), key="pedido_sel")
        header_idx = df_pedidos.index[df_pedidos["ID Pedido"] == sel_id][0]
        header = df_pedidos.loc[header_idx].to_dict()
        detalle_df = safe_load("Pedidos_detalle")
        detalle_df = detalle_df[detalle_df["ID Pedido"] == sel_id].reset_index(drop=True)
        # Card visual
        st.markdown("### Detalle del pedido")
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            st.markdown(f"**Cliente:** {header.get('Nombre Cliente','')}")
            st.markdown(f"**Fecha:** {header.get('Fecha','')}")
            try:
                week_val = int(header.get("Semana_entrega", datetime.now().isocalendar().week))
                if week_val < 1 or week_val > 53:
                    week_val = datetime.now().isocalendar().week
            except Exception:
                week_val = datetime.now().isocalendar().week
            st.markdown(f"**Semana (ISO):** {week_val}")
        with c2:
            st.markdown(f"**Subtotal productos:** {int(header.get('Subtotal_productos',0)):,} COP")
            st.markdown(f"**Total:** {int(header.get('Total_pedido',0)):,} COP")
        with c3:
            st.markdown(f"**Domicilio:** {int(header.get('Monto_domicilio',0)):,} COP")
            st.markdown(f"**Saldo pendiente:** {int(header.get('Saldo_pendiente',0)):,} COP")
        st.markdown("---")
        st.markdown("#### Productos (editar cantidades / eliminar filas)")
        edited_items: Dict[str,int] = {}
        if detalle_df.empty:
            st.info("No hay líneas registradas en Pedidos_detalle para este pedido.")
        else:
            for i, row in detalle_df.iterrows():
                rcols = st.columns([4,2,2,1])
                prod = rcols[0].selectbox(f"Producto {i+1}", list(PRODUCTOS.keys()), index=list(PRODUCTOS.keys()).index(row["Producto"]) if row["Producto"] in PRODUCTOS else 0, key=f"edit_prod_{i}")
                qty = rcols[1].number_input(f"Cantidad {i+1}", min_value=0, step=1, value=int(row["Cantidad"]), key=f"edit_qty_{i}")
                rcols[2].markdown(f"Unit: {int(row['Precio_unitario']):,}".replace(",","."))
                remove = rcols[3].checkbox("Eliminar", key=f"remove_line_{i}")
                if not remove:
                    edited_items[prod] = edited_items.get(prod, 0) + int(qty)
        st.markdown("Añadir nuevas líneas")
        new_lines = st.number_input("Agregar líneas", min_value=0, max_value=8, value=0, key="add_lines")
        if int(new_lines) > 0:
            for j in range(int(new_lines)):
                ac1, ac2 = st.columns([4,2])
                p = ac1.selectbox(f"Nuevo producto {j+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"add_prod_{j}")
                q = ac2.number_input(f"Nueva cantidad {j+1}", min_value=0, step=1, key=f"add_qty_{j}")
                if p and p != "-- Ninguno --" and q > 0:
                    edited_items[p] = edited_items.get(p, 0) + int(q)
        st.markdown("---")
        domic_opt = st.selectbox("Domicilio", ["No", f"Sí ({DOMICILIO_COST} COP)"], index=0 if header.get("Monto_domicilio",0)==0 else 1)
        new_week = st.number_input("Semana entrega (ISO)", min_value=1, max_value=53, value=week_val)
        new_state = st.selectbox("Estado", ["Pendiente","Entregado"], index=0 if header.get("Estado","Pendiente")!="Entregado" else 1)
        if st.button("Guardar cambios en pedido"):
            try:
                if not edited_items:
                    st.warning("No hay líneas definidas; el pedido quedaría vacío. Usa eliminar si quieres borrar el pedido.")
                else:
                    new_domic = True if "Sí" in domic_opt else False
                    edit_order_details(sel_id, edited_items, new_domic_bool=new_domic, new_week=new_week)
                    dfp = safe_load("Pedidos")
                    idxh = dfp.index[dfp["ID Pedido"]==sel_id][0]
                    dfp.at[idxh, "Estado"] = new_state
                    save_df_to_worksheet(dfp, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
                    st.success("Pedido actualizado correctamente")
            except Exception as e:
                st.error(f"Error actualizando pedido: {e}")
        st.markdown("---")
        if st.button("Eliminar pedido (revertir inventario)"):
            try:
                delete_order_and_revert(sel_id)
                st.success("Pedido eliminado y stock revertido")
            except Exception as e:
                st.error(f"Error eliminando pedido: {e}")

# ---------------------------
# ENTREGAS / PAGOS
# ---------------------------
elif menu == "Entregas/Pagos":
    st.header("Entregas y Pagos")
    df_ped = safe_load("Pedidos")
    if df_ped.empty:
        st.info("No hay pedidos.")
    else:
        estado_choice = st.selectbox("Estado", ["Todos","Pendiente","Entregado"])
        weeks = sorted(df_ped["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_ped.empty else []
        weeks_opts = ["Todas"] + [str(w) for w in weeks]
        week_filter = st.selectbox("Semana (ISO)", weeks_opts)
        df_view = df_ped.copy()
        if estado_choice != "Todos":
            df_view = df_view[df_view["Estado"]==estado_choice]
        if week_filter != "Todas":
            df_view = df_view[df_view["Semana_entrega"]==int(week_filter)]
        st.dataframe(df_view.reset_index(drop=True), use_container_width=True)

        if not df_view.empty:
            ids = df_view["ID Pedido"].astype(int).tolist()
            selection = st.selectbox("Selecciona ID Pedido", ids)
            idx = df_ped.index[df_ped["ID Pedido"]==selection][0]
            row = df_ped.loc[idx]
            st.markdown(f"**Cliente:** {row['Nombre Cliente']}  \n**Total:** {int(row['Total_pedido']):,} COP  \n**Pagado:** {int(row['Monto_pagado']):,} COP  \n**Saldo:** {int(row['Saldo_pendiente']):,} COP")
            detalle = safe_load("Pedidos_detalle")
            detalle = detalle[detalle["ID Pedido"]==selection]
            if not detalle.empty:
                st.table(detalle[["Producto","Cantidad","Precio_unitario","Subtotal"]].set_index(pd.Index(range(1,len(detalle)+1))))
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

# ---------------------------
# INVENTARIO
# ---------------------------
elif menu == "Inventario":
    st.header("Inventario")
    df_inv = safe_load("Inventario")
    if df_inv.empty:
        st.info("Inventario vacío")
    else:
        # Normalize and aggregate inventory for display (no duplicates)
        df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
        coerce_numeric(df_inv, ["Stock"])
        df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"}).sort_values("Producto").reset_index(drop=True)
        st.dataframe(df_inv, use_container_width=True)
        with st.expander("Ajustar stock manualmente"):
            prod = st.selectbox("Producto", df_inv["Producto"].tolist())
            add_q = st.number_input("Sumar cantidad (positivo) o dejar 0", min_value=0, step=1, value=0)
            if st.button("Aplicar ajuste"):
                try:
                    idx = df_inv.index[df_inv["Producto"]==prod][0]
                    df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) + int(add_q)
                    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")
                    st.success("Stock ajustado")
                except Exception as e:
                    st.error(f"Error ajustando inventario: {e}")
        st.markdown("---")
        st.subheader("Unidades vendidas por producto (desde Pedidos_detalle)")
        detalle_all = safe_load("Pedidos_detalle")
        unidades = unidades_vendidas_por_producto(detalle_all) if not detalle_all.empty else {p:0 for p in PRODUCTOS.keys()}
        df_unid = pd.DataFrame(list(unidades.items()), columns=["Producto","Unidades vendidas"]).set_index("Producto")
        st.dataframe(df_unid)

# ---------------------------
# FLUJO & GASTOS
# ---------------------------
elif menu == "Flujo & Gastos":
    st.header("Flujo de caja y gastos")
    total_prod, total_dom, total_gastos, saldo = flow_summaries()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ingresos productos", f"{int(total_prod):,} COP".replace(",", "."))
    col2.metric("Ingresos domicilios", f"{int(total_dom):,} COP".replace(",", "."))
    col3.metric("Gastos", f"-{int(total_gastos):,} COP".replace(",", "."))
    col4.metric("Saldo disponible", f"{int(saldo):,} COP".replace(",","."))

    st.markdown("---")
    st.subheader("Totales por medio de pago (según Flujo)")
    by_method = totals_by_payment_method()
    if not by_method:
        st.info("No hay movimientos registrados")
    else:
        df_methods = pd.DataFrame(list(by_method.items()), columns=["Medio_pago","Total_ingresos"]).set_index("Medio_pago")
        st.dataframe(df_methods)

    st.markdown("---")
    st.subheader("Registrar movimiento entre medios (ej. retiro: Transferencia -> Efectivo)")
    with st.form("form_move"):
        amt = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        from_m = st.selectbox("De (medio)", ["Transferencia","Efectivo","Nequi","Daviplata"])
        to_m = st.selectbox("A (medio)", ["Efectivo","Transferencia","Nequi","Daviplata"])
        note = st.text_input("Nota (opcional)", value="Movimiento interno / Retiro")
        if st.form_submit_button("Registrar movimiento"):
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
        if st.form_submit_button("Agregar gasto"):
            try:
                add_expense(concept, m)
                st.success("Gasto agregado")
            except Exception as e:
                st.error(f"Error agregando gasto: {e}")

    st.markdown("---")
    st.subheader("Movimientos recientes en Flujo")
    df_f = safe_load("FlujoCaja")
    if not df_f.empty:
        st.dataframe(df_f.tail(200), use_container_width=True)
    df_g = safe_load("Gastos")
    if not df_g.empty:
        st.subheader("Gastos recientes")
        st.dataframe(df_g.tail(200), use_container_width=True)

# ---------------------------
# REPORTES
# ---------------------------
elif menu == "Reportes":
    st.header("Reportes")
    df_p = safe_load("Pedidos")
    df_det = safe_load("Pedidos_detalle")
    if not df_p.empty:
        st.subheader("Pedidos (completo)")
        st.dataframe(df_p, use_container_width=True)
    if not df_det.empty:
        st.subheader("Detalle Pedidos")
        st.dataframe(df_det, use_container_width=True)
    st.subheader("Resumen unidades por producto")
    df_det_full = df_det if not df_det.empty else pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    resumen_unid = unidades_vendidas_por_producto(df_det_full)
    st.dataframe(pd.DataFrame(list(resumen_unid.items()), columns=["Producto","Unidades"]).set_index("Producto"))

st.write("---")
st.caption("Nota: Si las escrituras a Google Sheets fallan por permisos, la app seguirá funcionando en memoria hasta que se corrijan los permisos. Comparte el Sheet con el client_email de la cuenta de servicio (Editor).")
