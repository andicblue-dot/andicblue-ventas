# andicblue_full_optimized.py
# AndicBlue - App completa optimizada para Streamlit Cloud
# Google Sheet name: andicblue_pedidos
# Requisitos:
#  - Añadir st.secrets["gcp_service_account"] con JSON de la cuenta de servicio
#  - Compartir el Google Sheet 'andicblue_pedidos' con el client_email de la cuenta de servicio (Editor)
#
# Funcionalidades incluidas (optimizada):
#  - Clientes
#  - Pedidos (cabecera) + Pedidos_detalle (líneas)
#  - CRUD pedidos (crear, editar, eliminar) con reversión de inventario
#  - Inventario editable (permitir valores negativos, evita duplicados en la hoja)
#  - Entregas/Pagos: pagos parciales, desglose producto vs domicilio (DOMICILIO_COST)
#  - FlujoCaja y Gastos; totales por medio de pago; movimientos entre medios (retiros)
#  - Dashboard con métricas visuales: ventas por producto, ventas diarias, ingresos por medio, pedidos por semana, top productos, stock
#  - Protecciones: caché (st.cache_data), reintentos con backoff para 429, modo offline con copia local backup, validaciones de input
#  - Prevención de duplicación de encabezados en las hojas
#  - Interfaz mejorada: tarjetas, tablas, controles, formularios
#
# Nota: Este archivo intenta ser robusto frente a límites de API y problemas de permisos.
#       Si experimentas "Quota exceeded", reduce frecuencia de uso simultáneo o usa backups temporales.
#
# Autor: Generado y adaptado a pedido por ChatGPT
# Fecha: 2025-10

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import json
import io
from typing import Any, Dict, List, Tuple

# ---------------------------
# CONFIG UI / CONSTANTES
# ---------------------------
st.set_page_config(page_title="AndicBlue — Gestión Integral", page_icon="🫐", layout="wide")
st.title("🫐 AndicBlue — Gestión de Pedidos")

SHEET_NAME = "andicblue_pedidos"
DOMICILIO_COST = 3000  # COP

# Productos y precios canónicos
PRODUCTOS = {
    "Docena de Arándanos 125g": 52500,
    "Arandanos_125g": 5000,
    "Arandanos_250g": 10000,
    "Arandanos_500g": 20000,
    "Kilo_industrial": 30000,
    "Mermelada_azucar": 16000,
    "Mermelada_sin_azucar": 20000,
}

# Encabezados esperados en las hojas
HEAD_CLIENTES = ["ID Cliente", "Nombre", "Telefono", "Direccion"]
HEAD_PEDIDOS = [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
    "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Estado",
    "Medio_pago", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"
]
HEAD_PEDIDOS_DETALLE = ["ID Pedido", "Producto", "Cantidad", "Precio_unitario", "Subtotal"]
HEAD_INVENTARIO = ["Producto", "Stock"]
# Corrección: HEAD_FLUJO debe coincidir con los campos que se usan
HEAD_FLUJO = ["Fecha", "ID Pedido", "Cliente", "Medio_pago", "Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total_pedido"]
HEAD_GASTOS = ["Fecha", "Concepto", "Monto"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ---------------------------
# AUTH: validar secrets
# ---------------------------
if "gcp_service_account" not in st.secrets:
    st.error("⚠️ Debes añadir 'gcp_service_account' en Streamlit Secrets con JSON de la cuenta de servicio.")
    st.stop()

# crear cliente gspread (con manejo)
@st.cache_resource
def build_gspread_client():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error creando cliente de Google: {e}. Revisa tus credenciales.")
        return None

gc = build_gspread_client()
if gc is None:
    st.stop()

# ---------------------------
# UTIL: backoff / safe open
# ---------------------------
def exponential_backoff_sleep(attempt: int):
    # sleeps 0.5,1,2,4,8 seconds bounded
    time.sleep(min(8, 0.5 * (2 ** attempt)))

def safe_open_spreadsheet(name: str, retries: int = 5):
    last_exc = None
    for attempt in range(retries):
        try:
            ss = gc.open(name)
            return ss
        except gspread.exceptions.APIError as e:
            last_exc = e
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "User Rate Limit" in msg or "[429]" in msg:
                st.warning("⚠️ Límite de lectura/escritura de Google Sheets alcanzado. Reintentando...")
                exponential_backoff_sleep(attempt)
                continue
            else:
                st.error(f"APIError abriendo spreadsheet: {e}")
                raise
        except Exception as e:
            last_exc = e
            st.warning(f"Error abriendo sheet: {e}. Reintentando...")
            exponential_backoff_sleep(attempt)
    st.error("❌ No se pudo abrir el spreadsheet tras varios intentos. Revisa permisos y cuota.")
    if last_exc:
        raise last_exc
    st.stop()

def safe_get_worksheet(ss, title: str):
    try:
        ws = ss.worksheet(title)
        return ws
    except gspread.exceptions.WorksheetNotFound:
        # intentar crear
        try:
            st.info(f"Creando hoja '{title}' porque no existe...")
            ws = ss.add_worksheet(title=title, rows="1000", cols="20")
            return ws
        except Exception as e:
            st.error(f"❌ No se pudo crear la hoja '{title}': {e}")
            st.stop()
    except Exception as e:
        st.error(f"❌ Error al intentar acceder/crear la hoja '{title}': {e}")
        st.stop()


# session cache bust token
if "cache_bust" not in st.session_state:
    st.session_state["cache_bust"] = 0

# ---------------------------
# LECTURAS CACHÉADAS
# ---------------------------
@st.cache_data(ttl=90, show_spinner="Cargando datos de Google Sheets...")
def load_sheet_to_df(sheet_title: str, cache_bust: int = 0) -> pd.DataFrame:
    """
    Carga hoja a DataFrame con reintentos. cache_bust permite forzar recarga tras escrituras.
    """
    ss = safe_open_spreadsheet(SHEET_NAME)
    ws = safe_get_worksheet(ss, sheet_title)
    
    expected_headers = []
    if sheet_title == "Clientes": expected_headers = HEAD_CLIENTES
    elif sheet_title == "Pedidos": expected_headers = HEAD_PEDIDOS
    elif sheet_title == "Pedidos_detalle": expected_headers = HEAD_PEDIDOS_DETALLE
    elif sheet_title == "Inventario": expected_headers = HEAD_INVENTARIO
    elif sheet_title == "FlujoCaja": expected_headers = HEAD_FLUJO
    elif sheet_title == "Gastos": expected_headers = HEAD_GASTOS

    for attempt in range(4):
        try:
            # Intentar obtener todas las filas como registros (diccionarios)
            # esto usa la primera fila como encabezado por defecto
            rows = ws.get_all_records()
            df = pd.DataFrame(rows)

            # Si el DataFrame está vacío, asegurar las columnas esperadas
            if df.empty:
                return pd.DataFrame(columns=expected_headers)
            
            # Reindexar para asegurar el orden y añadir columnas faltantes, o eliminar sobrantes
            df = df.reindex(columns=expected_headers)

            return df
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                st.warning(f"⚠️ Límite de lectura de Google Sheets alcanzado para '{sheet_title}'. Reintentando...")
                exponential_backoff_sleep(attempt)
                continue
            st.error(f"❌ APIError leyendo hoja '{sheet_title}': {e}")
            return pd.DataFrame(columns=expected_headers)
        except Exception as e:
            st.error(f"❌ Error leyendo hoja '{sheet_title}': {e}")
            return pd.DataFrame(columns=expected_headers)
    st.error(f"❌ No se pudo leer la hoja '{sheet_title}' tras varios intentos.")
    return pd.DataFrame(columns=expected_headers)


# ---------------------------
# ESCRITURAS SEGURAS (sin duplicar encabezados)
# ---------------------------
def ensure_headers(ws, headers: List[str]):
    """
    Asegura que la primera fila de la hoja sean los headers correctos.
    Si la hoja está vacía, o los encabezados son incorrectos, los inserta.
    """
    try:
        current_headers = ws.row_values(1)
        # Si la hoja está vacía o los encabezados no coinciden, corregir
        if not current_headers or current_headers[:len(headers)] != headers:
            # Si hay contenido en la primera fila, borrarlo para reinsertar
            if current_headers: # solo si hay algo
                ws.delete_rows(1)
            ws.insert_row(headers, index=1)
    except Exception as e:
        st.warning(f"⚠️ Error asegurando encabezados en hoja {ws.title}: {e}. Intentando insertar directamente.")
        try:
            ws.insert_row(headers, index=1)
        except Exception as e_insert:
            st.error(f"❌ Fallo al insertar encabezados en {ws.title}: {e_insert}")


def save_df_to_worksheet(df: pd.DataFrame, sheet_title: str, headers: List[str], cache_bust_key: str = "cache_bust"):
    """
    Sobrescribe hoja entera: escribe headers y luego filas.
    Previene duplicación de encabezados y actualiza token cache_bust.
    """
    for attempt in range(3): # Reintentar la escritura completa
        try:
            ss = safe_open_spreadsheet(SHEET_NAME)
            ws = safe_get_worksheet(ss, sheet_title)
            
            # Limpiar y escribir headers + filas
            ws.clear()
            ws.append_row(headers)
            
            # Añadir filas en bloques para evitar timeouts y asegurar que los None se manejen
            # Convertir todos los NaN/None a cadenas vacías antes de enviar
            rows = df.fillna("").values.tolist()
            if rows:
                ws.append_rows(rows) # Usa append_rows para eficiencia
            
            st.session_state[cache_bust_key] = st.session_state.get(cache_bust_key, 0) + 1
            return # Éxito, salir de la función
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                st.warning(f"⚠️ Límite de escritura de Google Sheets alcanzado para '{sheet_title}'. Reintentando...")
                exponential_backoff_sleep(attempt)
                continue
            st.error(f"❌ No se pudo guardar '{sheet_title}' (APIError): {e}")
            return
        except Exception as e:
            st.error(f"❌ No se pudo guardar '{sheet_title}': {e}")
            return
    st.error(f"❌ Fallo persistente al guardar '{sheet_title}' tras varios intentos.")


def safe_append_row(sheet_title: str, row: List[Any], cache_bust_key: str = "cache_bust"):
    """
    Anexa fila sin tocar encabezado. Si la hoja está vacía, primero crea encabezados apropiados.
    """
    for attempt in range(3): # Reintentar la anexión de fila
        try:
            ss = safe_open_spreadsheet(SHEET_NAME)
            ws = safe_get_worksheet(ss, sheet_title)
            
            # Ensure headers exist for known sheets
            mapping = {
                "Clientes": HEAD_CLIENTES,
                "Pedidos": HEAD_PEDIDOS,
                "Pedidos_detalle": HEAD_PEDIDOS_DETALLE,
                "Inventario": HEAD_INVENTARIO,
                "FlujoCaja": HEAD_FLUJO,
                "Gastos": HEAD_GASTOS
            }
            if sheet_title in mapping:
                ensure_headers(ws, mapping[sheet_title])
            
            # Convertir valores None a cadena vacía para evitar errores de gspread
            row_to_append = [("" if v is None else v) for v in row]
            ws.append_row(row_to_append)
            
            st.session_state[cache_bust_key] = st.session_state.get(cache_bust_key, 0) + 1
            return # Éxito, salir de la función
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "Quota exceeded" in msg or "rateLimitExceeded" in msg or "[429]" in msg:
                st.warning(f"⚠️ Límite de escritura de Google Sheets alcanzado al anexar en '{sheet_title}'. Reintentando...")
                exponential_backoff_sleep(attempt)
                continue
            st.error(f"❌ No se pudo anexar fila en '{sheet_title}' (APIError): {e}")
            return
        except Exception as e:
            st.error(f"❌ No se pudo anexar fila en '{sheet_title}': {e}")
            return
    st.error(f"❌ Fallo persistente al anexar fila en '{sheet_title}' tras varios intentos.")


# ---------------------------
# HELPERS: normalización y parsing
# ---------------------------
def coerce_numeric(df: pd.DataFrame, cols: List[str]):
    """
    Convierte columnas a numérico, rellenando valores no numéricos con 0.
    Maneja comas como separadores decimales si es necesario.
    """
    for c in cols:
        if c in df.columns:
            # Asegurarse de que la columna sea tratada como string antes de reemplazar y convertir
            if df[c].dtype == 'object' or pd.api.types.is_string_dtype(df[c]):
                df[c] = pd.to_numeric(df[c].astype(str).str.replace('.', '', regex=False).str.replace(',', '.'), errors="coerce").fillna(0)
            else: # Ya es numérico o booleano, intentar convertir directamente
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)


def canonical_product_name(name: str) -> str:
    """
    Normaliza el nombre de un producto para que coincida con la lista PRODUCTOS.
    """
    if not isinstance(name, str):
        return str(name) # Convertir a string para evitar errores

    s = name.strip()
    # Si ya es un nombre canónico, retornarlo
    if s in PRODUCTOS:
        return s

    # Normalizar para búsqueda flexible (minúsculas, sin espacios/guiones)
    def norm(x):
        return str(x).lower().replace(" ", "").replace("_","").replace("-","")
    
    ns = norm(s)
    
    # Buscar coincidencia exacta normalizada
    for k in PRODUCTOS.keys():
        if norm(k) == ns:
            return k
    
    # Buscar substring (menos fiable, pero puede ayudar con typos)
    for k in PRODUCTOS.keys():
        if ns in norm(k) or norm(k) in ns:
            return k
            
    # Si no se encuentra, retornar el nombre original saneado.
    # No queremos agregar productos que no están en la lista canónica de PRODUCTOS.
    return s


def parse_legacy_product_cell(cell_text: str) -> Dict[str,int]:
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
            # Ignorar partes mal formadas
            continue
    return items

def build_detalle_rows(order_id: int, items: Dict[str,int]) -> List[List[Any]]:
    rows = []
    for prod_raw, qty in items.items():
        prod = canonical_product_name(prod_raw)
        # Asegurarse que el producto existe en PRODUCTOS antes de usar su precio
        if prod in PRODUCTOS:
            price = PRODUCTOS[prod]
            subtotal = int(qty) * int(price)
            rows.append([order_id, prod, int(qty), int(price), int(subtotal)])
        else:
            st.warning(f"Producto '{prod_raw}' no reconocido en la lista canónica de productos. No se agregará al detalle.")
    return rows

def next_id(df: pd.DataFrame, col: str) -> int:
    if df is None or df.empty or col not in df.columns:
        return 1
    # Asegurarse de que la columna sea numérica antes de buscar el máximo
    df_temp = df.copy()
    coerce_numeric(df_temp, [col])
    vals = df_temp[col].dropna().astype(int).tolist()
    return max(vals) + 1 if vals else 1

# ---------------------------
# Inicializar hojas y headers si faltan (safe)
# ---------------------------
def initialize_sheets_if_missing():
    """
    Asegura que todas las hojas esperadas existan y tengan sus encabezados correctos.
    """
    ss = safe_open_spreadsheet(SHEET_NAME)
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
            ws = safe_get_worksheet(ss, title) # Esto ya maneja la creación si no existe
            ensure_headers(ws, headers)
            st.success(f"Hoja '{title}' verificada/inicializada correctamente.")
        except Exception as e:
            st.error(f"❌ Error al inicializar/verificar la hoja '{title}': {e}")

# intentar inicializar (no bloquear si falla en el primer intento, pero lo loguea)
try:
    initialize_sheets_if_missing()
except Exception as e:
    st.error(f"❌ Fallo general al intentar inicializar hojas: {e}")

# ---------------------------
# CORE: Pedidos CRUD, Inventario, Pagos
# ---------------------------
def create_order(cliente_id: int, items: Dict[str,int], domicilio_bool: bool=False, fecha_entrega=None) -> int:
    df_clients = load_sheet_to_df("Clientes", st.session_state["cache_bust"])
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    # Asegurarse de que las columnas numéricas sean de tipo numérico
    coerce_numeric(df_clients, ["ID Cliente"])
    coerce_numeric(df_ped, ["ID Pedido"])
    coerce_numeric(df_inv, ["Stock"])

    client_name = ""
    if not df_clients.empty and "ID Cliente" in df_clients.columns:
        # Asegurarse de que el ID Cliente sea comparable
        cliente_id_series = df_clients["ID Cliente"].astype(int)
        if cliente_id in cliente_id_series.values:
            client_name = df_clients.loc[cliente_id_series == cliente_id, "Nombre"].values[0]
        else:
            raise ValueError(f"ID Cliente {cliente_id} no encontrado. Por favor, verifica el ID o crea un nuevo cliente.")

    subtotal = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in items.items())
    monto_dom = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + monto_dom
    
    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = int(fecha_dt.isocalendar().week)

    pid = next_id(df_ped, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Asegurar que el Saldo_pendiente sea el Total_pedido inicialmente
    header_row = [pid, fecha_actual, cliente_id, client_name, subtotal, monto_dom, total, "Pendiente", "", 0, total, semana_entrega]

    # Append header and detalle safely (no headers duplication)
    safe_append_row("Pedidos", header_row)
    detalle_rows = build_detalle_rows(pid, items)
    for r in detalle_rows:
        safe_append_row("Pedidos_detalle", r)

    # Update inventory: read full sheet and apply diffs
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
    
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    coerce_numeric(df_inv, ["Stock"])

    for prod_raw, qty in items.items():
        prod = canonical_product_name(prod_raw)
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
        elif prod in PRODUCTOS: # Solo añadir si el producto es canónico
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)
        else:
            st.warning(f"El producto '{prod_raw}' no está en la lista de productos canónicos y no se actualizará en el inventario.")

    # Re-agrupar para consolidar el inventario si hay duplicados
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")
    return pid

def edit_order(order_id: int, new_items: Dict[str,int], new_domic_bool: bool=None, new_week: int=None, new_state: str = None):
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    # Asegurar tipos numéricos
    coerce_numeric(df_det, ["ID Pedido", "Cantidad"])
    coerce_numeric(df_ped, ["ID Pedido", "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"])
    coerce_numeric(df_inv, ["Stock"])

    if df_ped.empty:
        raise ValueError("No hay pedidos registrados.")
    if order_id not in df_ped["ID Pedido"].values:
        raise ValueError("Pedido no encontrado.")
    
    if df_det.empty:
        df_det = pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)

    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    
    # Revertir cantidades antiguas al inventario
    old_lines = df_det[df_det["ID Pedido"] == order_id]
    old_counts = {}
    for _, r in old_lines.iterrows():
        prod = canonical_product_name(r["Producto"])
        qty = int(r["Cantidad"])
        old_counts[prod] = old_counts.get(prod, 0) + qty
    
    for prod, qty in old_counts.items():
        if prod in df_inv["Producto"].values:
            idx = df_inv.index[df_inv["Producto"] == prod][0]
            df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) + qty
        elif prod in PRODUCTOS:
            df_inv = pd.concat([df_inv, pd.DataFrame([[prod, qty]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # Eliminar filas de detalle antiguas para este pedido
    df_det = df_det[df_det["ID Pedido"] != order_id].reset_index(drop=True)

    # Añadir nuevas filas de detalle y sustraer del inventario
    for prod_raw, qty in new_items.items():
        prod = canonical_product_name(prod_raw)
        if prod in PRODUCTOS: # Solo añadir si el producto es canónico
            precio = PRODUCTOS[prod]
            new_row = [order_id, prod, int(qty), int(precio), int(qty) * int(precio)]
            df_det = pd.concat([df_det, pd.DataFrame([new_row], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
            
            if prod in df_inv["Producto"].values:
                idx = df_inv.index[df_inv["Producto"] == prod][0]
                df_inv.at[idx, "Stock"] = int(df_inv.at[idx, "Stock"]) - int(qty)
            elif prod in PRODUCTOS:
                df_inv = pd.concat([df_inv, pd.DataFrame([[prod, -int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)
        else:
            st.warning(f"El producto '{prod_raw}' no está en la lista de productos canónicos y no se agregará/actualizará en el detalle/inventario.")

    # Actualizar totales en la cabecera del pedido
    subtotal = sum(PRODUCTOS.get(canonical_product_name(p), 0) * int(q) for p,q in new_items.items())
    idx_header = df_ped.index[df_ped["ID Pedido"] == order_id][0]
    
    current_domicilio = float(df_ped.at[idx_header, "Monto_domicilio"])
    domicilio = current_domicilio if new_domic_bool is None else (DOMICILIO_COST if new_domic_bool else 0)
    
    total = subtotal + domicilio
    monto_pagado = float(df_ped.at[idx_header, "Monto_pagado"])
    saldo = total - monto_pagado
    
    df_ped.at[idx_header, "Subtotal_productos"] = subtotal
    df_ped.at[idx_header, "Monto_domicilio"] = domicilio
    df_ped.at[idx_header, "Total_pedido"] = total
    df_ped.at[idx_header, "Saldo_pendiente"] = saldo
    
    if new_week:
        df_ped.at[idx_header, "Semana_entrega"] = int(new_week)
    if new_state: # Actualiza el estado si se proporciona uno nuevo
        df_ped.at[idx_header, "Estado"] = new_state


    # Persistir cambios
    save_df_to_worksheet(df_ped, "Pedidos", HEAD_PEDIDOS, cache_bust_key="cache_bust")
    save_df_to_worksheet(df_det, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE, cache_bust_key="cache_bust")
    
    df_inv["Producto"] = df_inv["Producto"].astype(str).apply(lambda x: canonical_product_name(x))
    df_inv = df_inv.groupby("Producto", as_index=False).agg({"Stock":"sum"})
    save_df_to_worksheet(df_inv, "Inventario", HEAD_INVENTARIO, cache_bust_key="cache_bust")

def delete_order(order_id: int):
    df_det = load_sheet_to_df("Pedidos_detalle", st.session_state["cache_bust"])
    df_ped = load_sheet_to_df("Pedidos", st.session_state["cache_bust"])
    df_inv = load_sheet_to_df("Inventario", st.session_state["cache_bust"])

    coerce_numeric(df_det, ["ID Pedido", "Cantidad"])
    coerce_numeric(df_ped, ["ID Pedido"])
    coerce_numeric(df_inv, ["Stock"])

    if df_det.empty:
        df_det = pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    if df_inv.empty:
        df_inv = pd.DataFrame([[p, 0] for p in PRODUCTOS.keys()], columns=HEAD_INVENTARIO)
    
    df_inv["Producto"] = df_inv["Producto"].astype(str).