# andicblue_enterprise_detalle.py
# AndicBlue - Enterprise (Pedidos con detalle en hoja Pedidos_detalle)
# Requisitos: st.secrets["gcp_service_account"] con JSON de la cuenta de servicio
# Google Sheet name: andicblue_pedidos
# Hojas esperadas (will be created if missing): Clientes, Pedidos, Pedidos_detalle, Inventario, FlujoCaja, Gastos

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="AndicBlue - Enterprise (Detalle pedidos)", page_icon="🫐", layout="wide")
st.title("🫐 AndicBlue — Gestión (Pedidos con detalle)")

# ---------------------------
# CONFIG
# ---------------------------
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
DOMICILIO_COST = 3000  # COP

# ---------------------------
# AUTH
# ---------------------------
if "gcp_service_account" not in st.secrets:
    st.error("⚠️ Falta la sección 'gcp_service_account' en Streamlit Secrets.")
    st.stop()

creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

# ---------------------------
# HEADERS / HOJAS
# ---------------------------
HEAD_CLIENTES = ["ID Cliente", "Nombre", "Telefono", "Direccion"]
HEAD_PEDIDOS = [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
    "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Estado",
    "Medio_pago", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"
]
# Note: Productos_detalle se maneja en hoja Pedidos_detalle
HEAD_PEDIDOS_DETALLE = ["ID Pedido", "Producto", "Cantidad", "Precio_unitario", "Subtotal"]
HEAD_INVENTARIO = ["Producto", "Stock"]
HEAD_FLUJO = [
    "Fecha", "ID Pedido", "Cliente", "Medio_pago",
    "Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"
]
HEAD_GASTOS = ["Fecha", "Concepto", "Monto"]

# ---------------------------
# UTILIDADES SHEETS (seguras)
# ---------------------------
def open_spreadsheet(name):
    try:
        ss = gc.open(name)
    except Exception as e:
        st.error(f"No se puede abrir el Sheet '{name}'. Asegúrate de haberlo creado y compartido con la cuenta de servicio. Detalle: {e}")
        st.stop()
    return ss

def ensure_worksheet(ss, title, headers):
    """Ensure worksheet exists and headers present (try to create worksheet if missing)."""
    try:
        ws = ss.worksheet(title)
    except Exception:
        try:
            ss.add_worksheet(title=title, rows="1000", cols="20")
            ws = ss.worksheet(title)
        except Exception:
            st.error(f"No se puede crear/abrir la hoja '{title}' en el Sheet.")
            st.stop()
    # ensure headers
    try:
        vals = ws.row_values(1)
    except Exception:
        vals = []
    try:
        if not vals or vals[:len(headers)] != headers:
            # delete first row if contains something
            try:
                if ws.row_count >= 1 and any(ws.row_values(1)):
                    ws.delete_rows(1)
            except Exception:
                pass
            try:
                ws.insert_row(headers, index=1)
            except Exception:
                pass
    except Exception:
        pass
    return ws

ss = open_spreadsheet(SHEET_NAME)
ws_clientes = ensure_worksheet(ss, "Clientes", HEAD_CLIENTES)
ws_pedidos = ensure_worksheet(ss, "Pedidos", HEAD_PEDIDOS)
ws_pedidos_detalle = ensure_worksheet(ss, "Pedidos_detalle", HEAD_PEDIDOS_DETALLE)
ws_inventario = ensure_worksheet(ss, "Inventario", HEAD_INVENTARIO)
ws_flujo = ensure_worksheet(ss, "FlujoCaja", HEAD_FLUJO)
ws_gastos = ensure_worksheet(ss, "Gastos", HEAD_GASTOS)

# ---------------------------
# CARGA INICIAL (con manejo)
# ---------------------------
def safe_load(ws, headers):
    try:
        df = pd.DataFrame(ws.get_all_records())
        if df.empty:
            df = pd.DataFrame(columns=headers)
    except Exception:
        df = pd.DataFrame(columns=headers)
    return df

df_clientes = safe_load(ws_clientes, HEAD_CLIENTES)
df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
df_pedidos_detalle = safe_load(ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
df_flujo = safe_load(ws_flujo, HEAD_FLUJO)
df_gastos = safe_load(ws_gastos, HEAD_GASTOS)

# normalize numeric columns
def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

coerce_numeric(df_pedidos, ["Subtotal_productos","Monto_domicilio","Total_pedido","Monto_pagado","Saldo_pendiente","Semana_entrega"])
coerce_numeric(df_pedidos_detalle, ["Cantidad","Precio_unitario","Subtotal"])
coerce_numeric(df_inventario, ["Stock"])
coerce_numeric(df_flujo, ["Ingreso_productos_recibido","Ingreso_domicilio_recibido","Saldo_pendiente_total"])
coerce_numeric(df_gastos, ["Monto"])

# initialize inventory rows for all PRODUCTS if missing
if df_inventario.empty:
    for p in PRODUCTOS.keys():
        try:
            ws_inventario.append_row([p, 0])
        except Exception:
            pass
    df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
    coerce_numeric(df_inventario, ["Stock"])

# ---------------------------
# SAFE WRITE UTILS
# ---------------------------
def _row_to_values(row):
    vals = []
    for v in list(row):
        if pd.isna(v):
            vals.append("")
        else:
            vals.append(v)
    return vals

def save_df_to_ws(df, ws, headers):
    try:
        ws.clear()
        ws.append_row(headers)
        for _, r in df.iterrows():
            ws.append_row(_row_to_values(r))
    except Exception:
        # don't break the app if sheet write fails
        pass

def next_id(df, col):
    if df.empty or col not in df.columns:
        return 1
    existing = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
    return max(existing) + 1 if existing else 1

# ---------------------------
# PARSING PRODUCT DETAIL (from single-cell format) - backward compat
# ---------------------------
def parse_productos_detalle_text(cell_text):
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
            name_qty = part.split(" x")
            name = name_qty[0].strip()
            qty = int(name_qty[1].split(" ")[0])
            productos[name] = productos.get(name, 0) + qty
        except Exception:
            continue
    return productos

# ---------------------------
# BUSINESS LOGIC: CRUD Orders with detalle sheet and inventory adjustments
# ---------------------------
def build_detalle_rows_from_dict(order_id, items_dict):
    """Given dict {producto: cantidad}, produce detalle rows with price & subtotal."""
    rows = []
    for prod, qty in items_dict.items():
        precio = PRODUCTOS.get(prod, 0)
        subtotal = int(qty) * int(precio)
        rows.append([order_id, prod, int(qty), int(precio), int(subtotal)])
    return rows

def create_order_with_details(cliente_id, items_dict, domicilio_bool=False, fecha_entrega=None):
    """
    items_dict: {product_name: qty}
    Creates header in df_pedidos and lines in df_pedidos_detalle. Adjust inventory (subtract qty).
    """
    global df_pedidos, df_pedidos_detalle, df_inventario
    if df_clientes.empty or cliente_id not in df_clientes["ID Cliente"].astype(int).tolist():
        raise ValueError("ID cliente no encontrado")
    cliente_nombre = df_clientes.loc[df_clientes["ID Cliente"]==cliente_id, "Nombre"].values[0]

    subtotal = sum(PRODUCTOS.get(p,0) * int(q) for p,q in items_dict.items())
    domicilio_monto = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio_monto

    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = int(fecha_dt.isocalendar().week)

    pid = next_id(df_pedidos, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # header (no Productos_detalle column here; details go to Pedidos_detalle)
    header_row = [pid, fecha_actual, cliente_id, cliente_nombre, subtotal, domicilio_monto, total, "Pendiente", "", 0, total, semana_entrega]
    df_pedidos = pd.concat([df_pedidos, pd.DataFrame([header_row], columns=HEAD_PEDIDOS)], ignore_index=True)

    # detalle rows
    detalle_rows = build_detalle_rows_from_dict(pid, items_dict)
    for r in detalle_rows:
        df_pedidos_detalle = pd.concat([df_pedidos_detalle, pd.DataFrame([r], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
    # assign to outer scope copy
    globals()["df_pedidos_detalle"] = df_pedidos_detalle

    # update inventory (subtract quantities)
    for prod, qty in items_dict.items():
        q = int(qty)
        if prod in df_inventario["Producto"].values:
            i = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) - q
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, -q]], columns=HEAD_INVENTARIO)], ignore_index=True)
    globals()["df_inventario"] = df_inventario

    # persist
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_pedidos_detalle, ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)

    # coerce types
    coerce_numeric(df_pedidos, ["Subtotal_productos","Monto_domicilio","Total_pedido","Monto_pagado","Saldo_pendiente","Semana_entrega"])
    coerce_numeric(df_pedidos_detalle, ["Cantidad","Precio_unitario","Subtotal"])
    return pid

def get_order_details_df(order_id):
    """Return dataframe lines for order_id from df_pedidos_detalle."""
    if df_pedidos_detalle.empty:
        return pd.DataFrame(columns=HEAD_PEDIDOS_DETALLE)
    return df_pedidos_detalle[df_pedidos_detalle["ID Pedido"]==order_id].copy()

def edit_order_details(order_id, new_items_dict, new_domicilio_bool=None, new_week=None):
    """
    Replace detalle lines for order_id with new_items_dict.
    Adjust inventory: revert old quantities (add back), subtract new quantities.
    Recalculate subtotal/total/saldo and persist.
    """
    global df_pedidos, df_pedidos_detalle, df_inventario
    # find order index
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except Exception:
        raise ValueError("Pedido no encontrado")

    # old detalles
    old_lines = get_order_details_df(order_id)
    old_counts = {}
    for _, r in old_lines.iterrows():
        old_counts[r["Producto"]] = old_counts.get(r["Producto"], 0) + int(r["Cantidad"])

    # revert inventory: add back old quantities
    for prod, qty in old_counts.items():
        if prod in df_inventario["Producto"].values:
            i = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) + int(qty)
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, int(qty)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # remove old detalle rows
    df_pedidos_detalle = df_pedidos_detalle[df_pedidos_detalle["ID Pedido"]!=order_id].reset_index(drop=True)

    # create new detalle rows and subtract new quantities from inventory
    detalle_rows = build_detalle_rows_from_dict(order_id, new_items_dict)
    for r in detalle_rows:
        df_pedidos_detalle = pd.concat([df_pedidos_detalle, pd.DataFrame([r], columns=HEAD_PEDIDOS_DETALLE)], ignore_index=True)
    for prod, qty in new_items_dict.items():
        q = int(qty)
        if prod in df_inventario["Producto"].values:
            i = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) - q
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, -q]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # update header totals
    subtotal = sum(PRODUCTOS.get(p,0) * int(q) for p,q in new_items_dict.items())
    if new_domicilio_bool is None:
        domicilio = float(df_pedidos.at[idx, "Monto_domicilio"])
    else:
        domicilio = DOMICILIO_COST if new_domicilio_bool else 0
    total = subtotal + domicilio
    monto_pagado = float(df_pedidos.at[idx, "Monto_pagado"])
    saldo = total - monto_pagado

    df_pedidos.at[idx, "Subtotal_productos"] = subtotal
    df_pedidos.at[idx, "Monto_domicilio"] = domicilio
    df_pedidos.at[idx, "Total_pedido"] = total
    df_pedidos.at[idx, "Saldo_pendiente"] = saldo
    if new_week:
        df_pedidos.at[idx, "Semana_entrega"] = int(new_week)

    # persist
    globals()["df_pedidos_detalle"] = df_pedidos_detalle
    globals()["df_inventario"] = df_inventario
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_pedidos_detalle, ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
    coerce_numeric(df_pedidos_detalle, ["Cantidad","Precio_unitario","Subtotal"])
    coerce_numeric(df_inventario, ["Stock"])

def delete_order_and_revert(order_id):
    """Delete order header and detalle, revert inventory and persist."""
    global df_pedidos, df_pedidos_detalle, df_inventario
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except Exception:
        raise ValueError("Pedido no encontrado")
    # revert inventory from detalle
    detalle = get_order_details_df(order_id)
    for _, r in detalle.iterrows():
        prod = r["Producto"]; qty = int(r["Cantidad"])
        if prod in df_inventario["Producto"].values:
            i = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) + qty
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, qty]], columns=HEAD_INVENTARIO)], ignore_index=True)
    # remove detalle and header
    df_pedidos_detalle = df_pedidos_detalle[df_pedidos_detalle["ID Pedido"]!=order_id].reset_index(drop=True)
    df_pedidos = df_pedidos[df_pedidos["ID Pedido"]!=order_id].reset_index(drop=True)
    globals()["df_pedidos_detalle"] = df_pedidos_detalle
    globals()["df_inventario"] = df_inventario
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_pedidos_detalle, ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)

# ---------------------------
# PAYMENTS & FLOW
# ---------------------------
def register_payment(order_id, medio_pago, monto):
    """
    Register a payment for order_id.
    Splits monto between products and domicilio correctly, based on remaining balances.
    Adds a row to flujo with only the amounts received in this transaction.
    """
    global df_pedidos, df_flujo
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except Exception:
        raise ValueError("Pedido no encontrado")
    row = df_pedidos.loc[idx]
    subtotal_products = float(row.get("Subtotal_productos", 0))
    domicilio_monto = float(row.get("Monto_domicilio", 0))
    monto_anterior = float(row.get("Monto_pagado", 0))

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

    # update order
    df_pedidos.at[idx, "Monto_pagado"] = monto_total_reg
    df_pedidos.at[idx, "Saldo_pendiente"] = saldo_total
    df_pedidos.at[idx, "Medio_pago"] = medio_pago
    df_pedidos.at[idx, "Estado"] = "Entregado" if saldo_total == 0 else "Pendiente"

    # append to flujo
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_flow = [fecha, order_id, row["Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]
    df_flujo = pd.concat([df_flujo, pd.DataFrame([new_flow], columns=HEAD_FLUJO)], ignore_index=True)

    # persist
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido","Ingreso_domicilio_recibido"])
    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def totals_by_payment_method():
    """Return dict {medio: total_ingresos} from df_flujo."""
    if df_flujo.empty:
        return {}
    df = df_flujo.copy()
    df["total_ingreso"] = pd.to_numeric(df["Ingreso_productos_recibido"], errors="coerce").fillna(0) + pd.to_numeric(df["Ingreso_domicilio_recibido"], errors="coerce").fillna(0)
    grouped = df.groupby("Medio_pago")["total_ingreso"].sum().to_dict()
    grouped = {str(k): float(v) for k,v in grouped.items() if str(k).strip() != ""}
    return grouped

def flow_summaries():
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido","Ingreso_domicilio_recibido"])
    coerce_numeric(df_gastos, ["Monto"])
    total_prod = df_flujo["Ingreso_productos_recibido"].sum() if not df_flujo.empty else 0
    total_dom = df_flujo["Ingreso_domicilio_recibido"].sum() if not df_flujo.empty else 0
    total_gastos = df_gastos["Monto"].sum() if not df_gastos.empty else 0
    saldo = total_prod + total_dom - total_gastos
    return total_prod, total_dom, total_gastos, saldo

def add_expense(concepto, monto):
    global df_gastos
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_gastos = pd.concat([df_gastos, pd.DataFrame([[fecha, concepto, monto]], columns=HEAD_GASTOS)], ignore_index=True)
    save_df_to_ws(df_gastos, ws_gastos, HEAD_GASTOS)

def move_funds(amount, from_method, to_method, note="Movimiento interno"):
    """Record two rows in flujo: negative on from_method, positive on to_method."""
    global df_flujo
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    neg = [fecha, 0, note + f" ({from_method} -> {to_method})", from_method, -float(amount), 0, 0]
    pos = [fecha, 0, note + f" ({from_method} -> {to_method})", to_method, float(amount), 0, 0]
    df_flujo = pd.concat([df_flujo, pd.DataFrame([neg], columns=HEAD_FLUJO)], ignore_index=True)
    df_flujo = pd.concat([df_flujo, pd.DataFrame([pos], columns=HEAD_FLUJO)], ignore_index=True)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)

# ---------------------------
# UI: Pedidos area - improved visual & editable detalle rows
# ---------------------------
st.markdown("Aplicación con detalle de pedidos — Los cambios se sincronizan con Google Sheets cuando es posible.")

menu = st.sidebar.selectbox("Módulo", ["Pedidos", "Entregas/Pagos", "Inventario", "Flujo & Gastos", "Clientes", "Reportes"])
st.write("---")

# ---------- PEDIDOS ----------
if menu == "Pedidos":
    st.header("📦 Pedidos — Crear / Editar / Eliminar (detalle por línea)")
    # Create new order
    with st.expander("Registrar nuevo pedido"):
        if df_clientes.empty:
            st.warning("No hay clientes registrados. Ve al módulo Clientes para agregar uno.")
        else:
            cliente_sel = st.selectbox("Cliente", df_clientes["ID Cliente"].astype(str) + " - " + df_clientes["Nombre"], key="new_cliente")
            cliente_id = int(cliente_sel.split(" - ")[0])
            # number of product lines to input
            num_lines = st.number_input("Número de líneas de producto", min_value=1, max_value=12, value=3, step=1)
            new_items = {}
            cols = st.columns(2)
            for i in range(int(num_lines)):
                with cols[i % 2]:
                    prod = st.selectbox(f"Producto {i+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"new_prod_{i}")
                    qty = st.number_input(f"Cantidad {i+1}", min_value=0, step=1, value=0, key=f"new_qty_{i}")
                if prod and prod != "-- Ninguno --" and qty > 0:
                    new_items[prod] = new_items.get(prod, 0) + int(qty)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada de entrega", value=datetime.now(), key="new_fecha_entrega")
            if st.button("Crear pedido"):
                try:
                    pid = create_order_with_details(cliente_id, new_items, domicilio, fecha_entrega)
                    st.success(f"Pedido creado con ID {pid}")
                    # refresh in-memory DataFrames
                    df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
                    df_pedidos_detalle = safe_load(ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
                except Exception as e:
                    st.error(f"Error creando pedido: {e}")

    st.write("---")
    st.subheader("Listado y edición")
    estado_opts = ["Todos", "Pendiente", "Entregado"]
    estado_filter = st.selectbox("Filtrar por estado", estado_opts, index=0)
    weeks = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    week_opts = ["Todas"] + [str(w) for w in weeks]
    week_sel = st.selectbox("Filtrar por semana (ISO)", week_opts, index=0)

    df_display = df_pedidos.copy()
    if estado_filter != "Todos":
        df_display = df_display[df_display["Estado"] == estado_filter]
    if week_sel != "Todas":
        df_display = df_display[df_display["Semana_entrega"] == int(week_sel)]

    st.dataframe(df_display.reset_index(drop=True), use_container_width=True)

    if not df_display.empty:
        st.write("Selecciona un pedido para ver detalles (tarjeta) y editar:")
        ids = df_display["ID Pedido"].astype(int).tolist()
        sel_id = st.selectbox("ID Pedido", ids, key="pedido_sel")
        # load header and detalle
        header_idx = df_pedidos.index[df_pedidos["ID Pedido"]==sel_id][0]
        header = df_pedidos.loc[header_idx].to_dict()
        detalle_df = get_order_details_df(sel_id)

        # Visual card
        st.markdown("### Detalle del pedido")
        card_cols = st.columns([2,1,1])
        with card_cols[0]:
            st.markdown(f"**Cliente:** {header.get('Nombre Cliente','')}")
            st.markdown(f"**Fecha:** {header.get('Fecha','')}")
            st.markdown(f"**Semana (ISO):** {int(header.get('Semana_entrega',0))}")
        with card_cols[1]:
            st.markdown(f"**Total:** {int(header.get('Total_pedido',0)):,} COP")
            st.markdown(f"**Subtotal productos:** {int(header.get('Subtotal_productos',0)):,} COP")
        with card_cols[2]:
            st.markdown(f"**Domicilio:** {int(header.get('Monto_domicilio',0)):,} COP")
            st.markdown(f"**Saldo pendiente:** {int(header.get('Saldo_pendiente',0)):,} COP")

        st.markdown("---")
        st.markdown("#### Productos (editar filas y cantidades)")
        # Show editable rows for existing detalle plus option to add new lines
        edited_items = {}
        # existing lines
        if detalle_df.empty:
            st.info("No hay líneas de detalle para este pedido.")
        else:
            for i, row in detalle_df.reset_index(drop=True).iterrows():
                rcols = st.columns([4,2,2,1])
                prod = rcols[0].selectbox(f"Producto {i+1}", list(PRODUCTOS.keys()), index=list(PRODUCTOS.keys()).index(row["Producto"]) if row["Producto"] in PRODUCTOS else 0, key=f"edit_prod_{i}")
                qty = rcols[1].number_input(f"Cantidad {i+1}", min_value=0, step=1, value=int(row["Cantidad"]), key=f"edit_qty_{i}")
                price = PRODUCTOS.get(prod, 0)
                rcols[2].markdown(f"Unitario: {price:,}".replace(",","."))
                # include a checkbox to mark this line to remove
                remove = rcols[3].checkbox("Eliminar", key=f"remove_line_{i}")
                if not remove:
                    edited_items[prod] = edited_items.get(prod, 0) + int(qty)
                else:
                    # if removed, nothing added to edited_items (effectively delete)
                    pass

        # allow adding new lines
        st.markdown("Añadir nuevas líneas")
        new_lines = st.number_input("Agregar N líneas", min_value=0, max_value=8, value=0, key="add_lines")
        if int(new_lines) > 0:
            for j in range(int(new_lines)):
                acols = st.columns([4,2])
                p = acols[0].selectbox(f"Nuevo producto {j+1}", ["-- Ninguno --"] + list(PRODUCTOS.keys()), key=f"add_prod_{j}")
                q = acols[1].number_input(f"Nueva cantidad {j+1}", min_value=0, step=1, value=0, key=f"add_qty_{j}")
                if p and p != "-- Ninguno --" and q > 0:
                    edited_items[p] = edited_items.get(p, 0) + int(q)

        # optional: change domicilio and week
        st.markdown("---")
        domic_opt = st.selectbox("Domicilio", ["No", f"Sí ({DOMICILIO_COST} COP)"], index=0 if header.get("Monto_domicilio",0)==0 else 1)
        new_week = st.number_input("Semana entrega (ISO)", min_value=1, max_value=53, value=int(header.get("Semana_entrega", datetime.now().isocalendar().week)))
        new_state = st.selectbox("Estado", ["Pendiente","Entregado"], index=0 if header.get("Estado","Pendiente")!="Entregado" else 1)

        if st.button("Guardar cambios en pedido"):
            try:
                new_items_dict = edited_items
                if not new_items_dict:
                    st.warning("No hay líneas definidas (pedido quedaría vacío). Si deseas eliminar el pedido, usa la opción eliminar.")
                else:
                    new_domic = True if "Sí" in domic_opt else False
                    edit_order_details(sel_id, new_items_dict, new_domic_bool=new_domic, new_week=new_week)
                    # update other fields if state changed
                    header_idx = df_pedidos.index[df_pedidos["ID Pedido"]==sel_id][0]
                    df_pedidos.at[header_idx, "Estado"] = new_state
                    # recalc saldo if necessary
                    # persist header already done in edit function
                    st.success("Pedido actualizado correctamente")
                    # refresh in memory
                    df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
                    df_pedidos_detalle = safe_load(ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
            except Exception as e:
                st.error(f"Error actualizando pedido: {e}")

        st.markdown("---")
        st.subheader("Eliminar pedido")
        if st.button("Eliminar pedido seleccionado (revertir inventario)"):
            try:
                delete_order_and_revert(sel_id)
                st.success("Pedido eliminado y inventario revertido")
                df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
                df_pedidos_detalle = safe_load(ws_pedidos_detalle, HEAD_PEDIDOS_DETALLE)
                df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
            except Exception as e:
                st.error(f"Error eliminando pedido: {e}")

# ---------- ENTREGAS/PAGOS ----------
elif menu == "Entregas/Pagos":
    st.header("🚚 Entregas y Pagos")
    st.subheader("Listado filtrable")
    estado_choice = st.selectbox("Estado", ["Todos","Pendiente","Entregado"])
    weeks = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    week_opts = ["Todas"] + [str(w) for w in weeks]
    week_filter = st.selectbox("Semana (ISO)", week_opts, index=0)

    df_view = df_pedidos.copy()
    if estado_choice != "Todos":
        df_view = df_view[df_view["Estado"]==estado_choice]
    if week_filter != "Todas":
        df_view = df_view[df_view["Semana_entrega"]==int(week_filter)]
    st.dataframe(df_view.reset_index(drop=True), use_container_width=True)

    st.write("---")
    st.subheader("Selecciona pedido y registra pago")
    if df_view.empty:
        st.info("No hay pedidos en la vista actual")
    else:
        ids = df_view["ID Pedido"].astype(int).tolist()
        selection = st.selectbox("ID Pedido", ids, key="pay_sel")
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==selection][0]
        row = df_pedidos.loc[idx]
        st.markdown(f"**Cliente:** {row['Nombre Cliente']}  \n**Total:** {int(row['Total_pedido']):,} COP  \n**Pagado:** {int(row['Monto_pagado']):,} COP  \n**Saldo:** {int(row['Saldo_pendiente']):,} COP")
        # show detalle lines for that order
        detalle = get_order_details_df(selection)
        if not detalle.empty:
            st.markdown("**Líneas del pedido:**")
            st.table(detalle[["Producto","Cantidad","Precio_unitario","Subtotal"]].set_index(pd.Index(range(1,len(detalle)+1))))
        with st.form("form_payment"):
            amount = st.number_input("Monto a pagar (COP)", min_value=0, step=1000, value=int(row.get("Saldo_pendiente",0)))
            medio = st.selectbox("Medio de pago", ["Efectivo","Transferencia","Nequi","Daviplata"])
            submit_payment = st.form_submit_button("Registrar pago")
            if submit_payment:
                try:
                    res = register_payment(int(selection), medio, amount)
                    st.success(f"Pago registrado: productos {res['prod_paid']} COP, domicilio {res['domicilio_paid']} COP. Saldo restante: {res['saldo_total']} COP")
                    # refresh memory
                    df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
                    df_flujo = safe_load(ws_flujo, HEAD_FLUJO)
                except Exception as e:
                    st.error(f"Error registrando pago: {e}")

# ---------- INVENTARIO ----------
elif menu == "Inventario":
    st.header("📦 Inventario")
    st.dataframe(df_inventario, use_container_width=True)
    with st.expander("Ajustar stock manualmente"):
        if df_inventario.empty:
            st.info("Inventario vacío")
        else:
            prod = st.selectbox("Producto", df_inventario["Producto"].tolist())
            add_q = st.number_input("Sumar cantidad (positivo) o dejar 0", min_value=0, step=1, value=0)
            if st.button("Aplicar ajuste"):
                try:
                    idx = df_inventario.index[df_inventario["Producto"]==prod][0]
                    df_inventario.at[idx, "Stock"] = int(df_inventario.at[idx, "Stock"]) + int(add_q)
                    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
                    st.success("Stock ajustado")
                except Exception as e:
                    st.error(f"Error ajustando inventario: {e}")

    st.markdown("---")
    st.subheader("Unidades vendidas por producto")
    week_opts = ["Todas"] + [str(w) for w in weeks]
    week_sel = st.selectbox("Filtrar por semana (ISO)", week_opts, index=0, key="inv_week")
    if week_sel == "Todas":
        resumen = unidades_vendidas_por_producto()
    else:
        dff = df_pedidos[df_pedidos["Semana_entrega"]==int(week_sel)]
        resumen = unidades_vendidas_por_producto(dff)
    df_unid = pd.DataFrame(list(resumen.items()), columns=["Producto","Unidades vendidas"]).set_index("Producto")
    st.dataframe(df_unid)

# ---------- FLUJO & GASTOS ----------
elif menu == "Flujo & Gastos":
    st.header("💰 Flujo de caja y gastos")
    total_prod, total_dom, total_gastos, saldo = flow_summaries()
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Ingresos productos", f"{int(total_prod):,} COP".replace(",","."))
    c2.metric("Ingresos domicilios", f"{int(total_dom):,} COP".replace(",","."))
    c3.metric("Gastos", f"-{int(total_gastos):,} COP".replace(",","."))
    c4.metric("Saldo disponible", f"{int(saldo):,} COP".replace(",","."))

    st.write("---")
    st.subheader("Totales por medio de pago (según Flujo)")
    by_method = totals_by_payment_method()
    if not by_method:
        st.info("No hay movimientos registrados")
    else:
        df_methods = pd.DataFrame(list(by_method.items()), columns=["Medio_pago","Total_ingresos"]).set_index("Medio_pago")
        st.dataframe(df_methods)

    st.write("---")
    st.subheader("Registrar movimiento entre medios (ej. retiro: Transferencia -> Efectivo)")
    with st.form("form_move"):
        amt = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        from_m = st.selectbox("De (medio)", ["Transferencia","Efectivo","Nequi","Daviplata"])
        to_m = st.selectbox("A (medio)", ["Efectivo","Transferencia","Nequi","Daviplata"])
        note = st.text_input("Nota (opcional)", value="Movimiento interno / Retiro")
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
                    df_flujo = safe_load(ws_flujo, HEAD_FLUJO)
                except Exception as e:
                    st.error(f"Error registrando movimiento: {e}")

    st.write("---")
    st.subheader("Registrar gasto")
    with st.form("form_gasto"):
        concept = st.text_input("Concepto")
        m = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        submit_g = st.form_submit_button("Agregar gasto")
        if submit_g:
            try:
                add_expense(concept, m)
                st.success("Gasto agregado")
                df_gastos = safe_load(ws_gastos, HEAD_GASTOS)
            except Exception as e:
                st.error(f"Error agregando gasto: {e}")

    st.write("---")
    st.subheader("Movimientos recientes en Flujo")
    st.dataframe(df_flujo.tail(200), use_container_width=True)
    st.subheader("Gastos recientes")
    st.dataframe(df_gastos.tail(200), use_container_width=True)

# ---------- CLIENTES ----------
elif menu == "Clientes":
    st.header("Clientes")
    st.dataframe(df_clientes, use_container_width=True)
    with st.form("form_cli"):
        nome = st.text_input("Nombre")
        tel = st.text_input("Teléfono")
        direc = st.text_input("Dirección")
        if st.form_submit_button("Agregar cliente"):
            if not nome:
                st.error("Nombre obligatorio")
            else:
                add_cliente(nome, tel, direc)
                st.success("Cliente agregado")
                df_clientes = safe_load(ws_clientes, HEAD_CLIENTES)

# ---------- REPORTES ----------
elif menu == "Reportes":
    st.header("📊 Reportes")
    st.subheader("Pedidos (completo)")
    st.dataframe(df_pedidos, use_container_width=True)
    st.subheader("Detalle Pedidos")
    st.dataframe(df_pedidos_detalle, use_container_width=True)
    st.subheader("Flujo completo")
    st.dataframe(df_flujo, use_container_width=True)
    st.subheader("Gastos completo")
    st.dataframe(df_gastos, use_container_width=True)

st.write("---")
st.caption("Nota: Se usa la hoja Pedidos (cabecera) y Pedidos_detalle (líneas). Si las escrituras a Google Sheets fallan por permisos, la app seguirá funcionando en memoria pero debes compartir el Sheet con la cuenta de servicio para persistir datos.")
