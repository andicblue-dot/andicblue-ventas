# andicblue_enterprise.py
# Versión final para Streamlit Cloud
# Funcionalidades:
# - CRUD completo de pedidos (crear, editar campos básicos, eliminar con ajuste de inventario)
# - Pagos parciales / totales discriminando productos y domicilio (domicilio máximo fijo)
# - Filtrado por semana de entrega y listado filtrable en Entregas/Pagos
# - Flujo de caja con totales por medio de pago y posibilidad de mover fondos (ej. Transferencia -> Efectivo)
# - Registro de retiros (como movimiento entre medios)
# - Resumen de unidades vendidas por producto (con filtro por semana)
# - Manejo robusto de errores (try/except) al leer/escribir Google Sheets para que la app no se caiga

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="AndicBlue - Gestión Empresarial", page_icon="🫐", layout="wide")
st.title("🫐 AndicBlue — Gestión de Pedidos, Flujo e Inventario (Enterprise)")

# ---------------------------
# CONFIG
# ---------------------------
SHEET_NAME = "andicblue_pedidos"
PRODUCTOS = {
    "Docena de Arándanos 125g": 52500,
    "Arandanos 125g": 5000,
    "Arandanos 250g": 10000,
    "Arandanos 500g": 20000,
    "Kilo industrial": 30000,
    "Mermelada azucar": 16000,
    "Mermelada sin azucar": 20000,
}
DOMICILIO_COST = 3000  # COP fijo por pedido con domicilio

# ---------------------------
# AUTHENTICATION (Streamlit Secrets must include gcp_service_account JSON)
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
# HOJAS / HEADERS
# ---------------------------
HEAD_CLIENTES = ["ID Cliente", "Nombre", "Telefono", "Direccion"]
HEAD_PEDIDOS = [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente", "Productos_detalle",
    "Subtotal_productos", "Monto_domicilio", "Total_pedido", "Estado",
    "Medio_pago", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"
]
HEAD_INVENTARIO = ["Producto", "Stock"]
HEAD_FLUJO = [
    "Fecha", "ID Pedido", "Cliente", "Medio_pago",
    "Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"
]
HEAD_GASTOS = ["Fecha", "Concepto", "Monto"]

# ---------------------------
# UTILIDADES DE HOJAS (seguros)
# ---------------------------
def open_spreadsheet(name):
    try:
        ss = gc.open(name)
    except Exception:
        st.error(f"No se puede abrir el Sheet '{name}'. Asegúrate de haberlo creado y compartido con la cuenta de servicio.")
        st.stop()
    return ss

def ensure_worksheet(ss, title, headers):
    """Asegura que exista la worksheet y sus headers (no crea spreadsheet)."""
    try:
        ws = ss.worksheet(title)
    except Exception:
        # intentar crear la pestaña si el spreadsheet lo permite
        try:
            ss.add_worksheet(title=title, rows="1000", cols="20")
            ws = ss.worksheet(title)
        except Exception:
            st.error(f"No se puede acceder ni crear la hoja '{title}'. Revisa permisos.")
            st.stop()
    # asegurar encabezados
    try:
        vals = ws.row_values(1)
    except Exception:
        vals = []
    if not vals or vals[:len(headers)] != headers:
        try:
            if ws.row_count >= 1 and any(ws.row_values(1)):
                ws.delete_rows(1)
        except Exception:
            pass
        try:
            ws.insert_row(headers, index=1)
        except Exception:
            pass
    return ws

ss = open_spreadsheet(SHEET_NAME)
ws_clientes = ensure_worksheet(ss, "Clientes", HEAD_CLIENTES)
ws_pedidos = ensure_worksheet(ss, "Pedidos", HEAD_PEDIDOS)
ws_inventario = ensure_worksheet(ss, "Inventario", HEAD_INVENTARIO)
ws_flujo = ensure_worksheet(ss, "FlujoCaja", HEAD_FLUJO)
ws_gastos = ensure_worksheet(ss, "Gastos", HEAD_GASTOS)

# ---------------------------
# CARGA INICIAL (con try/except que previene crash)
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
df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
df_flujo = safe_load(ws_flujo, HEAD_FLUJO)
df_gastos = safe_load(ws_gastos, HEAD_GASTOS)

# normalizar tipos numéricos
def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

coerce_numeric(df_pedidos, ["Subtotal_productos", "Monto_domicilio", "Total_pedido", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"])
coerce_numeric(df_inventario, ["Stock"])
coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"])
coerce_numeric(df_gastos, ["Monto"])

# inicializar inventario si está vacío (no falla si write falla)
if df_inventario.empty:
    for p in PRODUCTOS.keys():
        try:
            ws_inventario.append_row([p, 0])
        except Exception:
            pass
    df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
    coerce_numeric(df_inventario, ["Stock"])

# ---------------------------
# ESCRITURA SEGURA
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
    """Escribe todo el df a la hoja; protegemos con try/except para que no falle la app."""
    try:
        ws.clear()
        ws.append_row(headers)
        for _, r in df.iterrows():
            ws.append_row(_row_to_values(r))
    except Exception:
        # no romper la app si falla escritura
        pass

def next_id(df, col):
    if df.empty or col not in df.columns:
        return 1
    existing = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
    return max(existing) + 1 if existing else 1

# ---------------------------
# PARSERS / LÓGICA
# ---------------------------
def parse_productos_detalle(detalle_str):
    """Parsea el campo Productos_detalle en dict {producto: cantidad}."""
    productos = {}
    if not detalle_str or pd.isna(detalle_str):
        return productos
    items = str(detalle_str).split(" | ")
    for item in items:
        try:
            # Formato esperado: "Nombre x2 (@5000)"
            nombre_cant = item.split(" x")
            nombre = nombre_cant[0].strip()
            cantidad = int(nombre_cant[1].split(" ")[0])
            productos[nombre] = productos.get(nombre, 0) + cantidad
        except Exception:
            continue
    return productos

def build_detalle_from_dict(d):
    parts = []
    for prod, cant in d.items():
        precio = PRODUCTOS.get(prod, 0)
        parts.append(f"{prod} x{int(cant)} (@{precio})")
    return " | ".join(parts)

# ---------------------------
# FUNCIONES PRINCIPALES
# ---------------------------
def add_cliente(nombre, telefono, direccion):
    global df_clientes
    cid = next_id(df_clientes, "ID Cliente")
    nuevo = [cid, nombre, telefono, direccion]
    df_clientes = pd.concat([df_clientes, pd.DataFrame([nuevo], columns=HEAD_CLIENTES)], ignore_index=True)
    try:
        ws_clientes.append_row(nuevo)
    except Exception:
        pass
    return cid

def create_order(cliente_id, productos_cant, domicilio_bool, estado_inicial="Pendiente", fecha_entrega=None):
    """Crea pedido, actualiza inventario (permite negativo) y guarda en Sheets."""
    global df_pedidos, df_inventario
    if df_clientes.empty or cliente_id not in df_clientes["ID Cliente"].astype(int).tolist():
        raise ValueError("ID cliente no encontrado")
    cliente_nombre = df_clientes.loc[df_clientes["ID Cliente"]==cliente_id, "Nombre"].values[0]

    subtotal = sum(PRODUCTOS.get(p, 0) * int(q) for p,q in productos_cant.items())
    detalle_str = build_detalle_from_dict({p:int(q) for p,q in productos_cant.items() if int(q)>0})
    domicilio_monto = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio_monto

    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = int(fecha_dt.isocalendar().week)

    pid = next_id(df_pedidos, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_row = [pid, fecha_actual, cliente_id, cliente_nombre, detalle_str,
               subtotal, domicilio_monto, total, estado_inicial, "", 0, total, semana_entrega]

    df_pedidos = pd.concat([df_pedidos, pd.DataFrame([new_row], columns=HEAD_PEDIDOS)], ignore_index=True)

    # actualizar inventario (sumar negativo)
    for prod, cant in productos_cant.items():
        cant = int(cant)
        if prod in df_inventario["Producto"].values:
            idx = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[idx, "Stock"] = int(df_inventario.at[idx, "Stock"]) - cant
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, -cant]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # persistir (no explota si falla)
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
    coerce_numeric(df_pedidos, ["Subtotal_productos", "Monto_domicilio", "Total_pedido", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"])
    coerce_numeric(df_inventario, ["Stock"])
    return pid

def edit_order(order_id, updates: dict):
    """
    updates: diccionario con columnas a actualizar en df_pedidos.
    Si se modifica Productos_detalle, se ajusta inventario (se revierte cantidades antiguas y se aplica nuevas).
    """
    global df_pedidos, df_inventario
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except Exception:
        raise ValueError("Pedido no encontrado")

    # Si se cambia detalle de productos, revertir inventario antiguo y aplicar nuevo
    if "Productos_detalle" in updates:
        old_det = parse_productos_detalle(df_pedidos.at[idx, "Productos_detalle"])
        new_det = parse_productos_detalle(updates["Productos_detalle"])
        # revertir: sumar de vuelta las cantidades del pedido antiguo
        for prod, cant in old_det.items():
            if prod in df_inventario["Producto"].values:
                i = df_inventario.index[df_inventario["Producto"]==prod][0]
                df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) + int(cant)
        # aplicar: restar nuevas cantidades
        for prod, cant in new_det.items():
            if prod in df_inventario["Producto"].values:
                i = df_inventario.index[df_inventario["Producto"]==prod][0]
                df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) - int(cant)
            else:
                df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, -int(cant)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # aplicar otras actualizaciones simples
    for k, v in updates.items():
        if k in df_pedidos.columns:
            df_pedidos.at[idx, k] = v

    # si cambian subtotal/total, asegurarse de coherencia: si cambia subtotal, ajustar Total_pedido = subtotal + domicilio
    if "Subtotal_productos" in updates:
        subtotal = float(updates["Subtotal_productos"])
        domicilio = float(df_pedidos.at[idx, "Monto_domicilio"])
        df_pedidos.at[idx, "Total_pedido"] = subtotal + domicilio
        df_pedidos.at[idx, "Saldo_pendiente"] = df_pedidos.at[idx, "Total_pedido"] - df_pedidos.at[idx, "Monto_pagado"]

    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
    coerce_numeric(df_pedidos, ["Subtotal_productos", "Monto_domicilio", "Total_pedido", "Monto_pagado", "Saldo_pendiente"])

def delete_order(order_id):
    """Elimina pedido y revierte inventario (suma las cantidades vendidas de vuelta)."""
    global df_pedidos, df_inventario
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except Exception:
        raise ValueError("Pedido no encontrado")

    row = df_pedidos.loc[idx]
    detalle = parse_productos_detalle(row.get("Productos_detalle", ""))
    # revertir inventario
    for prod, cant in detalle.items():
        if prod in df_inventario["Producto"].values:
            i = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[i, "Stock"] = int(df_inventario.at[i, "Stock"]) + int(cant)
        else:
            # no existía en inventario, agregar con la cantidad devuelta
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, int(cant)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # eliminar fila
    df_pedidos = df_pedidos[df_pedidos["ID Pedido"] != order_id].reset_index(drop=True)

    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)

def mark_order_paid_partial(order_id, medio_pago, monto_pagado):
    """Wrapper que llama a mark_order_delivered (mismo comportamiento)"""
    return mark_order_delivered(order_id, medio_pago, monto_pagado)

def mark_order_delivered(order_id, medio_pago, monto_pagado):
    """
    Registra pago (parcial o total). Calcula cuánto se abona a productos y domicilio,
    actualiza pedido y agrega fila en flujo con lo pagado en esta operación.
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

    monto_pagado = float(monto_pagado)
    nuevo_total_pagado = monto_anterior + monto_pagado

    # Totales acumulados por concepto
    prod_total_acum = min(nuevo_total_pagado, subtotal_products)
    dom_total_acum = min(max(0, nuevo_total_pagado - subtotal_products), domicilio_monto)

    # Lo ya pagado antes
    prod_pagado_antes = min(monto_anterior, subtotal_products)
    dom_pagado_antes = max(0, monto_anterior - subtotal_products)

    # Lo pagado en esta transacción
    prod_now = max(0, prod_total_acum - prod_pagado_antes)
    domicilio_now = max(0, dom_total_acum - dom_pagado_antes)

    saldo_total = (subtotal_products - prod_total_acum) + (domicilio_monto - dom_total_acum)
    monto_total_registrado = prod_total_acum + dom_total_acum

    # Actualizar pedido
    df_pedidos.at[idx, "Monto_pagado"] = monto_total_registrado
    df_pedidos.at[idx, "Saldo_pendiente"] = saldo_total
    df_pedidos.at[idx, "Medio_pago"] = medio_pago
    df_pedidos.at[idx, "Estado"] = "Entregado" if saldo_total == 0 else "Pendiente"

    # Registrar en flujo solo el monto efectivo de esta transacción (separamos producto y domicilio)
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_flow = [fecha, order_id, row["Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]
    df_flujo = pd.concat([df_flujo, pd.DataFrame([new_flow], columns=HEAD_FLUJO)], ignore_index=True)

    # Guardar (no fallar si write falla)
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)

    coerce_numeric(df_pedidos, ["Monto_pagado", "Saldo_pendiente"])
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def add_expense(concepto, monto):
    global df_gastos
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_gastos = pd.concat([df_gastos, pd.DataFrame([[fecha, concepto, monto]], columns=HEAD_GASTOS)], ignore_index=True)
    save_df_to_ws(df_gastos, ws_gastos, HEAD_GASTOS)

def move_funds_between_methods(amount, from_method, to_method, note="Movimiento"):
    """
    Mueve 'amount' de from_method a to_method registrando en flujo dos filas:
    - Una fila negativa para from_method
    - Una fila positiva para to_method
    Esto permite reflejar retiros / transferencias internas en los totales por medio de pago.
    """
    global df_flujo
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # negativa (resta del medio origen)
    neg_row = [fecha, 0, note + f" ({from_method} -> {to_method})", from_method, -float(amount), 0, 0]
    pos_row = [fecha, 0, note + f" ({from_method} -> {to_method})", to_method, float(amount), 0, 0]
    df_flujo = pd.concat([df_flujo, pd.DataFrame([neg_row], columns=HEAD_FLUJO)], ignore_index=True)
    df_flujo = pd.concat([df_flujo, pd.DataFrame([pos_row], columns=HEAD_FLUJO)], ignore_index=True)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])

# ---------------------------
# RESÚMENES Y REPORTES
# ---------------------------
def flow_summaries():
    # Totales por columna
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    coerce_numeric(df_gastos, ["Monto"])
    total_prod = df_flujo["Ingreso_productos_recibido"].sum() if not df_flujo.empty else 0
    total_dom = df_flujo["Ingreso_domicilio_recibido"].sum() if not df_flujo.empty else 0
    total_gastos = df_gastos["Monto"].sum() if not df_gastos.empty else 0
    saldo = total_prod + total_dom - total_gastos
    return float(total_prod), float(total_dom), float(total_gastos), float(saldo)

def totals_by_payment_method():
    """Suma totales (productos+domicilio) por Medio_pago usando df_flujo."""
    if df_flujo.empty:
        return {}
    df = df_flujo.copy()
    df["total_ingreso"] = pd.to_numeric(df["Ingreso_productos_recibido"], errors="coerce").fillna(0) + pd.to_numeric(df["Ingreso_domicilio_recibido"], errors="coerce").fillna(0)
    grouped = df.groupby("Medio_pago")["total_ingreso"].sum().to_dict()
    # limpiar llaves nan
    grouped = {str(k): float(v) for k,v in grouped.items() if str(k).strip() != ""}
    return grouped

def unidades_vendidas_por_producto(df_filter=None):
    df_src = df_filter if df_filter is not None else df_pedidos
    resumen = {p: 0 for p in PRODUCTOS.keys()}
    if df_src is None or df_src.empty:
        return resumen
    for _, r in df_src.iterrows():
        detalle = parse_productos_detalle(r.get("Productos_detalle", ""))
        for prod, cant in detalle.items():
            if prod in resumen:
                resumen[prod] += int(cant)
    return resumen

# ---------------------------
# INTERFAZ STREAMLIT
# ---------------------------
st.markdown("Aplicación desplegada en Streamlit Cloud — datos guardados en Google Sheets")

menu = st.sidebar.selectbox("Módulo", ["Pedidos", "Entregas/Pagos", "Inventario", "Flujo & Gastos", "Clientes", "Reportes"])
st.write("---")

# ---------- PEDIDOS ----------
if menu == "Pedidos":
    st.header("📦 Pedidos — crear / editar / eliminar")
    with st.expander("Registrar nuevo pedido"):
        if df_clientes.empty:
            st.warning("No hay clientes registrados. Ve a Clientes para agregar clientes.")
        else:
            cliente_sel = st.selectbox("Cliente", df_clientes["ID Cliente"].astype(str) + " - " + df_clientes["Nombre"])
            cliente_id = int(cliente_sel.split(" - ")[0])
            productos_cant = {}
            cols = st.columns(2)
            # inputs product
            for p, price in PRODUCTOS.items():
                with cols[0] if list(PRODUCTOS.keys()).index(p) % 2 == 0 else cols[1]:
                    q = st.number_input(f"{p} (COP {price})", min_value=0, step=1, value=0, key=f"new_{p}")
                    productos_cant[p] = int(q)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada de entrega", value=datetime.now())
            if st.button("Registrar pedido"):
                try:
                    pid = create_order(cliente_id, productos_cant, domicilio, "Pendiente", fecha_entrega)
                    st.success(f"Pedido creado con ID {pid}")
                except Exception as e:
                    st.error(f"No se pudo crear pedido: {e}")

    st.write("---")
    st.subheader("Pedidos (filtrar / editar / eliminar)")
    # filtros
    estado_opts = ["Todos", "Pendiente", "Entregado"]
    estado_filtro = st.selectbox("Filtrar por estado", estado_opts, index=0)
    semanas = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    semana_opts = ["Todas"] + [str(s) for s in semanas]
    semana_sel = st.selectbox("Filtrar por semana (ISO)", semana_opts, index=0)

    df_display = df_pedidos.copy()
    if estado_filtro != "Todos":
        df_display = df_display[df_display["Estado"] == estado_filtro]
    if semana_sel != "Todas":
        df_display = df_display[df_display["Semana_entrega"] == int(semana_sel)]
    st.dataframe(df_display.reset_index(drop=True), use_container_width=True)

    if not df_display.empty:
        st.write("Selecciona un pedido para editar o eliminar")
        ids = df_display["ID Pedido"].astype(int).tolist()
        sel_id = st.selectbox("ID Pedido", ids, key="edit_select")
        sel_idx = df_pedidos.index[df_pedidos["ID Pedido"]==sel_id][0]
        sel_row = df_pedidos.loc[sel_idx]

        st.markdown("**Detalles actuales del pedido seleccionado**")
        st.write(sel_row.to_dict())

        st.write("---")
        st.subheader("Editar pedido seleccionado")
        with st.form("form_edit_order"):
            new_estado = st.selectbox("Estado", ["Pendiente", "Entregado"], index=0 if sel_row["Estado"]!="Entregado" else 1)
            new_medio = st.selectbox("Medio de pago", ["", "Efectivo", "Transferencia", "Nequi", "Daviplata"], index=0)
            new_monto_pagado = st.number_input("Monto pagado (COP)", min_value=0, step=1000, value=int(sel_row.get("Monto_pagado", 0)))
            # permitir editar detalle como texto (recomendado editar cantidades manualmente)
            new_detalle_text = st.text_area("Productos_detalle (formato: 'Prod x2 (@precio) | ...')", value=str(sel_row.get("Productos_detalle","")))
            submit_edit = st.form_submit_button("Guardar cambios")
            if submit_edit:
                updates = {}
                updates["Estado"] = new_estado
                if new_medio:
                    updates["Medio_pago"] = new_medio
                updates["Monto_pagado"] = float(new_monto_pagado)
                # recalcular saldo pendiente
                try:
                    subtotal = float(sel_row.get("Subtotal_productos", 0))
                    domicilio_monto = float(sel_row.get("Monto_domicilio", 0))
                    total = subtotal + domicilio_monto
                    updates["Saldo_pendiente"] = float(total) - float(new_monto_pagado)
                except Exception:
                    updates["Saldo_pendiente"] = sel_row.get("Saldo_pendiente", 0)
                updates["Productos_detalle"] = new_detalle_text
                # aplicar edit
                try:
                    edit_order(sel_id, updates)
                    st.success("Pedido actualizado")
                except Exception as e:
                    st.error(f"No se pudo actualizar pedido: {e}")

        st.write("---")
        st.subheader("Eliminar pedido seleccionado")
        with st.form("form_delete_order"):
            confirm = st.checkbox("Confirmo eliminación del pedido y reversión de inventario")
            if st.form_submit_button("Eliminar pedido"):
                if confirm:
                    try:
                        delete_order(sel_id)
                        st.success("Pedido eliminado y inventario ajustado")
                    except Exception as e:
                        st.error(f"No se pudo eliminar pedido: {e}")
                else:
                    st.warning("Debes confirmar para eliminar")

# ---------- ENTREGAS / PAGOS ----------
elif menu == "Entregas/Pagos":
    st.header("🚚 Entregas y Pagos")
    st.subheader("Listado de pedidos (filtrable)")
    estado_filtro = st.selectbox("Filtrar por estado", ["Todos", "Pendiente", "Entregado"])
    semanas = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    semana_opts = ["Todas"] + [str(s) for s in semanas]
    semana_sel = st.selectbox("Filtrar por semana (ISO)", semana_opts)

    df_view = df_pedidos.copy()
    if estado_filtro != "Todos":
        df_view = df_view[df_view["Estado"] == estado_filtro]
    if semana_sel != "Todas":
        df_view = df_view[df_view["Semana_entrega"] == int(semana_sel)]
    st.dataframe(df_view.reset_index(drop=True), use_container_width=True)

    st.write("---")
    st.subheader("Registrar pago / cambiar estado")
    if df_view.empty:
        st.info("No hay pedidos en la vista actual")
    else:
        pedido_ids = df_view["ID Pedido"].astype(int).tolist()
        sel = st.selectbox("Selecciona ID Pedido", pedido_ids, key="pay_select")
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==sel][0]
        row = df_pedidos.loc[idx]
        st.markdown(f"**Cliente:** {row['Nombre Cliente']}  \n**Total pedido:** {row['Total_pedido']} COP  \n**Monto pagado:** {row['Monto_pagado']} COP  \n**Saldo pendiente:** {row['Saldo_pendiente']} COP")
        with st.form("form_pay"):
            monto = st.number_input("Monto a pagar (COP)", min_value=0, step=1000, value=int(row.get("Saldo_pendiente", 0)))
            medio = st.selectbox("Medio de pago", ["Efectivo", "Transferencia", "Nequi", "Daviplata"])
            nuevo_estado = st.selectbox("Opcional: Cambiar estado", ["Mantener", "Pendiente", "Entregado"])
            submit = st.form_submit_button("Registrar pago / actualizar")
            if submit:
                try:
                    if monto > 0:
                        res = mark_order_delivered(int(sel), medio, monto)
                        st.success(f"Pago registrado. Productos: {res['prod_paid']} COP; Domicilio: {res['domicilio_paid']} COP; Saldo: {res['saldo_total']} COP")
                    if nuevo_estado != "Mantener":
                        df_pedidos.at[idx, "Estado"] = nuevo_estado
                        save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
                        st.success(f"Estado actualizado a: {nuevo_estado}")
                except Exception as e:
                    st.error(f"Error al registrar pago: {e}")

# ---------- INVENTARIO ----------
elif menu == "Inventario":
    st.header("📦 Inventario")
    st.dataframe(df_inventario, use_container_width=True)
    st.write("---")
    st.subheader("Ajustar stock manualmente")
    if df_inventario.empty:
        st.info("Inventario vacío")
    else:
        prod_sel = st.selectbox("Producto", df_inventario["Producto"].tolist())
        adj = st.number_input("Cantidad a sumar (use 0 si no desea cambiar)", min_value=0, step=1)
        if st.button("Actualizar stock"):
            try:
                idx = df_inventario.index[df_inventario["Producto"]==prod_sel][0]
                df_inventario.at[idx, "Stock"] = int(df_inventario.at[idx, "Stock"]) + int(adj)
                save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
                st.success("Stock actualizado")
            except Exception as e:
                st.error(f"Error actualizando stock: {e}")

    st.write("---")
    st.subheader("Resumen de unidades vendidas por producto")
    semanas = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    semana_opts = ["Todas"] + [str(s) for s in semanas]
    semana_sel = st.selectbox("Filtrar por semana (ISO)", semana_opts, key="inv_unidades")
    if semana_sel == "Todas":
        resumen = unidades_vendidas_por_producto()
    else:
        df_filtro = df_pedidos[df_pedidos["Semana_entrega"] == int(semana_sel)]
        resumen = unidades_vendidas_por_producto(df_filtro)
    df_unidades = pd.DataFrame(list(resumen.items()), columns=["Producto", "Unidades vendidas"]).set_index("Producto")
    st.dataframe(df_unidades)

# ---------- FLUJO & GASTOS ----------
elif menu == "Flujo & Gastos":
    st.header("💰 Flujo de caja y Gastos")
    total_prod, total_dom, total_gastos, saldo_real = flow_summaries()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ingresos productos", f"{int(total_prod):,} COP".replace(",", "."))
    col2.metric("Ingresos domicilios", f"{int(total_dom):,} COP".replace(",", "."))
    col3.metric("Gastos", f"-{int(total_gastos):,} COP".replace(",", "."))
    col4.metric("Saldo disponible", f"{int(saldo_real):,} COP".replace(",", "."))

    st.write("---")
    st.subheader("Totales por medio de pago (según Flujo)")
    by_method = totals_by_payment_method()
    if not by_method:
        st.info("No hay movimientos registrados en Flujo")
    else:
        df_methods = pd.DataFrame(list(by_method.items()), columns=["Medio_pago", "Total_ingresos"]).set_index("Medio_pago")
        st.dataframe(df_methods)

    st.write("---")
    st.subheader("Registrar movimiento entre medios (ej. retiro: Transferencia -> Efectivo)")
    with st.form("form_move_funds"):
        amt = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        from_m = st.selectbox("De (medio)", ["Transferencia", "Efectivo", "Nequi", "Daviplata"])
        to_m = st.selectbox("A (medio)", ["Efectivo", "Transferencia", "Nequi", "Daviplata"])
        note = st.text_input("Nota (opcional)", value="Movimiento interno / Retiro")
        submit_move = st.form_submit_button("Registrar movimiento")
        if submit_move:
            if amt <= 0:
                st.error("Monto debe ser mayor a 0")
            elif from_m == to_m:
                st.error("Selecciona medios diferentes")
            else:
                try:
                    move_funds_between_methods(amt, from_m, to_m, note)
                    st.success(f"Movimiento registrado: {amt} COP de {from_m} a {to_m}")
                except Exception as e:
                    st.error(f"Error registrando movimiento: {e}")

    st.write("---")
    st.subheader("Registrar gasto")
    with st.form("form_add_expense"):
        concepto = st.text_input("Concepto gasto")
        monto = st.number_input("Monto (COP)", min_value=0.0, step=1000.0)
        submit_gasto = st.form_submit_button("Agregar gasto")
        if submit_gasto:
            try:
                add_expense(concepto, monto)
                st.success("Gasto agregado")
            except Exception as e:
                st.error(f"Error agregando gasto: {e}")

    st.write("---")
    st.subheader("Movimientos recientes de Flujo")
    st.dataframe(df_flujo.tail(100), use_container_width=True)

    st.subheader("Gastos recientes")
    st.dataframe(df_gastos.tail(100), use_container_width=True)

# ---------- CLIENTES ----------
elif menu == "Clientes":
    st.header("Clientes")
    st.dataframe(df_clientes, use_container_width=True)
    with st.form("form_add_client"):
        n = st.text_input("Nombre")
        t = st.text_input("Teléfono")
        d = st.text_input("Dirección")
        if st.form_submit_button("Agregar cliente"):
            if not n:
                st.error("Nombre es obligatorio")
            else:
                add_cliente(n, t, d)
                st.success("Cliente agregado")

# ---------- REPORTES ----------
elif menu == "Reportes":
    st.header("📊 Reportes")
    st.subheader("Pedidos (completo)")
    st.dataframe(df_pedidos, use_container_width=True)
    st.subheader("Flujo (completo)")
    st.dataframe(df_flujo, use_container_width=True)
    st.subheader("Gastos (completo)")
    st.dataframe(df_gastos, use_container_width=True)

st.write("---")
st.caption("Nota: El domicilio tiene un valor fijo de 3000 COP. Los pedidos pueden registrarse aunque el stock quede negativo; actualiza inventario cuando recolectes más producto.")
