# andicblue_streamlit_final_cloud.py
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="AndicBlue - Dashboard", page_icon="🫐", layout="wide")
st.title("🫐 AndicBlue - Sistema de Gestión y Dashboard")

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
DOMICILIO_COST = 3000

# ---------------------------
# AUTH & CLIENT
# ---------------------------
if "gcp_service_account" not in st.secrets:
    st.error("⚠️ Falta la sección 'gcp_service_account' en Streamlit Secrets.")
    st.stop()

creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)

# ---------------------------
# HOJAS Y UTILIDADES
# ---------------------------
def open_spreadsheet(name):
    try:
        ss = gc.open(name)
    except gspread.exceptions.APIError:
        st.error(f"No se puede abrir el Sheet '{name}'. Asegúrate de haberlo creado y compartido con la cuenta de servicio.")
        st.stop()
    return ss

def ensure_worksheet(ss, title, headers):
    try:
        ws = ss.worksheet(title)
    except:
        try:
            ss.add_worksheet(title=title, rows="1000", cols="20")
            ws = ss.worksheet(title)
        except gspread.exceptions.APIError:
            st.error(f"No se puede acceder ni crear la hoja '{title}' en el Sheet.")
            st.stop()
    try:
        vals = ws.row_values(1)
    except gspread.exceptions.APIError:
        vals = []
    if not vals or vals[:len(headers)] != headers:
        if ws.row_count >= 1 and any(ws.row_values(1)):
            ws.delete_rows(1)
        ws.insert_row(headers, index=1)
    return ws

ss = open_spreadsheet(SHEET_NAME)

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

ws_clientes = ensure_worksheet(ss, "Clientes", HEAD_CLIENTES)
ws_pedidos = ensure_worksheet(ss, "Pedidos", HEAD_PEDIDOS)
ws_inventario = ensure_worksheet(ss, "Inventario", HEAD_INVENTARIO)
ws_flujo = ensure_worksheet(ss, "FlujoCaja", HEAD_FLUJO)
ws_gastos = ensure_worksheet(ss, "Gastos", HEAD_GASTOS)

# ---------------------------
# CARGA INICIAL DE DATOS CON TRY/EXCEPT
# ---------------------------
def safe_load(ws, headers):
    try:
        df = pd.DataFrame(ws.get_all_records())
        if df.empty:
            df = pd.DataFrame(columns=headers)
    except gspread.exceptions.APIError:
        df = pd.DataFrame(columns=headers)
    return df

df_clientes = safe_load(ws_clientes, HEAD_CLIENTES)
df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
df_flujo = safe_load(ws_flujo, HEAD_FLUJO)
df_gastos = safe_load(ws_gastos, HEAD_GASTOS)

if df_inventario.empty:
    for p in PRODUCTOS.keys():
        ws_inventario.append_row([p, 0])
    df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)

# ---------------------------
# FUNCIONES AUXILIARES
# ---------------------------
def save_df_to_ws(df, ws, headers):
    ws.clear()
    ws.append_row(headers)
    for i, row in df.iterrows():
        ws.append_row(row.tolist())

def next_id(df, col):
    if df.empty or col not in df.columns:
        return 1
    existing = df[col].dropna().astype(int).tolist()
    return max(existing) + 1 if existing else 1

def parse_productos_detalle(detalle_str):
    productos = {}
    if not detalle_str:
        return productos
    items = detalle_str.split(" | ")
    for item in items:
        try:
            nombre_cant = item.split(" x")
            nombre = nombre_cant[0].strip()
            cantidad = int(nombre_cant[1].split(" ")[0])
            productos[nombre] = cantidad
        except:
            continue
    return productos

def add_cliente(nombre, telefono, direccion):
    global df_clientes
    cid = next_id(df_clientes, "ID Cliente")
    df_clientes = pd.concat([df_clientes, pd.DataFrame([[cid, nombre, telefono, direccion]], columns=HEAD_CLIENTES)], ignore_index=True)
    try:
        ws_clientes.append_row([cid, nombre, telefono, direccion])
    except gspread.exceptions.APIError:
        pass
    return cid

def create_order(cliente_id, productos_cant, domicilio_bool, estado_inicial, fecha_entrega=None):
    global df_pedidos, df_inventario, df_flujo
    cliente_nombre = df_clientes.loc[df_clientes["ID Cliente"]==cliente_id, "Nombre"].values[0]

    subtotal = sum(PRODUCTOS[p]*q for p,q in productos_cant.items())
    detalle_str = " | ".join([f"{p} x{q} (@{PRODUCTOS[p]})" for p,q in productos_cant.items() if q>0])
    domicilio_monto = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio_monto

    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = fecha_dt.isocalendar().week

    pid = next_id(df_pedidos, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_pedidos = pd.concat([df_pedidos, pd.DataFrame([[pid, fecha_actual, cliente_id, cliente_nombre,
        detalle_str, subtotal, domicilio_monto, total, estado_inicial, "", 0, subtotal, semana_entrega]], columns=HEAD_PEDIDOS)], ignore_index=True)

    # Actualizar inventario (permite negativo)
    for prod, cant in productos_cant.items():
        if prod in df_inventario["Producto"].values:
            idx = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[idx, "Stock"] = int(df_inventario.at[idx, "Stock"]) - cant
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, -cant]], columns=HEAD_INVENTARIO)], ignore_index=True)

    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
    return pid

def mark_order_delivered(order_id, medio_pago, monto_pagado):
    global df_pedidos, df_flujo
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except IndexError:
        st.error(f"Pedido ID {order_id} no encontrado")
        return

    row = df_pedidos.loc[idx]
    subtotal_products = float(row["Subtotal_productos"])
    domicilio_monto = float(row["Monto_domicilio"])
    monto_anterior = float(row["Monto_pagado"])

    prod_pagado_antes = min(monto_anterior, subtotal_products)
    domicilio_pagado_antes = max(0, monto_anterior - subtotal_products)

    monto_restante = monto_pagado
    prod_now = min(monto_restante, subtotal_products - prod_pagado_antes)
    monto_restante -= prod_now
    domicilio_now = min(monto_restante, domicilio_monto - domicilio_pagado_antes)

    prod_total = prod_pagado_antes + prod_now
    domicilio_total = domicilio_pagado_antes + domicilio_now
    saldo_total = (subtotal_products - prod_total) + (domicilio_monto - domicilio_total)
    monto_total = prod_total + domicilio_total

    df_pedidos.at[idx, "Estado"] = "Entregado" if saldo_total==0 else "Pendiente"
    df_pedidos.at[idx, "Medio_pago"] = medio_pago
    df_pedidos.at[idx, "Monto_pagado"] = monto_total
    df_pedidos.at[idx, "Saldo_pendiente"] = saldo_total

    # Registrar en flujo
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_flujo = pd.concat([df_flujo, pd.DataFrame([[fecha, order_id, row["Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]], columns=HEAD_FLUJO)], ignore_index=True)
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)

    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def add_expense(concepto, monto):
    global df_gastos
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_gastos = pd.concat([df_gastos, pd.DataFrame([[fecha, concepto, monto]], columns=HEAD_GASTOS)], ignore_index=True)
    save_df_to_ws(df_gastos, ws_gastos, HEAD_GASTOS)

# ---------------------------
# INTERFAZ STREAMLIT
# ---------------------------
st.markdown("Aplicación desplegada en Streamlit Cloud — datos guardados en Google Sheets")

menu = st.sidebar.selectbox("Selecciona módulo", ["Clientes", "Pedidos", "Inventario", "Entregas/Pagos", "Flujo & Gastos", "Reportes"])
st.write("---")

# ---------- CLIENTES ----------
if menu=="Clientes":
    st.header("Clientes")
    st.dataframe(df_clientes, use_container_width=True)
    with st.form("form_add_cliente"):
        st.subheader("Agregar nuevo cliente")
        n = st.text_input("Nombre completo")
        t = st.text_input("Teléfono")
        d = st.text_input("Dirección")
        s = st.form_submit_button("Agregar cliente")
        if s:
            if not n:
                st.error("⚠️ Nombre requerido")
            else:
                cid = add_cliente(n, t, d)
                st.success(f"Cliente agregado con ID {cid}")

# ---------- PEDIDOS ----------
elif menu=="Pedidos":
    st.header("Registrar pedido")
    if df_clientes.empty:
        st.warning("No hay clientes registrados.")
    else:
        with st.form("form_new_order"):
            cliente_sel = st.selectbox("Cliente", df_clientes["ID Cliente"].astype(str) + " - " + df_clientes["Nombre"])
            cliente_id = int(cliente_sel.split(" - ")[0])
            productos_cant = {}
            for p, price in PRODUCTOS.items():
                q = st.number_input(f"{p} (COP {price})", min_value=0, step=1, value=0)
                productos_cant[p] = q
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha de entrega")
            submit_order = st.form_submit_button("Registrar pedido")
            if submit_order:
                pid = create_order(cliente_id, productos_cant, domicilio, "Pendiente", fecha_entrega)
                st.success(f"Pedido registrado con ID {pid}")

        st.subheader("Pedidos por semana")
        semana_sel = st.number_input("Semana ISO", min_value=1, max_value=53, value=datetime.now().isocalendar().week)
        df_semana = df_pedidos[df_pedidos["Semana_entrega"]==semana_sel]
        st.dataframe(df_semana)

# ---------- INVENTARIO ----------
elif menu=="Inventario":
    st.header("Inventario")
    st.dataframe(df_inventario, use_container_width=True)
    with st.expander("Actualizar stock"):
        prod_sel = st.selectbox("Producto", df_inventario["Producto"])
        cantidad = st.number_input("Cantidad a sumar", min_value=0, step=1)
        if st.button("Actualizar stock"):
            idx = df_inventario.index[df_inventario["Producto"]==prod_sel][0]
            df_inventario.at[idx, "Stock"] += cantidad
            save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
            st.success("Stock actualizado")

# ---------- ENTREGAS/PAGOS ----------
elif menu=="Entregas/Pagos":
    st.header("Registrar pago / marcar entregado")
    pendientes = df_pedidos[df_pedidos["Estado"]=="Pendiente"]
    if pendientes.empty:
        st.info("No hay pedidos pendientes")
    else:
        pedido_sel = st.selectbox("Selecciona pedido pendiente", pendientes["ID Pedido"])
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==pedido_sel][0]
        row = df_pedidos.loc[idx]
        st.write(f"Cliente: {row['Nombre Cliente']}, Total pendiente: {row['Saldo_pendiente']} COP")
        with st.form("form_pago"):
            monto_pago = st.number_input("Monto a pagar", min_value=0, max_value=row["Saldo_pendiente"], value=int(row["Saldo_pendiente"]))
            medio_pago = st.selectbox("Medio de pago", ["Efectivo", "Transferencia", "Nequi", "Daviplata"])
            submit_pago = st.form_submit_button("Registrar pago")
            if submit_pago:
                result = mark_order_delivered(pedido_sel, medio_pago, monto_pago)
                st.success(f"Pago registrado: productos {result['prod_paid']} COP, domicilio {result['domicilio_paid']} COP. Saldo pendiente: {result['saldo_total']} COP")

# ---------- FLUJO & GASTOS ----------
elif menu=="Flujo & Gastos":
    st.header("Flujo de caja")
    st.dataframe(df_flujo, use_container_width=True)
    st.subheader("Registrar gasto")
    with st.form("form_gasto"):
        concepto = st.text_input("Concepto")
        monto = st.number_input("Monto", min_value=0)
        submit_gasto = st.form_submit_button("Agregar gasto")
        if submit_gasto:
            add_expense(concepto, monto)
            st.success("Gasto agregado")
