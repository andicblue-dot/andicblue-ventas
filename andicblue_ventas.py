# andicblue_streamlit_dashboard_final.py
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import plotly.express as px

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
def open_or_create_spreadsheet(name):
    try:
        ss = gc.open(name)
    except:
        ss = gc.create(name)
    return ss

def ensure_worksheet(ss, title, headers):
    try:
        ws = ss.worksheet(title)
    except:
        ws = ss.add_worksheet(title=title, rows="1000", cols="20")
    try:
        vals = ws.row_values(1)
    except:
        vals = []
    if not vals or vals[:len(headers)] != headers:
        if ws.row_count >= 1 and any(ws.row_values(1)):
            ws.delete_rows(1)
        ws.insert_row(headers, index=1)
    return ws

ss = open_or_create_spreadsheet(SHEET_NAME)

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
# CARGA INICIAL DE DATOS
# ---------------------------
df_clientes = pd.DataFrame(ws_clientes.get_all_records())
df_pedidos = pd.DataFrame(ws_pedidos.get_all_records())
df_inventario = pd.DataFrame(ws_inventario.get_all_records())
df_flujo = pd.DataFrame(ws_flujo.get_all_records())
df_gastos = pd.DataFrame(ws_gastos.get_all_records())

if df_inventario.empty:
    for p in PRODUCTOS.keys():
        ws_inventario.append_row([p, 0])
    df_inventario = pd.DataFrame(ws_inventario.get_all_records())

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
    ws_clientes.append_row([cid, nombre, telefono, direccion])
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

    # Actualizar inventario
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
    idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
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

    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_flujo = pd.concat([df_flujo, pd.DataFrame([[fecha, order_id, row["Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]], columns=HEAD_FLUJO)], ignore_index=True)

    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)
    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def add_expense(concepto, monto):
    global df_gastos
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_gastos = pd.concat([df_gastos, pd.DataFrame([[fecha, concepto, monto]], columns=HEAD_GASTOS)], ignore_index=True)
    ws_gastos.append_row([fecha, concepto, monto])

# ---------------------------
# INTERFAZ STREAMLIT
# ---------------------------
menu = st.sidebar.selectbox("Módulo", ["Clientes", "Pedidos", "Inventario", "Entregas/Pagos", "Flujo & Gastos", "Reportes"])
st.write("---")

# ---------- CLIENTES ----------
if menu=="Clientes":
    st.header("Clientes")
    st.dataframe(df_clientes, use_container_width=True)
    with st.expander("Agregar cliente"):
        n = st.text_input("Nombre")
        t = st.text_input("Teléfono")
        d = st.text_input("Dirección")
        if st.button("Agregar cliente"):
            if n:
                cid = add_cliente(n, t, d)
                st.success(f"Cliente agregado con ID {cid}")
            else:
                st.error("Nombre obligatorio")

# ---------- PEDIDOS ----------
elif menu=="Pedidos":
    st.header("Pedidos")
    if df_clientes.empty:
        st.warning("No hay clientes registrados.")
    else:
        with st.expander("Registrar pedido"):
            cliente_sel = st.selectbox("Cliente", df_clientes["ID Cliente"].astype(str) + " - " + df_clientes["Nombre"])
            cliente_id = int(cliente_sel.split(" - ")[0])
            productos_cant = {p: st.number_input(f"{p} (COP {price})", min_value=0, step=1) for p, price in PRODUCTOS.items()}
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)")
            fecha_entrega = st.date_input("Fecha de entrega")
            if st.button("Registrar pedido"):
                pid = create_order(cliente_id, productos_cant, domicilio, "Pendiente", fecha_entrega)
                st.success(f"Pedido registrado con ID {pid}")

        if not df_pedidos.empty:
            semanas_disponibles = sorted(df_pedidos["Semana_entrega"].unique())
            semana_sel = st.selectbox("Selecciona semana de entrega", semanas_disponibles)
            df_semana = df_pedidos[df_pedidos["Semana_entrega"]==semana_sel]
            st.write(f"Pedidos de la semana {semana_sel}")
            st.dataframe(df_semana, use_container_width=True)

# ---------- INVENTARIO ----------
elif menu=="Inventario":
    st.header("Inventario")
    st.dataframe(df_inventario, use_container_width=True)
    with st.expander("Actualizar stock"):
        prod = st.selectbox("Producto", df_inventario["Producto"])
        nueva = st.number_input("Cantidad a agregar", min_value=0, step=1)
        if st.button("Actualizar stock"):
            idx = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[idx, "Stock"] += nueva
            ws_inventario.update_cell(idx+2,2,df_inventario.at[idx,"Stock"])
            st.success(f"Stock actualizado: {prod} = {df_inventario.at[idx,'Stock']}")

# ---------- ENTREGAS/PAGOS ----------
elif menu=="Entregas/Pagos":
    st.header("Entregas y Pagos")
    filtrar = st.checkbox("Mostrar solo pendientes")
    dfp_display = df_pedidos[df_pedidos["Estado"]=="Pendiente"] if filtrar else df_pedidos
    st.dataframe(dfp_display, use_container_width=True)

    with st.expander("Registrar entrega/pago"):
        idp = st.number_input("ID Pedido", min_value=1, step=1)
        estado = st.selectbox("Estado del pedido", ["Pendiente","Entregado"])
        medio = st.selectbox("Medio de pago", ["Efectivo","Transferencia","Crédito","Pago parcial"])
        monto = st.number_input("Monto pagado (COP)", min_value=0, step=1000)
        if st.button("Registrar pago"):
            try:
                res = mark_order_delivered(idp, medio, monto)
                st.success(f"Pedido {idp} actualizado. Pagos: Productos={res['prod_paid']} / Domicilio={res['domicilio_paid']}, Saldo={res['saldo_total']}")
            except Exception as e:
                st.error(f"Error: {e}")

# ---------- FLUJO & GASTOS ----------
elif menu=="Flujo & Gastos":
    st.header("Flujo y gastos")
    total_prod = df_flujo["Ingreso_productos_recibido"].sum() if not df_flujo.empty else 0
    total_domic = df_flujo["Ingreso_domicilio_recibido"].sum() if not df_flujo.empty else 0
    total_gastos = df_gastos["Monto"].sum() if not df_gastos.empty else 0
    saldo_real = total_prod + total_domic - total_gastos

    st.metric("Ingresos productos", f"{int(total_prod):,} COP".replace(",","."), delta=None)
    st.metric("Ingresos domicilios", f"{int(total_domic):,} COP".replace(",","."), delta=None)
    st.metric("Gastos", f"-{int(total_gastos):,} COP".replace(",","."), delta=None)
    st.metric("Saldo disponible", f"{int(saldo_real):,} COP".replace(",","."), delta=None)

    with st.expander("Agregar gasto"):
        concepto = st.text_input("Concepto")
        monto_g = st.number_input("Monto gasto (COP)", min_value=0, step=1000)
        if st.button("Agregar gasto"):
            add_expense(concepto, monto_g)
            st.success("Gasto agregado")
            st.experimental_rerun()

    st.subheader("Unidades vendidas por producto")
    if not df_pedidos.empty:
        productos_totales = {p:0 for p in PRODUCTOS.keys()}
        for _, r in df_pedidos.iterrows():
            prods = parse_productos_detalle(r["Productos_detalle"])
            for p,q in prods.items():
                if p in productos_totales:
                    productos_totales[p] += q
        df_unidades = pd.DataFrame.from_dict(productos_totales, orient="index", columns=["Unidades vendidas"])
        st.dataframe(df_unidades)

# ---------- REPORTES ----------
elif menu=="Reportes":
    st.header("Reporte de pedidos")
    st.dataframe(df_pedidos, use_container_width=True)

st.caption("Nota: Los domicilios se contabilizan aparte (3000 COP).")
