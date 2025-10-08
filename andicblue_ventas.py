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
DOMICILIO_COST = 3000  # COP

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
    except Exception:
        try:
            ss.add_worksheet(title=title, rows="1000", cols="20")
            ws = ss.worksheet(title)
        except gspread.exceptions.APIError:
            st.error(f"No se puede acceder ni crear la hoja '{title}' en el Sheet.")
            st.stop()
    # asegurar encabezados (si la hoja está vacía o con encabezados distintos)
    try:
        vals = ws.row_values(1)
    except gspread.exceptions.APIError:
        vals = []
    if not vals or vals[:len(headers)] != headers:
        # intentar borrar primera fila si contiene algo
        try:
            if ws.row_count >= 1 and any(ws.row_values(1)):
                ws.delete_rows(1)
        except Exception:
            pass
        try:
            ws.insert_row(headers, index=1)
        except Exception:
            # si falla, no rompemos pero dejamos la hoja como está
            pass
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
    except Exception:
        df = pd.DataFrame(columns=headers)
    return df

df_clientes = safe_load(ws_clientes, HEAD_CLIENTES)
df_pedidos = safe_load(ws_pedidos, HEAD_PEDIDOS)
df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
df_flujo = safe_load(ws_flujo, HEAD_FLUJO)
df_gastos = safe_load(ws_gastos, HEAD_GASTOS)

# Forzar tipos numéricos donde corresponda y rellenar NaN
def coerce_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

coerce_numeric(df_pedidos, ["Subtotal_productos", "Monto_domicilio", "Total_pedido", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"])
coerce_numeric(df_inventario, ["Stock"])
coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"])
coerce_numeric(df_gastos, ["Monto"])

# Inicializar inventario si está vacío
if df_inventario.empty:
    for p in PRODUCTOS.keys():
        try:
            ws_inventario.append_row([p, 0])
        except Exception:
            pass
    df_inventario = safe_load(ws_inventario, HEAD_INVENTARIO)
    coerce_numeric(df_inventario, ["Stock"])

# ---------------------------
# UTILIDADES DE ESCRITURA SEGURA
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
        for _, row in df.iterrows():
            ws.append_row(_row_to_values(row))
    except Exception:
        # No falle la app por errores al escribir en Sheets
        pass

def next_id(df, col):
    if df.empty or col not in df.columns:
        return 1
    existing = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
    return max(existing) + 1 if existing else 1

# ---------------------------
# PARSE Y LÓGICA BASE
# ---------------------------
def parse_productos_detalle(detalle_str):
    productos = {}
    if not detalle_str or pd.isna(detalle_str):
        return productos
    items = str(detalle_str).split(" | ")
    for item in items:
        try:
            # Formato esperado: "Producto x2 (@5000)"
            nombre_cant = item.split(" x")
            nombre = nombre_cant[0].strip()
            cantidad = int(nombre_cant[1].split(" ")[0])
            productos[nombre] = productos.get(nombre, 0) + cantidad
        except Exception:
            continue
    return productos

def add_cliente(nombre, telefono, direccion):
    global df_clientes
    cid = next_id(df_clientes, "ID Cliente")
    new_row = [cid, nombre, telefono, direccion]
    df_clientes = pd.concat([df_clientes, pd.DataFrame([new_row], columns=HEAD_CLIENTES)], ignore_index=True)
    try:
        ws_clientes.append_row(new_row)
    except Exception:
        pass
    return cid

def create_order(cliente_id, productos_cant, domicilio_bool, estado_inicial, fecha_entrega=None):
    global df_pedidos, df_inventario
    # validar cliente
    if df_clientes.empty or cliente_id not in df_clientes["ID Cliente"].astype(int).tolist():
        raise ValueError("ID cliente no encontrado")
    cliente_nombre = df_clientes.loc[df_clientes["ID Cliente"]==cliente_id, "Nombre"].values[0]

    subtotal = 0
    detalle_items = []
    for p, q in productos_cant.items():
        precio = PRODUCTOS.get(p, 0)
        subtotal += precio * int(q)
        if int(q) > 0:
            detalle_items.append(f"{p} x{int(q)} (@{precio})")
    detalle_str = " | ".join(detalle_items)
    domicilio_monto = DOMICILIO_COST if domicilio_bool else 0
    total = subtotal + domicilio_monto

    # semana de entrega
    if fecha_entrega:
        fecha_dt = pd.to_datetime(fecha_entrega)
    else:
        fecha_dt = datetime.now()
    semana_entrega = fecha_dt.isocalendar().week

    pid = next_id(df_pedidos, "ID Pedido")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_row = [pid, fecha_actual, cliente_id, cliente_nombre,
               detalle_str, subtotal, domicilio_monto, total, estado_inicial,
               "", 0, total, semana_entrega]

    df_pedidos = pd.concat([df_pedidos, pd.DataFrame([new_row], columns=HEAD_PEDIDOS)], ignore_index=True)

    # actualizar inventario (permite negativo)
    for prod, cant in productos_cant.items():
        if prod in df_inventario["Producto"].values:
            idx = df_inventario.index[df_inventario["Producto"]==prod][0]
            df_inventario.at[idx, "Stock"] = int(df_inventario.at[idx, "Stock"]) - int(cant)
        else:
            df_inventario = pd.concat([df_inventario, pd.DataFrame([[prod, -int(cant)]], columns=HEAD_INVENTARIO)], ignore_index=True)

    # intentar persistir en Sheets (no falla si hay error)
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)

    # asegurar tipos
    coerce_numeric(df_pedidos, ["Subtotal_productos", "Monto_domicilio", "Total_pedido", "Monto_pagado", "Saldo_pendiente", "Semana_entrega"])
    coerce_numeric(df_inventario, ["Stock"])

    return pid

def mark_order_delivered(order_id, medio_pago, monto_pagado):
    """
    Registra un pago (parcial o total). Desglosa correctamente entre productos y domicilio.
    Registra en df_flujo únicamente lo pagado en esta transacción (prod_now, domicilio_now).
    """
    global df_pedidos, df_flujo
    try:
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==order_id][0]
    except Exception:
        st.error("Pedido no encontrado.")
        return {"prod_paid": 0, "domicilio_paid": 0, "saldo_total": None}

    row = df_pedidos.loc[idx]
    subtotal_products = float(row.get("Subtotal_productos", 0))
    domicilio_monto = float(row.get("Monto_domicilio", 0))
    monto_anterior = float(row.get("Monto_pagado", 0))

    # monto total pagado después de este pago
    nuevo_total_pagado = monto_anterior + float(monto_pagado)

    # calcular total pagado a productos y domicilio (acumulado)
    prod_total_acum = min(nuevo_total_pagado, subtotal_products)
    dom_total_acum = min(max(0, nuevo_total_pagado - subtotal_products), domicilio_monto)

    # cuánto se pagó en esta transacción para cada concepto
    prod_pagado_antes = min(monto_anterior, subtotal_products)
    dom_pagado_antes = max(0, monto_anterior - subtotal_products)
    prod_now = prod_total_acum - prod_pagado_antes
    domicilio_now = dom_total_acum - dom_pagado_antes

    # saldo restante
    saldo_total = (subtotal_products - prod_total_acum) + (domicilio_monto - dom_total_acum)
    monto_total_registrado = prod_total_acum + dom_total_acum

    # actualizar pedido
    df_pedidos.at[idx, "Monto_pagado"] = monto_total_registrado
    df_pedidos.at[idx, "Saldo_pendiente"] = saldo_total
    df_pedidos.at[idx, "Medio_pago"] = medio_pago
    df_pedidos.at[idx, "Estado"] = "Entregado" if saldo_total == 0 else "Pendiente"

    # registrar en flujo lo pagado ahora (solo incrementos)
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_fila = [fecha, order_id, row["Nombre Cliente"], medio_pago, prod_now, domicilio_now, saldo_total]
    df_flujo = pd.concat([df_flujo, pd.DataFrame([new_fila], columns=HEAD_FLUJO)], ignore_index=True)

    # persistir cambios (no falle la app si write falla)
    save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
    save_df_to_ws(df_flujo, ws_flujo, HEAD_FLUJO)

    # asegurar tipos
    coerce_numeric(df_pedidos, ["Monto_pagado", "Saldo_pendiente"])
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido", "Saldo_pendiente_total"])

    return {"prod_paid": prod_now, "domicilio_paid": domicilio_now, "saldo_total": saldo_total}

def add_expense(concepto, monto):
    global df_gastos
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_gastos = pd.concat([df_gastos, pd.DataFrame([[fecha, concepto, monto]], columns=HEAD_GASTOS)], ignore_index=True)
    save_df_to_ws(df_gastos, ws_gastos, HEAD_GASTOS)

# ---------------------------
# FUNCIONES DE CÁLCULO/RESUMEN
# ---------------------------
def flow_summaries():
    # asegura columnas numéricas
    coerce_numeric(df_flujo, ["Ingreso_productos_recibido", "Ingreso_domicilio_recibido"])
    coerce_numeric(df_gastos, ["Monto"])
    total_prod = df_flujo["Ingreso_productos_recibido"].sum() if not df_flujo.empty else 0
    total_domic = df_flujo["Ingreso_domicilio_recibido"].sum() if not df_flujo.empty else 0
    total_gastos = df_gastos["Monto"].sum() if not df_gastos.empty else 0
    saldo = total_prod + total_domic - total_gastos
    return int(total_prod), int(total_domic), int(total_gastos), int(saldo)

def unidades_vendidas_por_producto(df_filter=None):
    df_src = df_filter if df_filter is not None else df_pedidos
    resumen = {p: 0 for p in PRODUCTOS.keys()}
    if df_src is None or df_src.empty:
        return resumen
    for _, r in df_src.iterrows():
        detalle = parse_productos_detalle(r.get("Productos_detalle", ""))
        for prod, cant in detalle.items():
            if prod in resumen:
                try:
                    resumen[prod] += int(cant)
                except:
                    pass
    return resumen

# ---------------------------
# INTERFAZ STREAMLIT
# ---------------------------
st.markdown("Aplicación desplegada en Streamlit Cloud — datos guardados en Google Sheets")

menu = st.sidebar.selectbox("Selecciona módulo", ["Clientes", "Pedidos", "Inventario", "Entregas/Pagos", "Flujo & Gastos", "Reportes"])
st.write("---")

# ---------- CLIENTES ----------
if menu == "Clientes":
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
elif menu == "Pedidos":
    st.header("Registrar y ver pedidos")
    if df_clientes.empty:
        st.warning("No hay clientes registrados.")
    else:
        with st.expander("Registrar nuevo pedido"):
            cliente_sel = st.selectbox("Cliente", df_clientes["ID Cliente"].astype(str) + " - " + df_clientes["Nombre"])
            cliente_id = int(cliente_sel.split(" - ")[0])
            productos_cant = {}
            for p, price in PRODUCTOS.items():
                q = st.number_input(f"{p} (COP {price})", min_value=0, step=1, value=0)
                productos_cant[p] = int(q)
            domicilio = st.checkbox(f"Incluir domicilio ({DOMICILIO_COST} COP)", value=False)
            fecha_entrega = st.date_input("Fecha estimada de entrega")
            if st.button("Registrar pedido"):
                try:
                    pid = create_order(cliente_id, productos_cant, domicilio, "Pendiente", fecha_entrega)
                    st.success(f"Pedido registrado con ID {pid}")
                except Exception as e:
                    st.error(f"No se pudo crear pedido: {e}")

        st.write("---")
        st.subheader("Filtrar pedidos por semana")
        semanas = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
        semanas_display = ["Todas"] + [str(s) for s in semanas]
        semana_sel = st.selectbox("Selecciona semana (ISO)", semanas_display, index=0)
        if semana_sel == "Todas":
            df_display = df_pedidos.copy()
        else:
            df_display = df_pedidos[df_pedidos["Semana_entrega"] == int(semana_sel)]
        st.dataframe(df_display, use_container_width=True)

        st.write("---")
        st.subheader("Resumen por semana")
        if not df_pedidos.empty:
            df_grouped = df_pedidos.groupby("Semana_entrega").agg({"ID Pedido": "count", "Total_pedido": "sum"}).rename(columns={"ID Pedido":"Cantidad de pedidos","Total_pedido":"Total COP"})
            st.dataframe(df_grouped)

# ---------- INVENTARIO ----------
elif menu == "Inventario":
    st.header("Inventario")
    st.dataframe(df_inventario, use_container_width=True)
    with st.expander("Actualizar stock"):
        if df_inventario.empty:
            st.info("Inventario vacío")
        else:
            prod_sel = st.selectbox("Producto", df_inventario["Producto"].tolist())
            cantidad = st.number_input("Cantidad a sumar", min_value=0, step=1)
            if st.button("Actualizar stock"):
                try:
                    idx = df_inventario.index[df_inventario["Producto"]==prod_sel][0]
                    df_inventario.at[idx, "Stock"] = int(df_inventario.at[idx, "Stock"]) + int(cantidad)
                    save_df_to_ws(df_inventario, ws_inventario, HEAD_INVENTARIO)
                    st.success("Stock actualizado")
                except Exception as e:
                    st.error(f"Error actualizando stock: {e}")

# ---------- ENTREGAS/PAGOS ----------
elif menu == "Entregas/Pagos":
    st.header("Entregas y pagos")
    st.subheader("Listado de pedidos (filtrable)")
    # filtros
    estado_op = st.selectbox("Filtrar por estado", ["Todos", "Pendiente", "Entregado"])
    semanas = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    semanas_display = ["Todas"] + [str(s) for s in semanas]
    semana_sel = st.selectbox("Filtrar por semana (ISO)", semanas_display, index=0)

    df_display = df_pedidos.copy()
    if estado_op != "Todos":
        df_display = df_display[df_display["Estado"] == estado_op]
    if semana_sel != "Todas":
        df_display = df_display[df_display["Semana_entrega"] == int(semana_sel)]

    st.dataframe(df_display.reset_index(drop=True), use_container_width=True)

    st.write("---")
    st.subheader("Seleccionar pedido para registrar pago / cambiar estado")
    if df_display.empty:
        st.info("No hay pedidos en la vista actual")
    else:
        # lista de ids disponibles para seleccionar
        ids = df_display["ID Pedido"].astype(int).tolist()
        pedido_sel = st.selectbox("ID Pedido", ids)
        idx = df_pedidos.index[df_pedidos["ID Pedido"]==pedido_sel][0]
        pedido_row = df_pedidos.loc[idx]
        st.markdown(f"**Cliente:** {pedido_row['Nombre Cliente']}  \n**Total pedido:** {pedido_row['Total_pedido']} COP  \n**Monto pagado:** {pedido_row['Monto_pagado']} COP  \n**Saldo pendiente:** {pedido_row['Saldo_pendiente']} COP")
        with st.form("form_pago"):
            monto_pago = st.number_input("Monto a pagar (COP)", min_value=0, step=1000, value=int(pedido_row.get("Saldo_pendiente", 0)))
            medio = st.selectbox("Medio de pago", ["Efectivo", "Transferencia", "Nequi", "Daviplata"])
            estado_nuevo = st.selectbox("Cambiar estado (opcional)", ["Mantener", "Pendiente", "Entregado"])
            submit_pago = st.form_submit_button("Registrar")
            if submit_pago:
                try:
                    if monto_pago > 0:
                        res = mark_order_delivered(int(pedido_sel), medio, monto_pago)
                        st.success(f"Pago registrado. Productos: {res['prod_paid']} COP, Domicilio: {res['domicilio_paid']} COP. Nuevo saldo: {res['saldo_total']} COP")
                    if estado_nuevo != "Mantener":
                        df_pedidos.at[idx, "Estado"] = estado_nuevo
                        save_df_to_ws(df_pedidos, ws_pedidos, HEAD_PEDIDOS)
                        st.success(f"Estado del pedido actualizado a: {estado_nuevo}")
                except Exception as e:
                    st.error(f"Error: {e}")

# ---------- FLUJO & GASTOS ----------
elif menu == "Flujo & Gastos":
    st.header("Flujo de caja y gastos")

    total_prod, total_dom, total_gastos, saldo_real = flow_summaries()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ingresos productos", f"{total_prod:,} COP".replace(",", "."))
    col2.metric("Ingresos domicilios", f"{total_dom:,} COP".replace(",", "."))
    col3.metric("Gastos", f"-{total_gastos:,} COP".replace(",", "."))
    col4.metric("Saldo disponible", f"{saldo_real:,} COP".replace(",", "."))

    with st.expander("Agregar gasto"):
        concepto = st.text_input("Concepto")
        monto = st.number_input("Monto (COP)", min_value=0, step=1000)
        if st.button("Agregar gasto"):
            add_expense(concepto, monto)
            st.success("Gasto agregado")
            # refrescar variables en memoria (carga mínima)
            df_gastos_local = safe_load(ws_gastos, HEAD_GASTOS)
            coerce_numeric(df_gastos_local, ["Monto"])

    st.write("---")
    st.subheader("Unidades vendidas por producto")
    semanas = sorted(df_pedidos["Semana_entrega"].dropna().astype(int).unique().tolist()) if not df_pedidos.empty else []
    semanas_display = ["Todas"] + [str(s) for s in semanas]
    semana_sel = st.selectbox("Filtrar unidades por semana (ISO)", semanas_display, index=0, key="unidades_semana")

    if semana_sel == "Todas":
        resumen = unidades_vendidas_por_producto()
    else:
        df_filtrado = df_pedidos[df_pedidos["Semana_entrega"] == int(semana_sel)]
        resumen = unidades_vendidas_por_producto(df_filtrado)

    df_resumen = pd.DataFrame(list(resumen.items()), columns=["Producto", "Unidades vendidas"])
    st.dataframe(df_resumen.set_index("Producto"))

    st.write("---")
    st.subheader("Últimos movimientos de flujo")
    st.dataframe(df_flujo.tail(50), use_container_width=True)

    st.subheader("Últimos gastos")
    st.dataframe(df_gastos.tail(50), use_container_width=True)

# ---------- REPORTES ----------
elif menu == "Reportes":
    st.header("Reportes completos")
    st.subheader("Pedidos (completo)")
    st.dataframe(df_pedidos, use_container_width=True)
    st.subheader("Flujo (completo)")
    st.dataframe(df_flujo, use_container_width=True)
    st.subheader("Gastos (completo)")
    st.dataframe(df_gastos, use_container_width=True)

st.write("---")
st.caption("Nota: El valor del domicilio es fijo (3000 COP). Los pedidos se pueden registrar aun cuando el stock quede en cero o negativo; una actualización de inventario posterior puede corregirlo.")
