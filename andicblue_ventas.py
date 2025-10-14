# ==============================================================
# ANDICBLUE VENTAS APP - VERSIÓN COMPLETA (Parte A)
# ==============================================================
# Dashboard Azul Moderno - Profesional y Visual
# ==============================================================
# Incluye:
#  - Control de pedidos, inventario, flujo de caja y reportes
#  - Tarjetas visuales, métricas, edición y eliminación
#  - Estilo corporativo AndicBlue
#  - Ejecución local con archivos CSV temporales
# ==============================================================

import streamlit as st
import pandas as pd
from datetime import datetime
import os
import plotly.express as px
import uuid

# ==============================================================
# CONFIGURACIÓN INICIAL
# ==============================================================

st.set_page_config(
    page_title="AndicBlue Ventas",
    page_icon="🫐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Crear carpeta de datos local
DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

# Archivos CSV
FILE_PEDIDOS = os.path.join(DATA_DIR, "pedidos.csv")
FILE_INVENTARIO = os.path.join(DATA_DIR, "inventario.csv")
FILE_FLUJO = os.path.join(DATA_DIR, "flujo.csv")

# ==============================================================
# FUNCIÓN DE CARGA O CREACIÓN DE DATAFRAMES
# ==============================================================

def load_or_create_csv(filepath, columns):
    if not os.path.exists(filepath):
        df = pd.DataFrame(columns=columns)
        df.to_csv(filepath, index=False)
    else:
        df = pd.read_csv(filepath)
    return df

df_pedidos = load_or_create_csv(FILE_PEDIDOS, [
    "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
    "Producto", "Cantidad", "Precio Unitario", "Subtotal",
    "Domicilio", "Total", "Estado", "Medio Pago",
    "Monto Pagado", "Saldo Pendiente", "Semana Entrega"
])

df_inventario = load_or_create_csv(FILE_INVENTARIO, [
    "Producto", "Stock", "Precio Unitario"
])

df_flujo = load_or_create_csv(FILE_FLUJO, [
    "Fecha", "ID Pedido", "Cliente", "Medio Pago",
    "Ingreso Productos", "Ingreso Domicilio", "Saldo Pendiente"
])

# ==============================================================
# APLICAR ESTILO VISUAL GLOBAL (Dashboard Azul Moderno)
# ==============================================================

def aplicar_estilos():
    st.markdown("""
    <style>
        /* ======= ESTILOS GLOBALES ======= */
        body {
            background-color: #F7FAFC;
            font-family: 'Poppins', sans-serif;
        }
        [data-testid="stSidebar"] {
            background-color: #004E89;
            color: white;
        }
        [data-testid="stSidebar"] .css-1v3fvcr, .css-1l02zno {
            color: white !important;
        }
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
            color: white !important;
        }

        /* ======= TITULOS ======= */
        h1, h2, h3 {
            color: #003366;
        }

        /* ======= BOTONES ======= */
        div.stButton > button:first-child {
            background-color: #0050A0;
            color: white;
            border-radius: 10px;
            padding: 8px 20px;
            font-weight: 600;
        }
        div.stButton > button:hover {
            background-color: #3CA6FF;
            color: white;
        }

        /* ======= TARJETAS ======= */
        .card {
            background-color: white;
            padding: 1.2em;
            border-radius: 15px;
            box-shadow: 0px 2px 8px rgba(0,0,0,0.1);
            margin-bottom: 1.2em;
        }

        .metric-card {
            background-color: white;
            border-left: 6px solid #0050A0;
            padding: 1em;
            border-radius: 12px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.05);
            text-align: center;
        }

        .metric-card h3 {
            color: #0050A0;
            margin-bottom: 5px;
        }

        .metric-card p {
            font-size: 22px;
            font-weight: 600;
        }

        /* ======= TABLAS ======= */
        .stDataFrame {
            border-radius: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

aplicar_estilos()

# ==============================================================
# ENCABEZADO CON LOGO, DATOS Y SEMANA
# ==============================================================

col_logo, col_info = st.columns([1, 4])
with col_logo:
    st.image("andicblue_logo.png", width=180)
with col_info:
    st.markdown("""
    <div style='margin-top:10px'>
        <h2 style='color:#004E89'>🫐 <b>AndicBlue Ventas</b></h2>
        <p><b>Cultivo orgullosamente nariñense</b><br>
        📍 Nariño, Colombia  |  📞 +57 300 000 0000  |  ✉️ contacto@andicblue.com</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

semana_actual = datetime.now().isocalendar()[1]
st.markdown(f"### 📅 Semana actual de entrega: <span style='color:#0050A0; font-weight:600'>{semana_actual}</span>", unsafe_allow_html=True)

# ==============================================================
# SIDEBAR DE NAVEGACIÓN Y MÉTRICAS GLOBALES
# ==============================================================

menu = st.sidebar.radio("📍 Navegación", ["Pedidos", "Inventario", "Flujo / Gastos", "Reportes"], index=0)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Estado general")

try:
    ingresos_total = df_pedidos["Monto Pagado"].sum()
    saldo_total = df_pedidos["Saldo Pendiente"].sum()
    pedidos_pendientes = df_pedidos[df_pedidos["Estado"] == "Pendiente"].shape[0]

    st.sidebar.metric("💵 Ingresos totales", f"${ingresos_total:,.0f}")
    st.sidebar.metric("📦 Pedidos pendientes", pedidos_pendientes)
    st.sidebar.metric("⏳ Saldo por cobrar", f"${saldo_total:,.0f}")
except Exception:
    st.sidebar.info("Sin datos aún.")
# ==============================================================
# ANDICBLUE VENTAS APP - PARTE B
# ==============================================================
# SECCIÓN DE PEDIDOS
# ==============================================================

if menu == "Pedidos":
    st.markdown("## 📝 Gestión de Pedidos")

    # ----------------------------------------------------------
    # Mostrar métricas rápidas de pedidos
    # ----------------------------------------------------------
    col1, col2, col3 = st.columns(3)
    with col1:
        total_pedidos = len(df_pedidos)
        st.markdown(
            f"<div class='metric-card'><h3>Pedidos Totales</h3><p>{total_pedidos}</p></div>",
            unsafe_allow_html=True,
        )
    with col2:
        pedidos_semana = df_pedidos[df_pedidos["Semana Entrega"] == semana_actual]
        st.markdown(
            f"<div class='metric-card'><h3>Semana {semana_actual}</h3><p>{len(pedidos_semana)}</p></div>",
            unsafe_allow_html=True,
        )
    with col3:
        total_ventas = df_pedidos["Monto Pagado"].sum()
        st.markdown(
            f"<div class='metric-card'><h3>Ingresos Totales</h3><p>${total_ventas:,.0f}</p></div>",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ----------------------------------------------------------
    # Formulario para agregar pedidos nuevos
    # ----------------------------------------------------------
    st.markdown("### ➕ Nuevo Pedido")

    with st.form("nuevo_pedido_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            nombre_cliente = st.text_input("Nombre del Cliente")
            id_cliente = str(uuid.uuid4())[:8]
            domicilio = st.number_input("Costo de Domicilio", min_value=0.0, step=1000.0)
        with col2:
            # Filtramos para eliminar el producto genérico “Producto”
            lista_productos = [p for p in df_inventario["Producto"].unique() if p != "Producto"]
            producto = st.selectbox("Producto", options=lista_productos if len(lista_productos) > 0 else ["Sin productos"])
            cantidad = st.number_input("Cantidad", min_value=1, step=1)
        with col3:
            precio_unitario = (
                df_inventario.loc[df_inventario["Producto"] == producto, "Precio Unitario"].values[0]
                if producto in df_inventario["Producto"].values
                else 0
            )
            st.number_input("Precio Unitario", value=float(precio_unitario), disabled=True)
            medio_pago = st.selectbox("Medio de Pago", ["Efectivo", "Transferencia", "Nequi", "Daviplata", "Bancolombia"])
        
        submitted = st.form_submit_button("Registrar Pedido")

        if submitted:
            if not nombre_cliente or producto == "Sin productos":
                st.warning("Por favor, ingrese todos los datos del pedido correctamente.")
            else:
                subtotal = cantidad * precio_unitario
                total = subtotal + domicilio
                fecha_pedido = datetime.now().strftime("%Y-%m-%d")

                nuevo_pedido = pd.DataFrame([{
                    "ID Pedido": str(uuid.uuid4())[:8],
                    "Fecha": fecha_pedido,
                    "ID Cliente": id_cliente,
                    "Nombre Cliente": nombre_cliente,
                    "Producto": producto,
                    "Cantidad": cantidad,
                    "Precio Unitario": precio_unitario,
                    "Subtotal": subtotal,
                    "Domicilio": domicilio,
                    "Total": total,
                    "Estado": "Pendiente",
                    "Medio Pago": medio_pago,
                    "Monto Pagado": 0,
                    "Saldo Pendiente": total,
                    "Semana Entrega": semana_actual
                }])

                # Evitar duplicar encabezados al guardar
                if os.path.exists(FILE_PEDIDOS):
                    nuevo_pedido.to_csv(FILE_PEDIDOS, mode="a", header=False, index=False)
                else:
                    nuevo_pedido.to_csv(FILE_PEDIDOS, index=False)

                df_pedidos = pd.concat([df_pedidos, nuevo_pedido], ignore_index=True)

                st.success("✅ Pedido registrado correctamente.")

    st.markdown("---")

    # ----------------------------------------------------------
    # TABLA DE PEDIDOS EXISTENTES
    # ----------------------------------------------------------
    st.markdown("### 📋 Pedidos Registrados")

    col1, col2 = st.columns([2, 1])
    with col1:
        filtro_estado = st.selectbox("Filtrar por estado", ["Todos", "Pendiente", "Entregado"])
    with col2:
        filtro_semana = st.number_input("Semana", value=semana_actual, min_value=1, max_value=52)

    df_filtrado = df_pedidos.copy()
    if filtro_estado != "Todos":
        df_filtrado = df_filtrado[df_filtrado["Estado"] == filtro_estado]
    if filtro_semana:
        df_filtrado = df_filtrado[df_filtrado["Semana Entrega"] == filtro_semana]

    if df_filtrado.empty:
        st.info("No hay pedidos para los filtros seleccionados.")
    else:
        st.dataframe(
            df_filtrado.sort_values("Fecha", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    # ----------------------------------------------------------
    # SECCIÓN DE ACTUALIZACIÓN DE ESTADO Y PAGOS
    # ----------------------------------------------------------
    st.markdown("---")
    st.markdown("### 🔄 Actualizar Estado o Pago")

    col1, col2, col3 = st.columns(3)
    with col1:
        pedido_id = st.selectbox("Seleccionar Pedido", options=df_pedidos["ID Pedido"])
    with col2:
        nuevo_estado = st.selectbox("Nuevo Estado", ["Pendiente", "Entregado"])
    with col3:
        monto_pagado = st.number_input("Monto Pagado", min_value=0.0, step=500.0)

    if st.button("Actualizar Pedido"):
        idx = df_pedidos[df_pedidos["ID Pedido"] == pedido_id].index
        if not idx.empty:
            total = df_pedidos.loc[idx, "Total"].values[0]
            pagado = df_pedidos.loc[idx, "Monto Pagado"].values[0] + monto_pagado
            saldo = total - pagado
            df_pedidos.loc[idx, "Monto Pagado"] = pagado
            df_pedidos.loc[idx, "Saldo Pendiente"] = saldo
            df_pedidos.loc[idx, "Estado"] = nuevo_estado
            df_pedidos.to_csv(FILE_PEDIDOS, index=False)
            st.success("✅ Pedido actualizado correctamente.")
        else:
            st.warning("No se encontró el pedido.")

    # ----------------------------------------------------------
    # ELIMINAR PEDIDOS
    # ----------------------------------------------------------
    st.markdown("---")
    st.markdown("### 🗑️ Eliminar Pedido")

    col1, col2 = st.columns([3, 1])
    with col1:
        pedido_eliminar = st.selectbox("Seleccionar Pedido a eliminar", options=df_pedidos["ID Pedido"])
    with col2:
        eliminar = st.button("Eliminar")

    if eliminar:
        df_pedidos = df_pedidos[df_pedidos["ID Pedido"] != pedido_eliminar]
        df_pedidos.to_csv(FILE_PEDIDOS, index=False)
        st.success("🧹 Pedido eliminado correctamente.")
# ==============================================================
# ANDICBLUE VENTAS APP - PARTE C
# ==============================================================
# SECCIÓN DE INVENTARIO + FLUJO DE CAJA + MÉTRICAS
# ==============================================================

elif menu == "Inventario":
    st.markdown("## 📦 Control de Inventario")

    # ----------------------------------------------------------
    # Mostrar métricas del inventario
    # ----------------------------------------------------------
    total_items = len(df_inventario)
    total_stock = df_inventario["Stock"].sum()
    valor_inventario = (df_inventario["Stock"] * df_inventario["Precio Unitario"]).sum()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"<div class='metric-card'><h3>Productos</h3><p>{total_items}</p></div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div class='metric-card'><h3>Unidades Totales</h3><p>{total_stock}</p></div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div class='metric-card'><h3>Valor Total</h3><p>${valor_inventario:,.0f}</p></div>", unsafe_allow_html=True)

    st.markdown("---")

    # ----------------------------------------------------------
    # Tabla de inventario
    # ----------------------------------------------------------
    st.markdown("### 🧾 Inventario Actual")
    st.dataframe(df_inventario, use_container_width=True, hide_index=True)

    # ----------------------------------------------------------
    # Ajuste de stock (puede ser negativo)
    # ----------------------------------------------------------
    st.markdown("### 🔧 Ajustar Stock")

    col1, col2, col3 = st.columns(3)
    with col1:
        producto_sel = st.selectbox("Seleccionar Producto", df_inventario["Producto"].unique())
    with col2:
        cantidad_ajuste = st.number_input("Cantidad a Ajustar (+/-)", step=1, format="%d")
    with col3:
        if st.button("Aplicar Ajuste"):
            df_inventario.loc[df_inventario["Producto"] == producto_sel, "Stock"] += cantidad_ajuste
            df_inventario.to_csv(FILE_INVENTARIO, index=False)
            st.success("✅ Stock actualizado correctamente.")

    st.markdown("---")

    # ----------------------------------------------------------
    # Agregar nuevo producto
    # ----------------------------------------------------------
    st.markdown("### ➕ Agregar Nuevo Producto")
    with st.form("nuevo_producto"):
        col1, col2, col3 = st.columns(3)
        with col1:
            nuevo_producto = st.text_input("Nombre del producto")
        with col2:
            nuevo_precio = st.number_input("Precio unitario", min_value=0.0, step=500.0)
        with col3:
            nuevo_stock = st.number_input("Stock inicial", step=1, format="%d")
        guardar = st.form_submit_button("Guardar")

        if guardar and nuevo_producto:
            if nuevo_producto not in df_inventario["Producto"].values:
                nuevo = pd.DataFrame([{
                    "Producto": nuevo_producto,
                    "Precio Unitario": nuevo_precio,
                    "Stock": nuevo_stock
                }])
                df_inventario = pd.concat([df_inventario, nuevo], ignore_index=True)
                df_inventario.to_csv(FILE_INVENTARIO, index=False)
                st.success("✅ Producto agregado al inventario.")
            else:
                st.warning("⚠️ Este producto ya existe en el inventario.")

# ==============================================================
# SECCIÓN DE FLUJO DE CAJA
# ==============================================================
elif menu == "Flujo de Caja":
    st.markdown("## 💰 Flujo de Caja - AndicBlue")

    # ----------------------------------------------------------
    # Cálculos base
    # ----------------------------------------------------------
    df_pedidos["Fecha"] = pd.to_datetime(df_pedidos["Fecha"], errors="coerce")
    ingresos_total = df_pedidos["Monto Pagado"].sum()

    ingresos_por_medio = df_pedidos.groupby("Medio Pago")["Monto Pagado"].sum().reset_index()
    ingresos_por_medio.columns = ["Medio de Pago", "Total Ingresado"]

    st.markdown("### 📊 Resumen de Ingresos por Medio de Pago")
    st.dataframe(ingresos_por_medio, use_container_width=True, hide_index=True)

    # Métricas visuales
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"<div class='metric-card'><h3>Total Ingresos</h3><p>${ingresos_total:,.0f}</p></div>", unsafe_allow_html=True)
    with c2:
        total_pendiente = df_pedidos["Saldo Pendiente"].sum()
        st.markdown(f"<div class='metric-card'><h3>Saldo Pendiente</h3><p>${total_pendiente:,.0f}</p></div>", unsafe_allow_html=True)
    with c3:
        total_domicilio = df_pedidos["Monto Domicilio"].sum() if "Monto Domicilio" in df_pedidos.columns else 0
        st.markdown(f"<div class='metric-card'><h3>Ingresos Domicilios</h3><p>${total_domicilio:,.0f}</p></div>", unsafe_allow_html=True)

    st.markdown("---")

    # ----------------------------------------------------------
    # GESTIÓN DE RETIROS (CAMBIO DE TRANSFERENCIA A EFECTIVO)
    # ----------------------------------------------------------
    st.markdown("### 🏧 Control de Retiros desde Cajero")

    col1, col2, col3 = st.columns(3)
    with col1:
        monto_retiro = st.number_input("Monto retirado", min_value=0.0, step=1000.0)
    with col2:
        medio_origen = st.selectbox("Desde cuenta", ["Transferencia", "Nequi", "Daviplata", "Bancolombia"])
    with col3:
        confirmar = st.button("Registrar Retiro")

    if confirmar and monto_retiro > 0:
        st.success(f"💵 Retiro registrado: ${monto_retiro:,.0f} desde {medio_origen}")
        # (Simulación local – en versión GSheets se agregaría fila de movimiento)

    st.markdown("---")

    # ----------------------------------------------------------
    # GRÁFICO DE INGRESOS POR MEDIO DE PAGO
    # ----------------------------------------------------------
    st.markdown("### 📈 Visualización de Ingresos")
    fig, ax = plt.subplots(figsize=(6, 3))
    ax.bar(ingresos_por_medio["Medio de Pago"], ingresos_por_medio["Total Ingresado"])
    ax.set_ylabel("Monto ($)")
    ax.set_xlabel("Medio de Pago")
    ax.set_title("Ingresos por Medio de Pago")
    st.pyplot(fig)

# ==============================================================
# PANEL DE REPORTES Y MÉTRICAS GLOBALES
# ==============================================================
elif menu == "Reportes":
    st.markdown("## 📊 Panel de Reportes Generales")

    st.markdown("### 💼 Totales Generales")
    total_vendido = df_pedidos["Monto Pagado"].sum()
    total_saldos = df_pedidos["Saldo Pendiente"].sum()
    total_pedidos = len(df_pedidos)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"<div class='metric-card'><h3>Total Vendido</h3><p>${total_vendido:,.0f}</p></div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div class='metric-card'><h3>Saldo Pendiente</h3><p>${total_saldos:,.0f}</p></div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div class='metric-card'><h3>Pedidos Totales</h3><p>{total_pedidos}</p></div>", unsafe_allow_html=True)

    st.markdown("---")

    # ----------------------------------------------------------
    # Gráfico de evolución semanal
    # ----------------------------------------------------------
    st.markdown("### 📅 Ingresos por Semana de Entrega")
    df_semana = df_pedidos.groupby("Semana Entrega")["Monto Pagado"].sum().reset_index()
    if not df_semana.empty:
        fig2, ax2 = plt.subplots(figsize=(7, 3))
        ax2.plot(df_semana["Semana Entrega"], df_semana["Monto Pagado"], marker="o")
        ax2.set_title("Evolución Semanal de Ingresos")
        ax2.set_xlabel("Semana")
        ax2.set_ylabel("Monto Pagado ($)")
        st.pyplot(fig2)
    else:
        st.info("Aún no hay datos suficientes para graficar.")
# ==============================================================
# ANDICBLUE VENTAS APP - PARTE D (INTERFAZ COMPLETA)
# ==============================================================

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime

# --------------------------------------------------------------
# CONFIGURACIÓN GENERAL
# --------------------------------------------------------------
st.set_page_config(
    page_title="AndicBlue Ventas Dashboard",
    layout="wide",
    page_icon="🫐",
)

# --------------------------------------------------------------
# ESTILOS PERSONALIZADOS
# --------------------------------------------------------------
st.markdown("""
<style>
    /* Fondo general */
    .stApp {
        background-color: #f7faff;
        color: #1d3557;
    }

    /* Tarjetas */
    .metric-card {
        background: linear-gradient(145deg, #ffffff, #e4ecf7);
        border-radius: 18px;
        padding: 1.2em;
        text-align: center;
        box-shadow: 3px 3px 10px rgba(0,0,0,0.1);
        margin-bottom: 15px;
    }
    .metric-card h3 {
        color: #1d3557;
        font-size: 18px;
        margin-bottom: 6px;
    }
    .metric-card p {
        font-size: 22px;
        font-weight: 600;
        color: #0077b6;
    }

    /* Botones principales */
    div.stButton > button {
        background-color: #0077b6;
        color: white;
        font-weight: 500;
        border-radius: 8px;
        height: 40px;
        transition: 0.3s;
    }
    div.stButton > button:hover {
        background-color: #023e8a;
        transform: scale(1.02);
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1d3557, #003049);
        color: white;
    }
    [data-testid="stSidebar"] * {
        color: white !important;
    }

    /* Encabezado superior */
    .header-container {
        display: flex;
        align-items: center;
        justify-content: space-between;
        background: #0077b6;
        color: white;
        padding: 1em 2em;
        border-radius: 12px;
        margin-bottom: 25px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
    }
    .header-logo img {
        width: 85px;
        height: auto;
        border-radius: 12px;
        margin-right: 1em;
    }
    .header-title {
        font-size: 26px;
        font-weight: 700;
        letter-spacing: 1px;
    }
    .header-contact {
        text-align: right;
        font-size: 14px;
    }

    /* Accesos rápidos */
    .quick-access {
        display: flex;
        justify-content: center;
        gap: 2em;
        margin-bottom: 20px;
    }
    .quick-access button {
        background-color: #1d3557 !important;
        color: white !important;
        font-weight: 500;
        border-radius: 10px !important;
        padding: 0.6em 1.2em !important;
    }
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------------------
# ENCABEZADO CON LOGO Y CONTACTO
# --------------------------------------------------------------
col1, col2, col3 = st.columns([1, 4, 2])
with col1:
    if os.path.exists("andicblue_logo.png"):
        st.image("andicblue_logo.png", width=100)
with col2:
    st.markdown("<div class='header-title'>ANDICBLUE - Gestión de Ventas y Logística</div>", unsafe_allow_html=True)
with col3:
    st.markdown("""
    <div class='header-contact'>
        📍 Nariño, Colombia<br>
        📧 contacto@andicblue.com<br>
        ☎️ +57 320 000 0000
    </div>
    """, unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

# --------------------------------------------------------------
# INFORMACIÓN DE SEMANA ACTUAL
# --------------------------------------------------------------
current_week = datetime.now().isocalendar().week
st.markdown(f"### 🗓️ Semana actual de entrega: **{current_week}**")

# --------------------------------------------------------------
# ACCESOS RÁPIDOS A SECCIONES
# --------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("🛒 Pedidos"):
        st.session_state.menu = "Pedidos"
with col2:
    if st.button("📦 Inventario"):
        st.session_state.menu = "Inventario"
with col3:
    if st.button("💰 Flujo de Caja"):
        st.session_state.menu = "Flujo de Caja"
with col4:
    if st.button("📈 Reportes"):
        st.session_state.menu = "Reportes"

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------------------
# CARGA LOCAL DE DATOS (CSV TEMPORALES)
# --------------------------------------------------------------
FILE_PEDIDOS = "pedidos.csv"
FILE_INVENTARIO = "inventario.csv"

if not os.path.exists(FILE_PEDIDOS):
    pd.DataFrame(columns=[
        "ID Pedido", "Fecha", "ID Cliente", "Nombre Cliente",
        "Producto", "Cantidad", "Precio Unitario",
        "Subtotal", "Monto Domicilio", "Total Pedido",
        "Estado", "Medio Pago", "Monto Pagado",
        "Saldo Pendiente", "Semana Entrega"
    ]).to_csv(FILE_PEDIDOS, index=False)

if not os.path.exists(FILE_INVENTARIO):
    pd.DataFrame(columns=["Producto", "Precio Unitario", "Stock"]).to_csv(FILE_INVENTARIO, index=False)

df_pedidos = pd.read_csv(FILE_PEDIDOS)
df_inventario = pd.read_csv(FILE_INVENTARIO)

# --------------------------------------------------------------
# MENÚ LATERAL DE SECCIONES
# --------------------------------------------------------------
menu = st.sidebar.radio("📋 Menú Principal", ["Pedidos", "Inventario", "Flujo de Caja", "Reportes"])

# --------------------------------------------------------------
# IMPORTAR FUNCIONALIDAD DE PARTES A, B y C
# --------------------------------------------------------------
# (En esta versión completa, las partes anteriores A, B, C se incluyen debajo)
# Aquí vendrían todas las funciones ya desarrolladas: registro de pedidos,
# gestión de pagos, inventario editable, métricas visuales y reportes.

# ==============================================================
# NOTA: Para el código completo, aquí se concatenan las partes A + B + C
#       del flujo anterior sin cambios de lógica.
# ==============================================================

# Asegúrate de incluir el bloque del registro de pedidos, entregas/pagos,
# control de inventario y flujo de caja desde las partes previas.

st.markdown("<br><br><center>🫐 <b>AndicBlue</b> - Un cultivo orgullosamente nariñense</center>", unsafe_allow_html=True)
