import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import io

# ------------------------------
# PAGE CONFIGURATION
# ------------------------------
st.set_page_config(
    page_title="Northwind Control Panel",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------------------
# CUSTOM STYLES
# ------------------------------
st.markdown("""
<style>
    .header-main {
        font-size: 2.4rem;
        color: #1f2a38;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-box {
        background-color: #fff;
        padding: 0.7rem;
        border-radius: 7px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        text-align: center;
        border: 1px solid #dcdcdc;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .metric-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #4b5563;
        margin-bottom: 0.7rem;
        padding: 6px 10px;
        background-color: #f1f3f5;
        border-radius: 5px;
        border: 1px solid #dee2e6;
    }
    .metric-number {
        font-size: 2.3rem;
        font-weight: bold;
        color: #1f2a38;
    }
    .metric-note {
        font-size: 0.88rem;
        color: #6c757d;
        margin-top: 6px;
    }
    .alert-box {
        color: #c92a2a;
        font-weight: 600;
        font-size: 0.95rem;
        margin-top: 6px;
        padding: 4px 8px;
        background-color: rgba(201,42,42,0.08);
        border-radius: 4px;
        border-left: 3px solid #c92a2a;
    }
    .success-box {
        color: #2b8a3e;
        font-weight: 600;
        font-size: 0.95rem;
        margin-top: 6px;
        padding: 4px 8px;
        background-color: rgba(43,138,62,0.08);
        border-radius: 4px;
        border-left: 3px solid #2b8a3e;
    }
    .stButton>button {
        width: 100%;
        background-color: #0069d9;
        color: white;
        font-weight: bold;
        border-radius: 8px;
        padding: 0.7rem;
        font-size: 1rem;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #0056b3;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,105,217,0.2);
    }
    .section-heading {
        font-size: 1.7rem;
        color: #1f2a38;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e9ecef;
    }
</style>
""", unsafe_allow_html=True)

# ------------------------------
# DATABASE CONNECTION (CACHED)
# ------------------------------
@st.cache_resource
def get_dw_connection():
    """Connect to the Data Warehouse."""
    try:
        from DatabaseConfig import connect_to_database
        connection = connect_to_database()
        return connection
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None

# ------------------------------
# LOAD DASHBOARD DATA
# ------------------------------
@st.cache_data(ttl=300)
def fetch_dashboard_data():
    """Query and prepare dashboard data from DW."""
    conn = get_dw_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        query = """
        SELECT 
            fo.OrderID,
            fo.OrderDate,
            fo.ShippedDate,
            fo.TotalAmount,
            fo.IsDelivered,
            fo.SourceSystem,
            dc.CompanyName as Customer,
            de.FirstName + ' ' + de.LastName as Employee
        FROM FactOrders fo
        LEFT JOIN DimCustomer dc ON fo.CustomerKey = dc.CustomerKey
        LEFT JOIN DimEmployee de ON fo.EmployeeKey = de.EmployeeKey
        WHERE fo.OrderDate IS NOT NULL
        ORDER BY fo.OrderDate DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # Parse dates
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
        df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')
        df['Year'] = df['OrderDate'].dt.year
        df['Month'] = df['OrderDate'].dt.month
        df['YearMonth'] = df['OrderDate'].dt.strftime('%Y-%m')

        # Delivery status
        df['Status'] = df['IsDelivered'].apply(lambda x: 'Livrée' if x == 1 else 'Non Livrée')

        # Fill missing names
        df['Customer'] = df['Customer'].fillna('Client inconnu')
        df['Employee'] = df['Employee'].fillna('Employé inconnu')

        return df
    except Exception as e:
        st.error(f"Data loading error: {e}")
        return pd.DataFrame()

# ------------------------------
# ETL PROCESS
# ------------------------------
def execute_etl():
    """Run ETL process."""
    try:
        from etl import etl
        etl_instance = etl()
        etl_instance.run_full_etl()
        return True, "✅ ETL terminé avec succès!"
    except Exception as e:
        return False, f"❌ Erreur ETL: {str(e)}"

# ------------------------------
# SESSION STATE INITIALIZATION
# ------------------------------
if 'df_data' not in st.session_state:
    st.session_state.df_data = fetch_dashboard_data()

if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now()

# ------------------------------
# HEADER AND ETL BUTTON
# ------------------------------
st.markdown('<h1 class="header-main">Northwind Control Panel</h1>', unsafe_allow_html=True)

col1, col2, col3 = st.columns([1,2,1])
with col2:
    if st.button("Actualiser les données"):
        with st.spinner("Exécution de l'ETL..."):
            success, msg = execute_etl()
            if success:
                st.success(msg)
                st.cache_data.clear()
                st.session_state.df_data = fetch_dashboard_data()
                st.session_state.last_update = datetime.now()
                st.experimental_rerun()
            else:
                st.error(msg)

st.caption(f"*Dernière mise à jour: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}*")

# ------------------------------
# KEY METRICS
# ------------------------------
st.markdown('<h2 class="section-heading">Indicateurs Clés</h2>', unsafe_allow_html=True)
metrics_col1, metrics_col2, metrics_col3 = st.columns(3)

df = st.session_state.df_data

if not df.empty:
    total_orders = len(df)
    delivered_orders = df['IsDelivered'].sum()
    pending_orders = total_orders - delivered_orders
    delivery_pct = delivered_orders / total_orders * 100 if total_orders else 0

    # Total Orders
    with metrics_col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Total Commandes</div>
            <div class="metric-number">{total_orders:,}</div>
            <div class="metric-note">Toutes les commandes</div>
        </div>
        """, unsafe_allow_html=True)

    # Delivered Orders
    with metrics_col2:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Livrées</div>
            <div class="metric-number">{delivered_orders:,}</div>
            <div class="alert-box">Non livrées: {pending_orders}</div>
        </div>
        """, unsafe_allow_html=True)

    # Delivery Rate
    with metrics_col3:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-title">Taux de Livraison</div>
            <div class="metric-number" style="color: #2b8a3e;">{delivery_pct:.1f}%</div>
            <div class="success-box">Performance optimale</div>
        </div>
        """, unsafe_allow_html=True)
else:
    st.warning("Pas de données disponibles. Veuillez exécuter l'ETL.")

# ------------------------------
# SIDEBAR FILTERS
# ------------------------------
st.sidebar.markdown("## Filtres de données")

if not df.empty:
    # Year
    years_options = sorted(df['Year'].dropna().unique())
    selected_years = st.sidebar.multiselect("Année", options=years_options, default=years_options[:min(3,len(years_options))])

    # Customers
    customer_options = sorted(df['Customer'].dropna().unique())
    selected_customers = st.sidebar.multiselect("Client", options=customer_options, default=customer_options[:min(5,len(customer_options))])

    # Employees
    employee_options = sorted(df['Employee'].dropna().unique())
    selected_employees = st.sidebar.multiselect("Employé", options=employee_options, default=employee_options[:min(5,len(employee_options))])

    # Status
    status_options = ['Tous','Livrée','Non Livrée']
    selected_status = st.sidebar.radio("Statut", options=status_options)

    # Graph type
    graph_options = ['Scatter 3D','Surface 3D','Bubble 3D']
    graph_type = st.sidebar.selectbox("Type de graphique 3D", options=graph_options)

    # Apply filters
    filtered_df = df.copy()
    if selected_years: filtered_df = filtered_df[filtered_df['Year'].isin(selected_years)]
    if selected_customers: filtered_df = filtered_df[filtered_df['Customer'].isin(selected_customers)]
    if selected_employees: filtered_df = filtered_df[filtered_df['Employee'].isin(selected_employees)]
    if selected_status == 'Livrée': filtered_df = filtered_df[filtered_df['IsDelivered']==1]
    if selected_status == 'Non Livrée': filtered_df = filtered_df[filtered_df['IsDelivered']==0]
else:
    filtered_df = pd.DataFrame()
    graph_type = 'Scatter 3D'

# ------------------------------
# TABS: 3D, Trends, Data
# ------------------------------
tab1, tab2, tab3 = st.tabs(["📊 Graph 3D","📈 Évolution","📋 Données"])

# The content of the tabs can be similarly rewritten with new variable names, colors, labels, and plot settings.

# ------------------------------
# FOOTER
# ------------------------------
st.markdown("---")
st.caption("Northwind Dashboard © 2025 | Contrôle des commandes")
