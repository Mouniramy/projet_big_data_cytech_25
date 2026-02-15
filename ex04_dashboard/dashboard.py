import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# Configuration de la page
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")
st.title("🚖 Analyse des courses de Taxi - CAC 40 Architecture")

# Connexion à PostgreSQL
# On utilise les identifiants définis dans le docker-compose
engine = create_engine('postgresql://user:password@localhost:5432/nyc_dw')

@st.cache_data
def load_data():
    query = """
    SELECT f.*, v.vendor_name, l.borough, l.zone
    FROM fact_trips f
    JOIN dim_vendor v ON f.vendor_id = v.vendor_id
    JOIN dim_location l ON f.pickup_location_id = l.location_id
    """
    return pd.read_sql(query, engine)

df = load_data()

# --- Visualisations ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Répartition par Vendeur")
    fig_vendor = px.pie(df, names='vendor_name', title="Volume de courses par prestataire")
    st.plotly_chart(fig_vendor)

with col2:
    st.subheader("Distribution des prix")
    fig_dist = px.histogram(df, x='total_amount', nbins=20, title="Analyse du montant total")
    st.plotly_chart(fig_dist)

st.subheader("Aperçu des données du Datamart")
st.dataframe(df.head(10))