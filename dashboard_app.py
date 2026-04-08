import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

st.set_page_config(
    page_title="Dashboard E-commerce",
    page_icon="📊",
    layout="wide"
)

st.title("🛒 Dashboard E-commerce : Modern Data Stack")
st.markdown("Ce dashboard est branché directement sur notre modèle en étoile (`core.fact_orders` et `core.dim_customers`) dans PostgreSQL.")

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")
DB_NAME = os.getenv("POSTGRES_DB", "ecommerce_dw")
DB_HOST = "localhost"
DB_PORT = "5432"

@st.cache_data
def load_data(query):
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return pd.read_sql(query, engine)

with st.spinner('Extraction des données via SQL...'):
    query_fact = "SELECT * FROM core.fact_orders WHERE order_status = 'delivered'"
    df_orders = load_data(query_fact)
    
    query_dim = "SELECT * FROM core.dim_customers"
    df_customers = load_data(query_dim)

    df_denormalized = pd.merge(df_orders, df_customers, on='customer_id', how='inner')
    
    df_denormalized['order_date'] = pd.to_datetime(df_denormalized['order_date'])
    df_denormalized['month_year'] = df_denormalized['order_date'].dt.to_period('M').astype(str)

st.header("1. 💰 Indicateurs Financiers (Enrichis par l'API Frankfurter)")

total_ops = len(df_denormalized)
revenue_brl = df_denormalized['revenue_brl'].sum()
revenue_usd = df_denormalized['revenue_usd'].sum()

col1, col2, col3 = st.columns(3)
col1.metric("Nombre de Ventes", f"{total_ops:,}".replace(',', ' '))
col2.metric("Chiffre d'Affaires (BRL)", f"R$ {revenue_brl:,.2f}".replace(',', ' '))
col3.metric("Chiffre d'Affaires Réel (USD)", f"$ {revenue_usd:,.2f}".replace(',', ' '), 
            help="Converti avec les taux de change historiques exacts le jour de chaque achat")

st.divider()

st.header("2. 📈 Analyses Avancées")

col_graph1, col_graph2 = st.columns(2)

with col_graph1:
    st.subheader("Évolution du C.A. Mensuel (USD)")
    df_time = df_denormalized.groupby('month_year')['revenue_usd'].sum().reset_index()
    fig_time = px.line(df_time, x='month_year', y='revenue_usd', markers=True, 
                       title="Saisonnalité des ventes (Pics du Black Friday visibles)")
    st.plotly_chart(fig_time, use_container_width=True)

with col_graph2:
    st.subheader("Top des Ventes par État Brésilien")
    df_state = df_denormalized.groupby('state_name')['revenue_usd'].sum().reset_index().sort_values(by='revenue_usd', ascending=False).head(10)
    fig_state = px.bar(df_state, x='state_name', y='revenue_usd', color='revenue_usd',
                       title="Concentration des revenus selon les États")
    st.plotly_chart(fig_state, use_container_width=True)

st.divider()

st.header("3. 🌍 Le Taux de Pénétration (Le pouvoir du Web Scraping)")
st.markdown("Grâce au Web Scraping de Wikipédia, nous connaissons la population de chaque État. Comparons le nombre de commandes par rapport au nombre d'habitants pour voir où l'e-commerce cartonne !")

df_pop = df_denormalized.groupby('state_name').agg({
    'order_id': 'count', 
    'state_population': 'max'
}).reset_index()

df_pop['commandes_per_10k_habitants'] = (df_pop['order_id'] / df_pop['state_population']) * 10000
df_pop_sorted = df_pop.sort_values(by='commandes_per_10k_habitants', ascending=False)

fig_scatter = px.scatter(df_pop_sorted, x='state_population', y='order_id', 
                         size='commandes_per_10k_habitants', color='state_name',
                         hover_name='state_name', log_x=True, log_y=True,
                         labels={'state_population': 'Population (Échelle Log)', 'order_id': 'Ventes Totales (Échelle Log)'},
                         title="Y a-t-il des petits États qui sur-consomment ? (Taille de bulle : intensité d'achat)")
st.plotly_chart(fig_scatter, use_container_width=True)

st.success("🎉 Vous avez atteint la fin du cycle : Ingestion > Transformation > Orchestration > Visualisation !")