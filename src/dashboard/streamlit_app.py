import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sqlite3
from datetime import datetime
import os

st.set_page_config(page_title="Dashboard IoT", layout="wide")

st.title("Dashboard IoT - Surveillance Industrielle")
st.markdown("---")

db_path = "data/iot_data.db"
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    
    st.sidebar.title("Configuration")
    
    machines_query = "SELECT DISTINCT machine_id FROM iot_measurements"
    machines = pd.read_sql(machines_query, conn)['machine_id'].tolist()
    
    if not machines:
        machines = ["M001", "M002", "M003"]
    
    selected = st.sidebar.multiselect("Machines", machines, default=machines[:3])
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total = pd.read_sql("SELECT COUNT(*) FROM iot_measurements", conn).iloc[0,0]
        st.metric("Mesures totales", total)
    
    with col2:
        anomalies = pd.read_sql("SELECT COUNT(*) FROM iot_measurements WHERE is_anomaly=1", conn).iloc[0,0]
        st.metric("Anomalies", anomalies)
    
    with col3:
        machine_count = pd.read_sql("SELECT COUNT(DISTINCT machine_id) FROM iot_measurements", conn).iloc[0,0]
        st.metric("Machines", machine_count)
    
    with col4:
        alerts = pd.read_sql("SELECT COUNT(*) FROM alerts WHERE resolved=0", conn).iloc[0,0]
        st.metric("Alertes", alerts)
    
    st.markdown("---")
    
    if selected:
        query = f"SELECT * FROM iot_measurements WHERE machine_id IN ({','.join(['?']*len(selected))}) ORDER BY timestamp DESC LIMIT 100"
        df = pd.read_sql(query, conn, params=selected)
        
        if len(df) > 0:
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.subheader("Temperature")
                fig = go.Figure()
                for machine in df['machine_id'].unique():
                    machine_data = df[df['machine_id'] == machine]
                    fig.add_trace(go.Scatter(x=machine_data['timestamp'], y=machine_data['temperature'], name=machine))
                fig.add_hline(y=90, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
            
            with col_right:
                st.subheader("Pression")
                fig = go.Figure()
                for machine in df['machine_id'].unique():
                    machine_data = df[df['machine_id'] == machine]
                    fig.add_trace(go.Scatter(x=machine_data['timestamp'], y=machine_data['pressure'], name=machine))
                fig.add_hline(y=180, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Dernieres mesures")
            st.dataframe(df[['timestamp','machine_id','temperature','pressure','is_anomaly']].head(10))
        else:
            st.info("Aucune donnee")
    else:
        st.warning("Selectionnez des machines")
    
    conn.close()
else:
    st.error("Base de donnees non trouvee")
    st.info("Lancez le consumer d'abord")

st.markdown("---")
st.caption(f"Derniere mise a jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
