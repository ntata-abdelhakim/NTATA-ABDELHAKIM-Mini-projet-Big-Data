import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import sqlite3
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

def get_db_connection():
    return sqlite3.connect('data/iot_data.db')

def load_all_data():
    conn = get_db_connection()
    
    iot_df = pd.read_sql_query("SELECT * FROM iot_measurements ORDER BY timestamp DESC LIMIT 1000", conn)
    csv_df = pd.read_sql_query("SELECT * FROM csv_data ORDER BY timestamp DESC LIMIT 1000", conn)
    api_df = pd.read_sql_query("SELECT * FROM api_data ORDER BY timestamp DESC LIMIT 1000", conn)
    logs_df = pd.read_sql_query("SELECT * FROM system_logs ORDER BY timestamp DESC LIMIT 1000", conn)
    alerts_df = pd.read_sql_query("SELECT * FROM alerts ORDER BY timestamp DESC", conn)
    machines_df = pd.read_sql_query("SELECT * FROM machines", conn)
    
    conn.close()
    
    return iot_df, csv_df, api_df, logs_df, alerts_df, machines_df

def main():
    st.set_page_config(
        page_title="Dashboard IoT Multi-Sources",
        page_icon="",
        layout="wide"
    )
    
    st.title(" Dashboard IoT Multi-Sources")
    
    iot_df, csv_df, api_df, logs_df, alerts_df, machines_df = load_all_data()
    
    if iot_df.empty and csv_df.empty and api_df.empty:
        st.warning("Aucune donnée disponible.")
        return
    
    st.sidebar.header("Configuration")
    
    source_types = []
    if not iot_df.empty:
        source_types.append("IoT")
    if not csv_df.empty:
        source_types.append("CSV")
    if not api_df.empty:
        source_types.append("API")
    if not logs_df.empty:
        source_types.append("Logs")
    
    selected_sources = st.sidebar.multiselect(
        "Sources de données:",
        options=source_types,
        default=source_types
    )
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("IoT", len(iot_df))
    with col2:
        st.metric("CSV", len(csv_df))
    with col3:
        st.metric("API", len(api_df))
    with col4:
        st.metric("Logs", len(logs_df))
    with col5:
        st.metric("Alertes", len(alerts_df))
    
    tab1, tab2, tab3, tab4 = st.tabs(["IoT", "CSV", "Logs", "Analyse"])
    
    with tab1:
        if not iot_df.empty:
            iot_df['timestamp'] = pd.to_datetime(iot_df['timestamp'])
            
            fig_temp = px.line(
                iot_df, 
                x='timestamp', 
                y='temperature', 
                color='machine_id',
                title='Température IoT'
            )
            st.plotly_chart(fig_temp, use_container_width=True)
            
            fig_pressure = px.line(
                iot_df, 
                x='timestamp', 
                y='pressure', 
                color='machine_id',
                title='Pression IoT'
            )
            st.plotly_chart(fig_pressure, use_container_width=True)
    
    with tab2:
        if not csv_df.empty:
            csv_df['timestamp'] = pd.to_datetime(csv_df['timestamp'])
            
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Données CSV")
                st.dataframe(csv_df.head(10))
            
            with col2:
                st.subheader("Statistiques")
                stats = csv_df.groupby('machine_id').agg({
                    'temperature': ['mean', 'max', 'min'],
                    'pressure': ['mean', 'max', 'min']
                }).round(2)
                st.dataframe(stats)
    
    with tab3:
        if not logs_df.empty:
            logs_df['timestamp'] = pd.to_datetime(logs_df['timestamp'])
            
            st.subheader("Logs système")
            st.dataframe(logs_df.head(20))
            
            st.subheader("Distribution des logs")
            log_counts = logs_df['log_level'].value_counts()
            fig_logs = px.pie(
                values=log_counts.values,
                names=log_counts.index,
                title='Niveaux de log'
            )
            st.plotly_chart(fig_logs, use_container_width=True)
    
    with tab4:
        st.subheader("Analyse multi-sources")
        
        all_data = []
        if not iot_df.empty:
            iot_summary = {
                'source': 'IoT',
                'count': len(iot_df),
                'machines': iot_df['machine_id'].nunique(),
                'avg_temp': iot_df['temperature'].mean(),
                'avg_pressure': iot_df['pressure'].mean()
            }
            all_data.append(iot_summary)
        
        if not csv_df.empty:
            csv_summary = {
                'source': 'CSV',
                'count': len(csv_df),
                'machines': csv_df['machine_id'].nunique(),
                'avg_temp': csv_df['temperature'].mean(),
                'avg_pressure': csv_df['pressure'].mean()
            }
            all_data.append(csv_summary)
        
        if all_data:
            summary_df = pd.DataFrame(all_data)
            st.dataframe(summary_df)
            
            fig_sources = px.bar(
                summary_df,
                x='source',
                y='count',
                title='Données par source',
                color='source'
            )
            st.plotly_chart(fig_sources, use_container_width=True)

if __name__ == "__main__":
    main()
