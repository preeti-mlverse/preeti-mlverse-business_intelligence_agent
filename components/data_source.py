import streamlit as st
import sqlite3
import os
from utils.data_utilities import DatabaseManager

def render_data_source_page():
    '''
    Render the data source page with connection options and configurations.
    '''
    st.markdown('<h1 class="section-header">🔌 Connect Data Sources</h1>', unsafe_allow_html=True)
    
    st.markdown("### Select Data Sources to Connect")
    
    # Dropdown for selecting data source type
    data_source_options = ["SQLite", "Postgres", "MySQL", "Snowflake", "MongoDB", "AWS RDS", "GCP SQL", "Kafka"]
    selected_source = st.selectbox("Choose an option", data_source_options)
    
    if selected_source == "SQLite":
        st.markdown("### 🔧 Configure SQLite")
        
        # If we already have a connected SQLite DB, show it
        if st.session_state.connected_dbs:
            st.markdown("### Connected Sources")
            
            for db in st.session_state.connected_dbs:
                st.markdown(f"✅ {db['name']} – Connected")
    else:
        # For other data sources, show connection form
        st.markdown(f"### Configure {selected_source} Connection")
        
        if selected_source == "Postgres":
            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input("Host")
                port = st.text_input("Port", "5432")
            with col2:
                database = st.text_input("Database")
                schema = st.text_input("Schema", "public")
            
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.button("Test Connection"):
                st.info("This would test the connection in a real application")
        
        elif selected_source == "MySQL":
            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input("Host")
                port = st.text_input("Port", "3306")
            with col2:
                database = st.text_input("Database")
            
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.button("Test Connection"):
                st.info("This would test the connection in a real application")
        
        elif selected_source == "Slack":
            webhook_url = st.text_input("Webhook URL")
            
            if st.button("Save Connection"):
                st.success("Slack integration saved successfully!")