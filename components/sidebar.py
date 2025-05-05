import streamlit as st
import os
import sqlite3
from utils.data_utilities import DatabaseManager, SchemaAnalyzer

def render_sidebar():
    '''
    Render the sidebar with connected databases and page-specific controls.
    '''
    st.sidebar.markdown('<div class="welcome-banner">Welcome dufus (CEO)</div>', unsafe_allow_html=True)
    
    st.sidebar.markdown("### Connected Databases")
    
    if st.session_state.connected_dbs:
        for db in st.session_state.connected_dbs:
            st.sidebar.markdown(f'''
            <div>
                <span class="db-icon">📊</span> 
                <span>{db['name']}</span> <span class="connected-db">({db['type']}):</span>
            </div>
            <div class="connected-db">{db['path']}</div>
            ''', unsafe_allow_html=True)
    else:
        st.sidebar.write("No databases connected")
    
    st.sidebar.markdown("---")
    
    # Upload SQLite DB section if on Data Source page
    if st.session_state.current_page == 'Data Source':
        render_data_source_sidebar()
    
    # Filter reports section if on Reports Dashboard page
    elif st.session_state.current_page == 'Reports Dashboard':
        render_reports_sidebar()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Agentic BI Platform v1.0.0")
    st.sidebar.markdown("© 2025 Agentic BI")

def render_data_source_sidebar():
    '''
    Render the sidebar for the Data Source page.
    '''
    st.sidebar.markdown("### Upload SQLite DB")
    uploaded_file = st.sidebar.file_uploader("Upload SQLite DB", type=['db', 'sqlite', 'sqlite3'])
    
    if uploaded_file:
        # Save the uploaded file
        file_path = os.path.join(".", uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        # Connect to the database
        db_manager = DatabaseManager()
        conn_info = db_manager.connect_sqlite(file_path)
        
        if conn_info:
            # Add to session state
            # Check if already connected
            if not any(db['path'] == file_path for db in st.session_state.connected_dbs):
                st.session_state.connected_dbs.append(conn_info)
                st.session_state.selected_db = conn_info
                st.success(f"Successfully connected to {uploaded_file.name}")
                st.experimental_rerun()
        else:
            st.error(f"Failed to connect to {uploaded_file.name}")

def render_reports_sidebar():
    '''
    Render the sidebar for the Reports Dashboard page.
    '''
    st.sidebar.markdown("### Filter Reports")
    
    st.sidebar.markdown("Date Range")
    date_range = st.sidebar.text_input("YYYY/MM/DD - YYYY/MM/DD")
    
    st.sidebar.markdown("Goal")
    goal_option = st.sidebar.selectbox("Choose an option", 
                                      ["", "Exploration", "Decision Support", "Monitoring"], 
                                      index=0,
                                      key="goal_filter")
    
    st.sidebar.markdown("Chart Type")
    chart_option = st.sidebar.selectbox("Choose an option", 
                                       ["", "Line Chart", "Bar Chart", "Pie Chart", "Table"], 
                                       index=0,
                                       key="chart_filter")
    
    st.sidebar.markdown("Search by keyword...")
    keyword_search = st.sidebar.text_input("", key="keyword_search")
