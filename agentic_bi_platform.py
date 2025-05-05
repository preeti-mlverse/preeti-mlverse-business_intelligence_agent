"""
Agentic BI Platform
------------------
This is the main streamlit application file that integrates all components
of the Agentic BI Platform.
"""

import streamlit as st
import pandas as pd
import sqlite3
import os
import numpy as np
import datetime
import re
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, inspect, MetaData, Table
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image
import io
import base64
from langchain.chat_models import ChatAnthropic
from langchain.chains import create_sql_query_chain
from langchain.memory import ConversationBufferMemory
from langchain.prompts import ChatPromptTemplate
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from langchain.tools import Tool
from langchain.utilities import SQLDatabase
from langchain.agents import create_sql_agent
from Data_Agent import DataAgent
import time

# Add these imports
from dotenv import load_dotenv
import os
from utils.llm_utils import initialize_claude, get_schema_description
from utils.sql_generator import nl_to_sql
from utils.insights_generator import generate_insights, determine_visualization_type


# Configure the Streamlit page
st.set_page_config(
    page_title="Agentic BI Platform",
    page_icon="📊",
    layout="wide"
)

# Add custom CSS
def local_css(file_name):
    with open(file_name, "r") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Define CSS styles
css = """
<style>
    /* Sidebar */
    .css-1d391kg {
        background-color: #f5f7f9;
    }
    
    /* Main content */
    .css-18e3th9 {
        padding-top: 1rem;
    }
    
    /* Header */
    .css-1cpxqw2 {
        font-weight: 600;
        color: #1f2937;
    }
    
    /* Navigation buttons */
    .nav-button {
        background-color: white;
        border: 1px solid #e5e7eb;
        border-radius: 50%;
        padding: 10px;
        margin-right: 10px;
        cursor: pointer;
    }
    
    .nav-button.active {
        background-color: #ef4444;
        color: white;
    }
    
    /* Cards */
    .card {
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        background-color: white;
        margin-bottom: 1rem;
    }
    
    /* Welcome banner */
    .welcome-banner {
        background-color: #ecfdf5;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    
    /* Connected database label */
    .connected-db {
        font-size: 0.875rem;
        color: #6b7280;
    }
    
    /* Database icon */
    .db-icon {
        vertical-align: middle;
        margin-right: 0.5rem;
    }
    
    /* Navigation */
    .horizontal-tabs {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 2rem;
    }
    
    .tab-button {
        display: flex;
        align-items: center;
        padding: 0.5rem 1rem;
        border-radius: 9999px;
        cursor: pointer;
        background-color: #f3f4f6;
    }
    
    .tab-button.active {
        background-color: #ef4444;
        color: white;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.875rem;
        font-weight: 600;
        margin-bottom: 1.5rem;
        color: #1f2937;
    }
    
    /* Tables */
    .styled-table {
        width: 100%;
        border-collapse: collapse;
    }
    
    .styled-table th, .styled-table td {
        padding: 0.75rem;
        text-align: left;
        border-bottom: 1px solid #e5e7eb;
    }
    
    .styled-table th {
        background-color: #f9fafb;
        font-weight: 600;
    }
    
    /* Buttons */
    .primary-button {
        background-color: #3b82f6;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 0.375rem;
        font-weight: 500;
        cursor: pointer;
    }
    
    /* Form inputs */
    .form-input {
        width: 100%;
        padding: 0.5rem 0.75rem;
        border: 1px solid #d1d5db;
        border-radius: 0.375rem;
        margin-bottom: 1rem;
    }
</style>
"""

st.markdown(css, unsafe_allow_html=True)

# Initialize session state variables
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'Home'
if 'connected_dbs' not in st.session_state:
    st.session_state.connected_dbs = []
if 'selected_db' not in st.session_state:
    st.session_state.selected_db = None
if 'db_tables' not in st.session_state:
    st.session_state.db_tables = {}
if 'reports' not in st.session_state:
    st.session_state.reports = []

# Function to connect to a SQLite database
def connect_to_sqlite(db_path):
    if not os.path.exists(db_path):
        st.error(f"Database file {db_path} does not exist.")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Function to get database tables and schemas
def get_db_schema(conn):
    if conn is None:
        return {}
    
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    schema = {}
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        column_info = []
        for col in columns:
            column_info.append({
                'name': col[1],
                'type': col[2],
                'notnull': col[3],
                'default': col[4],
                'pk': col[5]
            })
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        
        schema[table_name] = {
            'columns': column_info,
            'row_count': row_count
        }
    
    return schema

# Function to load data from a table
def load_table_data(conn, table_name, limit=100):
    if conn is None:
        return None
    
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Error loading table data: {e}")
        return None

# Function to execute SQL query
def execute_query(conn, query):
    if conn is None:
        return None
    
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# Function to analyze data health
def analyze_data_health(conn, schema):
    if conn is None or not schema:
        return {}
    
    health_report = {}
    
    for table_name, table_info in schema.items():
        table_health = {
            'row_count': table_info['row_count'],
            'column_count': len(table_info['columns']),
            'null_counts': {},
            'data_types': {},
            'last_updated': 'Unknown'
        }
        
        # Check for nulls
        for col in table_info['columns']:
            col_name = col['name']
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL;"
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                null_count = cursor.fetchone()[0]
                table_health['null_counts'][col_name] = null_count
            except:
                table_health['null_counts'][col_name] = 'Error'
        
        # Try to find a timestamp column to determine last updated
        timestamp_columns = [col['name'] for col in table_info['columns'] 
                           if 'time' in col['name'].lower() or 
                              'date' in col['name'].lower() or 
                              '_at' in col['name'].lower()]
        
        if timestamp_columns:
            try:
                query = f"SELECT MAX({timestamp_columns[0]}) FROM {table_name};"
                cursor = conn.cursor()
                cursor.execute(query)
                last_updated = cursor.fetchone()[0]
                if last_updated:
                    table_health['last_updated'] = last_updated
            except:
                pass
        
        health_report[table_name] = table_health
    
    return health_report

# Function to generate a data insight based on natural language query
# Replace the existing generate_insight function with this one
def generate_insight(conn, query, schema):
    """
    Generate insights using the Data Agent.
    
    Args:
        conn: Database connection
        query (str): Natural language query
        schema (dict): Database schema information
        
    Returns:
        dict: Insight information including results and visualization
    """
    try:
        # Create Data Agent instance
        agent = DataAgent()
        
        # Initialize with database schema
        # We need to convert our schema format to match what the agent expects
        agent_schema = {
            'tables': [],
            'relationships': []
        }
        
        # Convert each table in our schema to agent's expected format
        for table_name, table_info in schema.items():
            table_data = {
                'name': table_name,
                'columns': []
            }
            
            # Convert columns
            for col in table_info['columns']:
                col_data = {
                    'name': col['name'],
                    'type': col['type'],
                    'primary_key': col['pk'] == 1
                }
                table_data['columns'].append(col_data)
            
            agent_schema['tables'].append(table_data)
        
        # Get database path for simulation purposes
        db_path = next((db['path'] for db in st.session_state.connected_dbs if db['conn'] == conn), None)
        data_source = {
            'name': 'current_db',
            'type': 'sqlite',
            'connection': db_path
        }
        
        # Initialize agent with schema
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(agent.initialize([data_source]))
        
        # Process the query
        result = loop.run_until_complete(agent.process_query(query))
        
        # Extract results from agent's response
        if 'error' in result:
            return {
                'success': False,
                'message': result['error']
            }
        
        # Convert agent results to DataFrame
        if 'results' in result:
            df = pd.DataFrame(result['results'])
        else:
            df = pd.DataFrame()
        
        # Extract visualization info
        visualization_type = 'table'  # Default
        fig = None
        
        if 'visualization' in result and result['visualization']:
            viz_info = result['visualization']
            viz_type = viz_info.get('type', 'table')
            viz_config = viz_info.get('config', {})
            
            # Create appropriate visualization
            if viz_type == 'line-chart':
                visualization_type = 'line'
                x_axis = viz_config.get('xAxis')
                y_axis = viz_config.get('yAxis')
                if x_axis and y_axis and x_axis in df.columns and y_axis in df.columns:
                    fig = px.line(df, x=x_axis, y=y_axis, title=query)
            
            elif viz_type == 'bar-chart':
                visualization_type = 'bar'
                x_axis = viz_config.get('xAxis')
                y_axis = viz_config.get('yAxis')
                if x_axis and y_axis and x_axis in df.columns and y_axis in df.columns:
                    fig = px.bar(df, x=x_axis, y=y_axis, title=query)
            
            elif viz_type == 'pie-chart':
                visualization_type = 'pie'
                label_column = viz_config.get('labelColumn')
                value_column = viz_config.get('valueColumn')
                if label_column and value_column and label_column in df.columns and value_column in df.columns:
                    fig = px.pie(df, names=label_column, values=value_column, title=query)
        
        # If no visualization was created but we have data, create default visualization
        if fig is None and not df.empty and len(df.columns) >= 2:
            if df[df.columns[0]].dtype == 'datetime64[ns]' or 'date' in df.columns[0].lower() or 'time' in df.columns[0].lower():
                visualization_type = 'line'
                fig = px.line(df, x=df.columns[0], y=df.columns[1], title=query)
            else:
                visualization_type = 'bar'
                fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=query)
        
        return {
            'success': True,
            'data': df,
            'sql_query': result.get('interpreted_query', ''),
            'visualization': fig,
            'type': visualization_type,
            'insights': f"Query processed using the Data Agent. SQL: {result.get('interpreted_query', '')}"
        }
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return {
            'success': False,
            'message': f'Error using Data Agent: {str(e)}\n\nDetails: {error_details}'
        }
# Navigation UI
def render_navigation():
    # Replace 'Collaboration' with 'Connect'
    pages = ['Home', 'Data Source', 'Data Health', 'Analyzer', 'Reports Dashboard', 'Collaboration', 'Settings', 'Logout']
    
    # Create columns with equal width
    cols = st.columns(len(pages))
    
    # Add CSS for uniform buttons
    st.markdown("""
    <style>
    div.stButton > button {
        width: 100%;
        text-align: center;
        height: 46px;
        white-space: normal !important;
        word-wrap: break-word;
        padding: 4px 8px;
        line-height: 1;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    
    div.stButton > button p {
        font-size: 12px;
        margin: 0;
        padding: 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Render buttons in each column
    for i, page in enumerate(pages):
        with cols[i]:
            # Format multi-word buttons with line breaks
            if page == "Data Source":
                button_text = "Data\nSource"
            elif page == "Data Health":
                button_text = "Data\nHealth"
            elif page == "Reports Dashboard":
                button_text = "Reports\nDashboard"
            else:
                button_text = page
            
            # Add dot indicator based on active page
            icon = "●" if st.session_state.current_page == page else "○"
            
            # Create the button with proper styling
            if st.button(f"{icon} {button_text}", key=f"nav_{page}", use_container_width=True):
                st.session_state.current_page = page
                st.rerun()
# Sidebar UI
def render_sidebar():
    st.sidebar.markdown('<div class="welcome-banner">Welcome dufus (CEO)</div>', unsafe_allow_html=True)
    
    st.sidebar.markdown("### Connected Databases")
    
    if st.session_state.connected_dbs:
        for db in st.session_state.connected_dbs:
            st.sidebar.markdown(f"""
            <div>
                <span class="db-icon">📊</span> 
                <span>{db['name']}</span> <span class="connected-db">({db['type']}):</span>
            </div>
            <div class="connected-db">{db['path']}</div>
            """, unsafe_allow_html=True)
    else:
        st.sidebar.write("No databases connected")
    
    st.sidebar.markdown("---")
    
    # Upload SQLite DB section if on Data Source page
    if st.session_state.current_page == 'Data Source':
        st.sidebar.markdown("### Upload SQLite DB")
        uploaded_file = st.sidebar.file_uploader("Upload SQLite DB", type=['db', 'sqlite', 'sqlite3'])
        
        if uploaded_file:
            # Save the uploaded file
            file_path = os.path.join(".", uploaded_file.name)
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            # Connect to the database
            conn = connect_to_sqlite(file_path)
            
            if conn:
                # Get the schema
                schema = get_db_schema(conn)
                
                # Add to session state
                db_info = {
                    'name': uploaded_file.name.split('.')[0],
                    'path': file_path,
                    'type': 'sqlite',
                    'conn': conn,
                    'schema': schema
                }
                
                # Check if already connected
                if not any(db['path'] == file_path for db in st.session_state.connected_dbs):
                    st.session_state.connected_dbs.append(db_info)
                    st.session_state.selected_db = db_info
                    st.success(f"Successfully connected to {uploaded_file.name}")
                    st.rerun()
    
    # Filter reports section if on Reports Dashboard page
    if st.session_state.current_page == 'Reports Dashboard':
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
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Agentic BI Platform v1.0.0")
    st.sidebar.markdown("© 2025 Agentic BI")

# Home page
def render_home_page():
    st.markdown('<h1 class="section-header">📈 BI Agent Dashboard</h1>', unsafe_allow_html=True)
    
    st.markdown('<div class="card">Welcome to Agentic BI. Select a tab to proceed.</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3>👉 Steps to use Agentic BI:</h3>', unsafe_allow_html=True)
        
        st.markdown("""
        1. Go to **Data Source** to upload your SQLite database.
        2. Head to the **Analyzer** tab to define your goal and output types.
        3. Ask questions, generate dashboards or insights.
        4. Save important results to the **Reports Dashboard**.
        5. Collaborate on findings from the **Collaboration** tab.
        """)
        
        if not st.session_state.connected_dbs:
            st.warning("Upload a SQLite DB file from the Data Source tab to continue.")
    
    with col2:
        st.markdown('<h3>Quick Access</h3>', unsafe_allow_html=True)
        
        if st.button("🔍 Start Analyzing Data", key="quick_analyze"):
            st.session_state.current_page = 'Analyzer'
            st.rerun()
        
        if st.button("📊 View Reports", key="quick_reports"):
            st.session_state.current_page = 'Reports Dashboard'
            st.rerun()

# Data Source page
def render_data_source_page():
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

# Data Health page
def render_data_health_page():
    st.markdown('<h1 class="section-header">Data Health Report</h1>', unsafe_allow_html=True)
    
    if not st.session_state.connected_dbs:
        st.warning("No databases connected. Go to Data Source to connect a database.")
        return
    
    # Database selector
    st.markdown("### Choose a connected source")
    db_names = [db['name'] for db in st.session_state.connected_dbs]
    selected_db_name = st.selectbox("", db_names)
    
    # Find the selected database info
    selected_db = next((db for db in st.session_state.connected_dbs if db['name'] == selected_db_name), None)
    
    if selected_db:
        # Generate health report
        health_report = analyze_data_health(selected_db['conn'], selected_db['schema'])
        
        if health_report:
            # Create a table to display health metrics
            table_data = []
            for table_name, health_info in health_report.items():
                # Calculate null percentage
                null_percentage = 0
                if health_info['row_count'] > 0:
                    total_nulls = 0
                    for val in health_info['null_counts'].values():
                        if isinstance(val, (int, float)):
                            total_nulls += val
                        elif isinstance(val, str) and val.isdigit():
                            total_nulls += int(val)
                    total_cells = health_info['row_count'] * health_info['column_count']
                    null_percentage = (total_nulls / total_cells) * 100 if total_cells > 0 else 0
                
                # Format column info for display
                columns_str = ", ".join([f"{col['name']} ({col['type']}) — {health_info['null_counts'].get(col['name'], 0)} nulls" 
                                        for col in selected_db['schema'][table_name]['columns']])
                
                table_data.append({
                    "Table": table_name,
                    "Columns": columns_str,
                    "Rows": health_info['row_count'],
                    "Last Updated": health_info['last_updated']
                })
            
            # Convert to DataFrame for display
            health_df = pd.DataFrame(table_data)
            
            # Display the table
            st.markdown("### Database Health Summary")
            st.dataframe(health_df)
            
            # Display specific table details on click
            if 'selected_table' not in st.session_state:
                st.session_state.selected_table = None
            
            st.markdown("### Table Details")
            selected_table = st.selectbox("Select a table for detailed analysis", 
                                         [""] + list(health_report.keys()),
                                         index=0)
            
            if selected_table:
                table_health = health_report[selected_table]
                table_schema = selected_db['schema'][selected_table]
                
                st.markdown(f"#### {selected_table}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows", table_health['row_count'])
                with col2:
                    st.metric("Columns", table_health['column_count'])
                with col3:
                    null_percentage = 0
                    if table_health['row_count'] > 0:
                        total_nulls = 0
                        for val in health_info['null_counts'].values():
                            if isinstance(val, (int, float)):
                                total_nulls += val
                            elif isinstance(val, str) and val.isdigit():
                                total_nulls += int(val)
                        total_cells = table_health['row_count'] * table_health['column_count']
                        null_percentage = (total_nulls / total_cells) * 100 if total_cells > 0 else 0
                    st.metric("Null %", f"{null_percentage:.1f}%")
                
                # Column null analysis
                st.markdown("#### Column Analysis")
                
                # Prepare data for visualization
                column_names = [col['name'] for col in table_schema['columns']]
                null_counts = [table_health['null_counts'].get(col_name, 0) for col_name in column_names]
                
                # Create a bar chart of null values by column
                fig = px.bar(
                    x=column_names,
                    y=null_counts,
                    labels={'x': 'Column', 'y': 'Null Count'},
                    title=f'Null Values by Column in {selected_table}'
                )
                st.plotly_chart(fig)
                
                # Preview the table data
                st.markdown("#### Data Preview")
                table_data = load_table_data(selected_db['conn'], selected_table, limit=5)
                if table_data is not None:
                    st.dataframe(table_data)
        else:
            st.warning("Could not generate health report for this database.")
    else:
        st.error("Selected database not found.")

# Analyzer page
def render_analyzer_page():
    st.markdown('<h1 class="section-header">🧠 Data Analyzer</h1>', unsafe_allow_html=True)
    
    if not st.session_state.connected_dbs:
        st.warning("No databases connected. Go to Data Source to connect a database.")
        return
    
    # Database selector
    st.markdown("### Data Source")
    db_names = [db['name'] for db in st.session_state.connected_dbs]
    selected_db_name = st.selectbox("", db_names)
    
    # Find the selected database info
    selected_db = next((db for db in st.session_state.connected_dbs if db['name'] == selected_db_name), None)
    
    if selected_db:
        st.markdown(f"Using {selected_db['name']} → {selected_db['path']}")
        
        # Analysis goals and outputs
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🎯 Goals")
            
            # Add goals using multiselect
            goals = st.multiselect(
                "",
                ["Exploration", "Decision Support", "Monitoring", "Prediction", "Optimization"],
                ["Decision Support"]  # Default selection
            )
        
        with col2:
            st.markdown("### 🖼️ Outputs")
            
            # Add output types using multiselect
            outputs = st.multiselect(
                "",
                ["dashboard", "report", "alert", "recommendation", "api"],
                ["dashboard"]  # Default selection
            )
        
        # Natural language query interface
        st.markdown("### 🔍 Ask a question about your data...")
        
        # Check if we have an example query from a previous action
        if 'example_query' in st.session_state:
            query = st.text_input("", value=st.session_state.example_query)
            # Clear the stored example query
            del st.session_state.example_query
        else:
            query_placeholder = "Show me the trend of event activities over time..."
            query = st.text_input("", placeholder=query_placeholder)
        
        if query:
            with st.spinner("Analyzing data with Data Agent..."):
                # Generate insight based on query
                insight = generate_insight(selected_db['conn'], query, selected_db['schema'])
                
                if insight['success']:
                    # Display the visualization if available
                    if 'visualization' in insight and insight['visualization'] is not None:
                        st.plotly_chart(insight['visualization'])
                    else:
                        st.dataframe(insight['data'])
                    
                    # Display insights if available
                    if 'insights' in insight and insight['insights']:
                        st.markdown("### 💡 Key Insights")
                        st.write(insight['insights'])
                    
                    # Display the data
                    with st.expander("View Data"):
                        st.dataframe(insight['data'])
                    
                    # Display the SQL query
                    with st.expander("View SQL Query"):
                        st.code(insight['sql_query'], language="sql")
                    
                    # Option to save to reports
                    if st.button("Save to Reports"):
                        # Generate a timestamp for the report
                        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                        
                        report = {
                            'title': query,
                            'data': insight['data'].to_dict('records'),
                            'sql_query': insight['sql_query'],
                            'visualization_type': insight['type'],
                            'timestamp': timestamp,
                            'database': selected_db['name'],
                            'goal': goals[0] if goals else "Exploration"
                        }
                        
                        st.session_state.reports.append(report)
                        st.success("Report saved successfully!")
                else:
                    st.error(insight['message'])
        
        # Example queries
        st.markdown("### 💡 Example Questions")
        
        example_queries = [
            "Show me the trend of event activities over time",
            "What's the distribution of companies by industry?",
            "Who are our top 10 users by event count?",
            "How many dashboards were created each month?",
            "What's the average engagement time by user type?"
        ]
        
        cols = st.columns(len(example_queries))
        for i, example in enumerate(example_queries):
            if cols[i].button(example, key=f"example_{i}"):
                # Set the query and trigger analysis
                st.session_state.example_query = example
                st.rerun()
# Reports Dashboard page
def render_reports_page():
    st.markdown('<h1 class="section-header">📂 Saved Reports</h1>', unsafe_allow_html=True)
    
    # Display the number of reports
    num_reports = len(st.session_state.reports)
    st.markdown(f"### 📋 Showing {num_reports} of {num_reports} reports")
    
    # Display reports
    if num_reports > 0:
        for i, report in enumerate(st.session_state.reports):
            # Use a main expander for the report
            with st.expander(f"📄 {report['title']} (Exploration) — {report['timestamp']}"):
                # Try to recreate the visualization
                if report['visualization_type'] == 'line':
                    df = pd.DataFrame(report['data'])
                    fig = px.line(df, x=df.columns[0], y=df.columns[1], title=report['title'])
                    st.plotly_chart(fig)
                elif report['visualization_type'] == 'bar':
                    df = pd.DataFrame(report['data'])
                    fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=report['title'])
                    st.plotly_chart(fig)
                elif report['visualization_type'] == 'pie':
                    df = pd.DataFrame(report['data'])
                    fig = px.pie(df, names=df.columns[0], values=df.columns[1], title=report['title'])
                    st.plotly_chart(fig)
                
                # Display the data
                st.dataframe(pd.DataFrame(report['data']))
                
                # Instead of using nested expanders, use a toggle or display directly
                st.markdown("#### SQL Query")
                st.code(report['sql_query'], language="sql")
                
                # Export options
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        "Export CSV",
                        data=pd.DataFrame(report['data']).to_csv(index=False),
                        file_name=f"report_{i}.csv",
                        mime="text/csv"
                    )
                with col2:
                    if st.button("Export PDF", key=f"pdf_{i}"):
                        st.info("In a real application, this would generate a PDF report")
    else:
        st.info("No reports saved yet. Go to the Analyzer tab to create reports.")
# Collaboration page
def render_collaboration_page():
    st.markdown('<h1 class="section-header">🔗 App & Tool Integrations</h1>', unsafe_allow_html=True)
    
    st.markdown("### 🔌 Connect New Tool")
    
    # Tool selection
    tools = ["Slack", "Microsoft Teams", "Email", "Jira", "Trello", "GitHub", "Notion", "Google Drive"]
    selected_tool = st.selectbox("Choose an app or tool", tools)
    
    # Display connection form based on selected tool
    if selected_tool == "Slack":
        st.text_input("Webhook URL")
        
        if st.button("🔒 Save Connection"):
            st.success("Slack integration saved successfully!")
    
    elif selected_tool == "Email":
        col1, col2 = st.columns(2)
        with col1:
            smtp_server = st.text_input("SMTP Server")
            smtp_port = st.text_input("SMTP Port")
        with col2:
            email_address = st.text_input("Email Address")
            password = st.text_input("Password", type="password")
        
        if st.button("🔒 Save Connection"):
            st.success("Email integration saved successfully!")
    
    elif selected_tool == "GitHub":
        github_token = st.text_input("GitHub Personal Access Token")
        repository = st.text_input("Repository URL (optional)")
        
        if st.button("🔒 Save Connection"):
            st.success("GitHub integration saved successfully!")
    
    # Existing integrations
    st.markdown("---")
    st.markdown("### Existing Integrations")
    
    # Dummy data for demonstration
    st.info("No integrations configured yet.")

# Settings page
def render_settings_page():
    st.markdown('<h1 class="section-header">⚙️ Settings & User Management</h1>', unsafe_allow_html=True)
    
    # Tabs for settings sections
    tab1, tab2 = st.tabs(["👤 My Profile", "👥 Manage Users"])
    
    with tab1:
        st.markdown("### Update Your Profile")
        
        display_name = st.text_input("Display Name")
        email = st.text_input("Email")
        
        role_options = ["Admin", "Analyst", "Viewer"]
        role = st.selectbox("Role", role_options)
        
        if st.button("💾 Save Profile"):
            st.success("Profile updated successfully!")
    
    with tab2:
        st.markdown("### User Management")
        
        # Dummy data for demonstration
        users_data = [
            {"name": "Dufus", "email": "dufus@example.com", "role": "Admin", "last_active": "2025-05-04"},
            {"name": "Jane Smith", "email": "jane@example.com", "role": "Analyst", "last_active": "2025-05-03"},
            {"name": "Bob Johnson", "email": "bob@example.com", "role": "Viewer", "last_active": "2025-05-01"}
        ]
        
        users_df = pd.DataFrame(users_data)
        st.dataframe(users_df)
        
        st.markdown("### Add New User")
        
        col1, col2 = st.columns(2)
        with col1:
            new_name = st.text_input("Name")
            new_email = st.text_input("Email", key="new_email")
        with col2:
            new_role = st.selectbox("Role", role_options, key="new_role")
        
        if st.button("➕ Add User"):
            st.success("User added successfully!")

# Main app function
def main():
    render_sidebar()
    render_navigation()
    
    # Render the appropriate page based on current_page
    if st.session_state.current_page == 'Home':
        render_home_page()
    elif st.session_state.current_page == 'Data Source':
        render_data_source_page()
    elif st.session_state.current_page == 'Data Health':
        render_data_health_page()
    elif st.session_state.current_page == 'Analyzer':
        render_analyzer_page()
    elif st.session_state.current_page == 'Reports Dashboard':
        render_reports_page()
    elif st.session_state.current_page == 'Collaboration':
        render_collaboration_page()
    elif st.session_state.current_page == 'Settings':
        render_settings_page()
    elif st.session_state.current_page == 'Logout':
        st.warning("In a real application, this would log you out.")
        # Reset to home after 2 seconds
        time.sleep(2)
        st.session_state.current_page = 'Home'
        st.rerun()

if __name__ == "__main__":
    main()