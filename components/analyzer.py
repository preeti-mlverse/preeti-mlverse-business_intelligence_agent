import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import uuid
from utils.data_utilities import DataAnalyzer
from models.report import Report, ReportConfig

def render_analyzer_page():
    '''
    Render the analyzer page with natural language query interface and visualizations.
    '''
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
        
        # Check if we have a example query from a previous action
        if 'example_query' in st.session_state:
            query = st.text_input("", value=st.session_state.example_query)
            # Clear the stored example query
            del st.session_state.example_query
        else:
            query_placeholder = "Show me the trend of event activities over time..."
            query = st.text_input("", placeholder=query_placeholder)
        
        if query:
            with st.spinner("Analyzing data..."):
                # Initialize the data analyzer
                data_analyzer = DataAnalyzer(selected_db['conn'], selected_db['schema'])
                
                # Generate insight based on query
                insight = data_analyzer.generate_insight(query)
                
                if insight['success']:
                    # Determine visualization type
                    viz_type = insight.get('visualization_type', 'table')
                    
                    # Display the visualization
                    if viz_type == 'line':
                        fig = px.line(
                            insight['data'], 
                            x=insight.get('x_axis', insight['data'].columns[0]), 
                            y=insight.get('y_axis', insight['data'].columns[1]), 
                            title=query
                        )
                        st.plotly_chart(fig)
                    
                    elif viz_type == 'bar':
                        fig = px.bar(
                            insight['data'], 
                            x=insight.get('x_axis', insight['data'].columns[0]), 
                            y=insight.get('y_axis', insight['data'].columns[1]), 
                            title=query
                        )
                        st.plotly_chart(fig)
                    
                    elif viz_type == 'pie':
                        fig = px.pie(
                            insight['data'], 
                            names=insight.get('x_axis', insight['data'].columns[0]), 
                            values=insight.get('y_axis', insight['data'].columns[1]), 
                            title=query
                        )
                        st.plotly_chart(fig)
                    
                    else:  # Default to table
                        st.dataframe(insight['data'])
                    
                    # Display the data
                    with st.expander("View Data"):
                        st.dataframe(insight['data'])
                    
                    # Display the SQL query
                    with st.expander("View SQL Query"):
                        st.code(insight['query'], language="sql")
                    
                    # Option to save to reports
                    if st.button("Save to Reports"):
                        # Generate a unique ID for the report
                        report_id = str(uuid.uuid4())
                        
                        # Create report config
                        config = ReportConfig(
                            visualization_type=viz_type,
                            x_axis=insight.get('x_axis'),
                            y_axis=insight.get('y_axis'),
                            title=query
                        )
                        
                        # Create the report
                        report = Report(
                            id=report_id,
                            title=query,
                            description=insight.get('explanation', ''),
                            query=query,
                            sql_query=insight['query'],
                            data=insight['data'].to_dict('records'),
                            config=config,
                            database=selected_db['name'],
                            goal=goals[0] if goals else "Exploration"
                        )
                        
                        # Add to reports list
                        if 'reports_manager' not in st.session_state:
                            from models.report import ReportManager
                            st.session_state.reports_manager = ReportManager()
                        
                        st.session_state.reports_manager.add_report(report)
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
                st.experimental_rerun()