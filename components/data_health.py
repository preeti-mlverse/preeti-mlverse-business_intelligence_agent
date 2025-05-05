import streamlit as st
import pandas as pd
import plotly.express as px
from utils.data_utilities import SchemaAnalyzer

def render_data_health_page():
    '''
    Render the data health page with database health metrics and analysis.
    '''
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
        schema_analyzer = SchemaAnalyzer()
        health_report = {}
        
        for table_name, table_info in selected_db['schema'].items():
            health_report[table_name] = schema_analyzer.analyze_table_health(selected_db['conn'], table_name, table_info)
        
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
    # Skip values like 'Error' that can't be converted to numbers
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
                        total_nulls = sum(table_health['null_counts'].values())
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
                query = f"SELECT * FROM {selected_table} LIMIT 5;"
                try:
                    df = pd.read_sql_query(query, selected_db['conn'])
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Error retrieving data: {e}")
        else:
            st.warning("Could not generate health report for this database.")
    else:
        st.error("Selected database not found.")