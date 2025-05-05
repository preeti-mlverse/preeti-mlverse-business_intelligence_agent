import streamlit as st
import pandas as pd
import plotly.express as px
import uuid
from models.report import Report, ReportManager
import datetime

def render_reports_page():
    '''
    Render the reports page with saved reports and filtering options.
    '''
    st.markdown('<h1 class="section-header">📂 Saved Reports</h1>', unsafe_allow_html=True)
    
    # Initialize or get report manager
    if 'reports_manager' not in st.session_state:
        from models.report import ReportManager
        st.session_state.reports_manager = ReportManager()
    
    # Get reports
    reports = list(st.session_state.reports_manager.reports.values())
    
    # Apply filters if any
    if 'goal_filter' in st.session_state and st.session_state.goal_filter:
        reports = [r for r in reports if r.goal == st.session_state.goal_filter]
    
    if 'chart_filter' in st.session_state and st.session_state.chart_filter:
        viz_type_map = {
            "Line Chart": "line",
            "Bar Chart": "bar",
            "Pie Chart": "pie",
            "Table": "table"
        }
        viz_type = viz_type_map.get(st.session_state.chart_filter)
        if viz_type:
            reports = [r for r in reports if r.config.visualization_type == viz_type]
    
    if 'keyword_search' in st.session_state and st.session_state.keyword_search:
        keyword = st.session_state.keyword_search.lower()
        reports = [r for r in reports if keyword in r.title.lower() or keyword in r.description.lower()]
    
    # Display the number of reports
    num_reports = len(reports)
    st.markdown(f"### 📋 Showing {num_reports} of {len(st.session_state.reports_manager.reports)} reports")
    
    # Display reports
    if num_reports > 0:
        for i, report in enumerate(reports):
            with st.expander(f"📄 {report.title} ({report.goal}) — {report.created_at.split('T')[0]}"):
                # Try to recreate the visualization
                try:
                    df = pd.DataFrame(report.data)
                    
                    if report.config.visualization_type == 'line':
                        fig = px.line(
                            df, 
                            x=report.config.x_axis or df.columns[0], 
                            y=report.config.y_axis or df.columns[1], 
                            title=report.title
                        )
                        st.plotly_chart(fig)
                    
                    elif report.config.visualization_type == 'bar':
                        fig = px.bar(
                            df, 
                            x=report.config.x_axis or df.columns[0], 
                            y=report.config.y_axis or df.columns[1], 
                            title=report.title
                        )
                        st.plotly_chart(fig)
                    
                    elif report.config.visualization_type == 'pie':
                        fig = px.pie(
                            df, 
                            names=report.config.x_axis or df.columns[0], 
                            values=report.config.y_axis or df.columns[1], 
                            title=report.title
                        )
                        st.plotly_chart(fig)
                    
                    else:  # Default to table
                        st.dataframe(df)
                except Exception as e:
                    st.error(f"Error rendering visualization: {e}")
                    st.dataframe(pd.DataFrame(report.data))
                
                # Display the SQL query
                with st.expander("View SQL Query"):
                    st.code(report.sql_query, language="sql")
                
                # Export options
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        "Export CSV",
                        data=pd.DataFrame(report.data).to_csv(index=False),
                        file_name=f"report_{report.id}.csv",
                        mime="text/csv"
                    )
                with col2:
                    if st.button("Export PDF", key=f"pdf_{report.id}"):
                        st.info("In a real application, this would generate a PDF report")
                
                # Delete option
                if st.button("Delete Report", key=f"delete_{report.id}"):
                    st.session_state.reports_manager.remove_report(report.id)
                    st.success("Report deleted successfully!")
                    st.experimental_rerun()
    else:
        st.info("No reports found with the current filters. Try different filters or create new reports in the Analyzer tab.")