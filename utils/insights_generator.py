"""
Utility functions for generating insights from query results.
"""

import pandas as pd
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from .llm_utils import initialize_claude

def generate_insights(query, sql_query, result_df):
    """Generate insights from query results using Claude."""
    try:
        llm = initialize_claude()
        
        # Prepare a summary of the data
        num_rows = len(result_df)
        num_cols = len(result_df.columns)
        column_info = ", ".join(result_df.columns)
        
        # For large dataframes, provide a sample
        if num_rows > 10:
            data_sample = result_df.head(10).to_string()
            data_description = f"The result has {num_rows} rows and {num_cols} columns: {column_info}. Here's a sample of the first 10 rows:\n{data_sample}"
        else:
            data_description = f"The result has {num_rows} rows and {num_cols} columns: {column_info}. Here's the complete result:\n{result_df.to_string()}"
        
        # Create a prompt for insight generation
        messages = [
            SystemMessage(content="""You are a data analyst providing insights on query results.
            Analyze the data and provide 2-3 key insights in bullet points.
            Also recommend the best visualization type for this data: line, bar, pie, scatter, or table.
            Format your response with an 'INSIGHTS:' section followed by bullet points, 
            and a 'VISUALIZATION:' section with your recommended type."""),
            HumanMessage(content=f"""User query: {query}
            SQL query: {sql_query}
            
            {data_description}
            
            Please provide key insights and recommend a visualization type.""")
        ]
        
        # Get insights from Claude
        response = llm.invoke(messages)
        insights_text = response.content
        
        # Extract visualization type from response
        viz_type = 'table'  # Default
        if "VISUALIZATION:" in insights_text:
            viz_section = insights_text.split("VISUALIZATION:")[1].lower()
            if "line" in viz_section:
                viz_type = 'line'
            elif "bar" in viz_section:
                viz_type = 'bar'
            elif "pie" in viz_section:
                viz_type = 'pie'
            elif "scatter" in viz_section:
                viz_type = 'scatter'
        
        return {
            'insights': insights_text,
            'visualization_type': viz_type
        }
    except Exception as e:
        return {
            'insights': f"Error generating insights: {str(e)}",
            'visualization_type': 'table'
        }

def determine_visualization_type(insights, df):
    """Extract visualization type recommendation from insights."""
    # This function is a fallback if the visualization type wasn't properly extracted
    # from the insights response
    
    if len(df) == 0:
        return 'table'
    
    # Default logic based on data structure
    num_rows = len(df)
    num_cols = len(df.columns)
    
    # Single value results
    if num_rows == 1 and num_cols == 1:
        return 'value'
    
    # For 2 columns, check if one might be a date/time
    if num_cols >= 2:
        first_col = df.columns[0]
        if df[first_col].dtype == 'datetime64[ns]' or 'date' in first_col.lower() or 'time' in first_col.lower():
            return 'line'
    
    # Few categories with numeric values
    if num_cols >= 2 and num_rows <= 10:
        return 'bar'
    
    # Many rows
    if num_rows > 10:
        return 'table'
    
    return 'table'