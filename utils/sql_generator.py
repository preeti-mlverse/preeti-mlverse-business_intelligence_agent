"""
Utility functions for generating SQL from natural language.
"""

from langchain.chains import create_sql_query_chain
from langchain.utilities import SQLDatabase
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from .llm_utils import initialize_claude

def nl_to_sql(query, db_uri, schema_description):
    """Convert natural language to SQL using Claude."""
    try:
        db = SQLDatabase.from_uri(db_uri)
        llm = initialize_claude()
        
        # Create a prompt for SQL generation
        messages = [
            SystemMessage(content=f"""You are an expert SQL query generator.
            Your task is to convert natural language questions into SQL queries.
            Use only the tables and columns described in the schema below.
            If the query is ambiguous, make reasonable assumptions.
            Return ONLY the SQL query without any explanations.
            
            DATABASE SCHEMA:
            {schema_description}"""),
            HumanMessage(content=f"Generate a SQL query to answer this question: {query}")
        ]
        
        # Generate SQL
        response = llm.invoke(messages)
        sql_query = response.content.strip()
        
        # Clean up the SQL - remove any markdown formatting if present
        if sql_query.startswith("```sql"):
            sql_query = sql_query.split("```sql")[1]
        if sql_query.endswith("```"):
            sql_query = sql_query.split("```")[0]
        
        return sql_query.strip()
    except Exception as e:
        raise Exception(f"Error generating SQL: {str(e)}")