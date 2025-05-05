"""
Utility functions for working with Claude LLM.
"""

import os
from langchain.chat_models import ChatAnthropic
from langchain.schema import HumanMessage, AIMessage, SystemMessage
from langchain.utilities import SQLDatabase

def initialize_claude(model_name="claude-3-opus-20240229", temperature=0):
    """Initialize Claude LLM."""
    return ChatAnthropic(
        model=model_name,
        temperature=temperature,
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY")
    )

def get_schema_description(db_uri):
    """Generate a natural language description of the database schema."""
    try:
        db = SQLDatabase.from_uri(db_uri)
        llm = initialize_claude()
        
        # Get raw schema information
        schema_info = db.get_table_info()
        
        # Ask Claude to analyze the schema and identify relationships
        messages = [
            SystemMessage(content="""You are a database expert analyzing schema information.
            Your task is to analyze the database schema and provide a clear, structured description.
            Identify tables, columns, primary keys, foreign keys, and relationships between tables.
            Format your response in a way that will be useful for SQL query generation."""),
            HumanMessage(content=f"Analyze this database schema and identify tables, columns, primary keys, foreign keys, and relationships:\n\n{schema_info}")
        ]
        
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        return f"Error analyzing schema: {str(e)}"