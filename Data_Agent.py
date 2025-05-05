"""
Agentic BI Platform - Data Agent
-------------------------------
Main agent class for handling data queries and visualization.
"""

import sqlite3
import pandas as pd
import numpy as np
import re
import json
import asyncio
from datetime import datetime
import os

class DataAgent:
    """Main agent class for handling data queries and visualization."""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
        self.query_transformer = QueryTransformer()
        self.context_manager = ContextManager()
        self.visualization_engine = VisualizationEngine()
    
    async def initialize(self, data_sources):
        """Initialize the agent with the provided data sources."""
        await self.schema_manager.load_schemas(data_sources)
        print(f"Agent initialized with schemas: {self.schema_manager.get_schema_overview()}")
    
    async def process_query(self, query):
        """Process a natural language query and return results with visualization."""
        try:
            # Update context with the new query
            self.context_manager.update_context(query)
            
            # Transform natural language to SQL
            sql_query = await self.query_transformer.transform_to_sql(
                query,
                self.schema_manager.get_schemas(),
                self.context_manager.get_context()
            )
            
            print(f"Generated SQL: {sql_query}")
            
            # Execute the SQL query
            results = await self.execute_query(sql_query)
            
            # Update context with results
            self.context_manager.update_with_results(results)
            
            # Determine the best visualization
            visualization = self.visualization_engine.recommend_visualization(results, query)
            
            # Return the complete response
            return {
                'results': results,
                'visualization': visualization,
                'interpreted_query': sql_query
            }
        except Exception as e:
            print(f"Error processing query: {e}")
            return {
                'error': "Failed to process query",
                'details': str(e)
            }
    
    async def execute_query(self, sql_query):
        """Execute the SQL query against the connected data source."""
        # This is a simulation for demo purposes
        # In a real implementation, this would connect to a database
        return self._simulate_query_execution(sql_query)
    
    def _simulate_query_execution(self, sql_query):
        """Simulate SQL query execution for demonstration."""
        print(f"Executing SQL: {sql_query}")
        
        # Simple pattern matching to return appropriate mock data
        if "COUNT" in sql_query:
            return [{"count": 42}]
        elif "SUM" in sql_query:
            return [{"sum": 1234.56}]
        elif "AVG" in sql_query:
            return [{"average": 78.9}]
        else:
            # Default mock data
            return [
                {"id": 1, "name": "Item 1", "value": 100, "date": "2023-01-01"},
                {"id": 2, "name": "Item 2", "value": 200, "date": "2023-02-01"},
                {"id": 3, "name": "Item 3", "value": 300, "date": "2023-03-01"}
            ]


class SchemaManager:
    """Manages database schema information."""
    
    def __init__(self):
        self.schemas = {}
    
    async def load_schemas(self, data_sources):
        """Load schema information from all connected data sources."""
        for source in data_sources:
            schema = await self.introspect_data_source(source)
            self.schemas[source['name']] = schema
    
    async def introspect_data_source(self, data_source):
        """Extract schema information from a data source."""
        # In a real implementation, this would connect to the database
        # and extract schema information
        
        # Mock implementation for demonstration
        tables = await self.discover_tables(data_source)
        relationships = await self.discover_relationships(data_source)
        
        return {
            'tables': tables,
            'relationships': relationships
        }
    
    async def discover_tables(self, data_source):
        """Discover tables and columns in a data source."""
        print(f"Discovering tables for {data_source['name']}")
        
        # Mock implementation - would query database metadata in reality
        if data_source['name'] == 'sales':
            return [
                {
                    'name': 'sales',
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                        {'name': 'date', 'type': 'DATE', 'primary_key': False},
                        {'name': 'amount', 'type': 'DECIMAL', 'primary_key': False},
                        {'name': 'product_id', 'type': 'INTEGER', 'primary_key': False}
                    ]
                },
                {
                    'name': 'products',
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                        {'name': 'name', 'type': 'TEXT', 'primary_key': False},
                        {'name': 'category', 'type': 'TEXT', 'primary_key': False},
                        {'name': 'price', 'type': 'DECIMAL', 'primary_key': False}
                    ]
                }
            ]
        elif data_source['name'] == 'customers':
            return [
                {
                    'name': 'customers',
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                        {'name': 'name', 'type': 'TEXT', 'primary_key': False},
                        {'name': 'email', 'type': 'TEXT', 'primary_key': False},
                        {'name': 'signup_date', 'type': 'DATE', 'primary_key': False}
                    ]
                },
                {
                    'name': 'orders',
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
                        {'name': 'customer_id', 'type': 'INTEGER', 'primary_key': False},
                        {'name': 'order_date', 'type': 'DATE', 'primary_key': False},
                        {'name': 'total', 'type': 'DECIMAL', 'primary_key': False}
                    ]
                }
            ]
        else:
            return []
    
    async def discover_relationships(self, data_source):
        """Discover relationships between tables."""
        print(f"Discovering relationships for {data_source['name']}")
        
        # Mock implementation - would query foreign key constraints in reality
        if data_source['name'] == 'sales':
            return [
                {
                    'table': 'sales',
                    'column': 'product_id',
                    'references_table': 'products',
                    'references_column': 'id'
                }
            ]
        elif data_source['name'] == 'customers':
            return [
                {
                    'table': 'orders',
                    'column': 'customer_id',
                    'references_table': 'customers',
                    'references_column': 'id'
                }
            ]
        else:
            return []
    
    def get_schemas(self):
        """Get all schema information."""
        return self.schemas
    
    def get_schema_overview(self):
        """Get a simplified overview of the schemas."""
        overview = {}
        for source_name, schema in self.schemas.items():
            overview[source_name] = {
                'table_count': len(schema['tables']),
                'relationship_count': len(schema['relationships'])
            }
        return overview


class QueryTransformer:
    """Transforms natural language queries to SQL."""
    
    def __init__(self):
        self.templates = self._load_query_templates()
    
    def _load_query_templates(self):
        """Load common query templates for pattern matching."""
        return [
            {
                'patterns': ["show me", "list", "display", "what are"],
                'sql_template': "SELECT {columns} FROM {table} {where_clause} {limit_clause}"
            },
            {
                'patterns': ["count", "how many"],
                'sql_template': "SELECT COUNT({columns}) FROM {table} {where_clause}"
            },
            {
                'patterns': ["sum", "total"],
                'sql_template': "SELECT SUM({columns}) FROM {table} {where_clause}"
            },
            {
                'patterns': ["average", "avg", "mean"],
                'sql_template': "SELECT AVG({columns}) FROM {table} {where_clause}"
            }
        ]
    
    async def transform_to_sql(self, query, schemas, context):
        """Transform a natural language query to SQL."""
        # In a real implementation, this would use NLP and more sophisticated techniques
        # This is a simplified version that uses pattern matching
        
        entities = self._extract_entities(query, schemas)
        query_intent = self._determine_query_intent(query)
        
        return self._build_sql_query(query_intent, entities, context)
    
    def _extract_entities(self, query, schemas):
        """Extract mentioned tables and columns from the query."""
        entities = {
            'tables': [],
            'columns': [],
            'conditions': []
        }
        
        query_lower = query.lower()
        
        # For each data source and schema
        for source_name, schema in schemas.items():
            # For each table in the schema
            for table in schema['tables']:
                table_name = table['name']
                if table_name.lower() in query_lower:
                    entities['tables'].append(table_name)
                    
                    # Check for mentioned columns
                    for column in table['columns']:
                        column_name = column['name']
                        if column_name.lower() in query_lower:
                            entities['columns'].append({
                                'table': table_name,
                                'column': column_name
                            })
        
        # If no columns specified but tables are, use * for all columns
        if not entities['columns'] and entities['tables']:
            entities['columns'].append({
                'table': entities['tables'][0],
                'column': '*'
            })
        
        return entities
    
    def _determine_query_intent(self, query):
        """Determine the intent of the query (SELECT, COUNT, etc.)."""
        query_lower = query.lower()
        
        if "how many" in query_lower or "count" in query_lower:
            return "COUNT"
        elif "sum" in query_lower or "total" in query_lower:
            return "SUM"
        elif "average" in query_lower or "avg" in query_lower or "mean" in query_lower:
            return "AVG"
        else:
            return "SELECT"
    
    def _build_sql_query(self, intent, entities, context):
        """Build an SQL query based on the intent and extracted entities."""
        if not entities['tables']:
            # If no specific table was identified, try to use context
            if context['current_tables']:
                entities['tables'] = [context['current_tables'][0]]
            else:
                # No table identified and no context to help
                return "SELECT 1"  # Default query that should return something
        
        table = entities['tables'][0]  # Use the first table for simplicity
        
        # Build the query based on intent
        if intent == "COUNT":
            if entities['columns'] and entities['columns'][0]['column'] != '*':
                column = entities['columns'][0]['column']
                sql = f"SELECT COUNT({column}) FROM {table}"
            else:
                sql = f"SELECT COUNT(*) FROM {table}"
        
        elif intent == "SUM":
            if entities['columns'] and entities['columns'][0]['column'] != '*':
                column = entities['columns'][0]['column']
                sql = f"SELECT SUM({column}) FROM {table}"
            else:
                # Default to the first numeric column if none specified
                sql = f"SELECT SUM(*) FROM {table}"
        
        elif intent == "AVG":
            if entities['columns'] and entities['columns'][0]['column'] != '*':
                column = entities['columns'][0]['column']
                sql = f"SELECT AVG({column}) FROM {table}"
            else:
                # Default to the first numeric column if none specified
                sql = f"SELECT AVG(*) FROM {table}"
        
        else:  # Default to SELECT
            if entities['columns']:
                # Format the columns list
                columns = ", ".join([f"{col['table']}.{col['column']}" if col['column'] != '*' else "*" 
                                  for col in entities['columns']])
                sql = f"SELECT {columns} FROM {table}"
            else:
                sql = f"SELECT * FROM {table}"
        
        # Add WHERE clause if there are conditions or context filters
        where_clause = self._build_where_clause(entities['conditions'], context)
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        return sql
    
    def _build_where_clause(self, conditions, context):
        """Build WHERE clause based on conditions and context."""
        # Use context filters if available
        if context['filters']:
            return context['filters'][0]
        
        # Otherwise build from explicit conditions
        if conditions:
            return " AND ".join(conditions)
        
        return ""


class ContextManager:
    """Manages conversation context for follow-up questions."""
    
    def __init__(self):
        self.context = {
            'current_tables': [],
            'previous_queries': [],
            'filters': [],
            'recent_values': {}
        }
    
    def update_context(self, query):
        """Update context with a new query."""
        # Keep track of query history
        self.context['previous_queries'].append(query)
        
        # Limit history to the most recent 5 queries
        if len(self.context['previous_queries']) > 5:
            self.context['previous_queries'].pop(0)
        
        # Extract tables, filters, etc. from the query
        # This is a simplified implementation
        query_lower = query.lower()
        
        # Look for table references
        tables = []
        common_tables = ['sales', 'products', 'customers', 'orders']
        for table in common_tables:
            if table in query_lower:
                tables.append(table)
        
        if tables:
            self.context['current_tables'] = tables
        
        print(f"Updated context with query: {query}")
    
    def update_with_results(self, results):
        """Update context with query results."""
        if results and len(results) > 0:
            # Store values from the first result for potential reference
            first_result = results[0]
            for key, value in first_result.items():
                self.context['recent_values'][key] = value
    
    def get_context(self):
        """Get the current context."""
        return self.context


class VisualizationEngine:
    """Recommends and creates visualizations based on data and query."""
    
    def __init__(self):
        self.visualization_types = [
            "table",
            "bar-chart",
            "line-chart",
            "pie-chart",
            "scatter-plot",
            "heatmap"
        ]
    
    def recommend_visualization(self, results, query):
        """Recommend a visualization type based on the data and query."""
        if not results or len(results) == 0:
            return None
        
        # Analyze data structure
        data_structure = self._analyze_data_structure(results)
        
        # Look for visualization hints in the query
        query_lower = query.lower()
        
        # Detect time series for line charts
        if data_structure['has_date_column'] and data_structure['has_numeric_column'] and \
           ("over time" in query_lower or "trend" in query_lower):
            return {
                'type': "line-chart",
                'config': self._generate_line_chart_config(results, data_structure)
            }
        
        # Detect categorical data for bar charts
        if data_structure['has_categorical_column'] and data_structure['has_numeric_column'] and \
           ("compare" in query_lower or "by category" in query_lower):
            return {
                'type': "bar-chart",
                'config': self._generate_bar_chart_config(results, data_structure)
            }
        
        # Detect distribution data for pie charts
        if data_structure['has_categorical_column'] and data_structure['has_numeric_column'] and \
           ("distribution" in query_lower or "percentage" in query_lower):
            return {
                'type': "pie-chart",
                'config': self._generate_pie_chart_config(results, data_structure)
            }
        
        # Default to table view
        return {
            'type': "table",
            'config': self._generate_table_config(results)
        }
    
    def _analyze_data_structure(self, results):
        """Analyze the structure of the data."""
        structure = {
            'column_count': 0,
            'row_count': len(results),
            'columns': [],
            'has_date_column': False,
            'has_numeric_column': False,
            'has_categorical_column': False,
            'date_columns': [],
            'numeric_columns': [],
            'categorical_columns': []
        }
        
        if results and len(results) > 0:
            first_row = results[0]
            structure['column_count'] = len(first_row)
            
            # Analyze each column
            for column_name, value in first_row.items():
                column_type = self._infer_column_type(value, column_name)
                
                structure['columns'].append({
                    'name': column_name,
                    'type': column_type
                })
                
                # Update flags based on column type
                if column_type == "date":
                    structure['has_date_column'] = True
                    structure['date_columns'].append(column_name)
                elif column_type == "numeric":
                    structure['has_numeric_column'] = True
                    structure['numeric_columns'].append(column_name)
                else:
                    structure['has_categorical_column'] = True
                    structure['categorical_columns'].append(column_name)
        
        return structure
    
    def _infer_column_type(self, value, column_name):
        """Infer the type of a column based on its value and name."""
        # Check if it's a date column
        column_name_lower = column_name.lower()
        if "date" in column_name_lower or "time" in column_name_lower or \
           "year" in column_name_lower or self._is_date_string(value):
            return "date"
        
        # Check if it's a numeric column
        if isinstance(value, (int, float)) or \
           (isinstance(value, str) and self._is_numeric(value)):
            return "numeric"
        
        # Default to categorical
        return "categorical"
    
    def _is_date_string(self, value):
        """Check if a string value looks like a date."""
        if not isinstance(value, str):
            return False
        
        # Simple check - would be more sophisticated in reality
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # ISO format: 2023-01-01
            r'\d{2}/\d{2}/\d{4}',  # US format: 01/01/2023
            r'\d{2}-\d{2}-\d{4}'   # Alternative format: 01-01-2023
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value):
                return True
        
        return False
    
    def _is_numeric(self, value):
        """Check if a string value is numeric."""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _generate_line_chart_config(self, results, data_structure):
        """Generate configuration for a line chart."""
        # Choose the first date column as X-axis
        x_axis = data_structure['date_columns'][0] if data_structure['date_columns'] else None
        
        # Choose the first numeric column as Y-axis
        y_axis = data_structure['numeric_columns'][0] if data_structure['numeric_columns'] else None
        
        return {
            'x_axis': x_axis,
            'y_axis': y_axis,
            'data': results
        }
    
    def _generate_bar_chart_config(self, results, data_structure):
        """Generate configuration for a bar chart."""
        # Choose the first categorical column as X-axis
        x_axis = data_structure['categorical_columns'][0] if data_structure['categorical_columns'] else None
        
        # Choose the first numeric column as Y-axis
        y_axis = data_structure['numeric_columns'][0] if data_structure['numeric_columns'] else None
        
        return {
            'x_axis': x_axis,
            'y_axis': y_axis,
            'data': results
        }
    
    def _generate_pie_chart_config(self, results, data_structure):
        """Generate configuration for a pie chart."""
        # Choose the first categorical column for labels
        label_column = data_structure['categorical_columns'][0] if data_structure['categorical_columns'] else None
        
        # Choose the first numeric column for values
        value_column = data_structure['numeric_columns'][0] if data_structure['numeric_columns'] else None
        
        return {
            'label_column': label_column,
            'value_column': value_column,
            'data': results
        }
    
    def _generate_table_config(self, results):
        """Generate configuration for a table view."""
        if not results or len(results) == 0:
            return {
                'columns': [],
                'data': []
            }
        
        # Extract column names from the first row
        columns = list(results[0].keys())
        
        return {
            'columns': columns,
            'data': results
        }


# Usage example
async def main():
    agent = DataAgent()
    
    # Initialize with data sources
    await agent.initialize([
        {'name': 'sales', 'type': 'mysql', 'connection': '...'},
        {'name': 'customers', 'type': 'postgres', 'connection': '...'}
    ])
    
    # Process a query
    result = await agent.process_query("Show me total sales by month for 2024")
    print("Query result:", result)
    
    # Process a follow-up query
    followup_result = await agent.process_query("How does that compare to last year?")
    print("Follow-up result:", followup_result)

# This would be the entry point in a real application
# if __name__ == "__main__":
#     asyncio.run(main())