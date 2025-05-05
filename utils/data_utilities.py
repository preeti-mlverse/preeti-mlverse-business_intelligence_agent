"""
Data Utilities for Agentic BI Platform
--------------------------------------
This module provides utility functions for data operations, including:
- Database connection management
- Data extraction and transformation
- Schema analysis
- Natural language to SQL conversion
"""

import sqlite3
import pandas as pd
import numpy as np
import os
import re
import json
from datetime import datetime

class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self):
        self.connections = {}
    
    def connect_sqlite(self, db_path, db_name=None):
        """
        Connect to a SQLite database.
        
        Args:
            db_path (str): Path to the SQLite database file
            db_name (str, optional): Name to identify this connection. Defaults to filename.
            
        Returns:
            dict: Connection info including connection object, or None if failed
        """
        if not os.path.exists(db_path):
            return None
        
        try:
            conn = sqlite3.connect(db_path)
            
            # Use filename as db_name if not provided
            if db_name is None:
                db_name = os.path.basename(db_path).split('.')[0]
                
            # Store connection info
            connection_info = {
                'name': db_name,
                'type': 'sqlite',
                'path': db_path,
                'conn': conn,
                'schema': self.extract_schema(conn)
            }
            
            self.connections[db_name] = connection_info
            return connection_info
            
        except Exception as e:
            print(f"Error connecting to SQLite database: {e}")
            return None
    
    def extract_schema(self, conn):
        """
        Extract database schema information.
        
        Args:
            conn: Database connection object
            
        Returns:
            dict: Schema information
        """
        schema = {}
        
        try:
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                
                # Get column information
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                
                column_info = []
                for col in columns:
                    column_info.append({
                        'name': col[1],
                        'type': col[2],
                        'notnull': col[3] == 1,
                        'default': col[4],
                        'pk': col[5] == 1
                    })
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                row_count = cursor.fetchone()[0]
                
                # Get sample data (first 5 rows)
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
                sample_rows = cursor.fetchall()
                
                # Convert sample rows to list of dicts
                sample_data = []
                if sample_rows:
                    # Get column names
                    col_names = [col[0] for col in cursor.description]
                    
                    for row in sample_rows:
                        sample_data.append(dict(zip(col_names, row)))
                
                # Store schema info for this table
                schema[table_name] = {
                    'columns': column_info,
                    'row_count': row_count,
                    'sample_data': sample_data
                }
            
            return schema
            
        except Exception as e:
            print(f"Error extracting schema: {e}")
            return {}
    
    def get_connection(self, db_name):
        """
        Get a database connection by name.
        
        Args:
            db_name (str): Name of the database connection
            
        Returns:
            dict: Connection info or None if not found
        """
        return self.connections.get(db_name)
    
    def close_all(self):
        """Close all database connections."""
        for conn_info in self.connections.values():
            if 'conn' in conn_info and conn_info['conn']:
                conn_info['conn'].close()
        
        self.connections = {}

class SchemaAnalyzer:
    """Analyzes database schemas for health and insights."""
    
    @staticmethod
    def analyze_table_health(conn, table_name, schema_info):
        """
        Analyze the health of a database table.
        
        Args:
            conn: Database connection
            table_name (str): Name of the table to analyze
            schema_info (dict): Schema information for the table
            
        Returns:
            dict: Health metrics for the table
        """
        health_metrics = {
            'row_count': schema_info['row_count'],
            'column_count': len(schema_info['columns']),
            'null_counts': {},
            'data_types': {},
            'data_quality': {},
            'last_updated': 'Unknown'
        }
        
        try:
            cursor = conn.cursor()
            
            # Check nulls for each column
            for col in schema_info['columns']:
                col_name = col['name']
                
                # Count nulls
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL;"
                cursor.execute(query)
                null_count = cursor.fetchone()[0]
                health_metrics['null_counts'][col_name] = null_count
                
                # Check data type distribution
                col_type = col['type'].lower()
                health_metrics['data_types'][col_name] = col_type
                
                # Add data quality checks based on type
                if 'int' in col_type or col_type == 'integer':
                    # For numeric columns, check min/max/avg
                    try:
                        query = f"SELECT MIN({col_name}), MAX({col_name}), AVG({col_name}) FROM {table_name} WHERE {col_name} IS NOT NULL;"
                        cursor.execute(query)
                        min_val, max_val, avg_val = cursor.fetchone()
                        
                        health_metrics['data_quality'][col_name] = {
                            'min': min_val,
                            'max': max_val,
                            'avg': avg_val
                        }
                    except:
                        pass
                
                elif 'char' in col_type or 'text' in col_type or 'varchar' in col_type:
                    # For text columns, check empty strings and length stats
                    try:
                        query = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} = '';"
                        cursor.execute(query)
                        empty_count = cursor.fetchone()[0]
                        
                        query = f"SELECT MIN(LENGTH({col_name})), MAX(LENGTH({col_name})), AVG(LENGTH({col_name})) FROM {table_name} WHERE {col_name} IS NOT NULL;"
                        cursor.execute(query)
                        min_len, max_len, avg_len = cursor.fetchone()
                        
                        health_metrics['data_quality'][col_name] = {
                            'empty_strings': empty_count,
                            'min_length': min_len,
                            'max_length': max_len,
                            'avg_length': avg_len
                        }
                    except:
                        pass
            
            # Try to determine last updated timestamp
            timestamp_columns = [col['name'] for col in schema_info['columns'] 
                              if any(term in col['name'].lower() for term in ['time', 'date', '_at', 'updated'])]
            
            if timestamp_columns:
                try:
                    query = f"SELECT MAX({timestamp_columns[0]}) FROM {table_name};"
                    cursor.execute(query)
                    last_updated = cursor.fetchone()[0]
                    
                    if last_updated:
                        health_metrics['last_updated'] = last_updated
                except:
                    pass
            
            return health_metrics
            
        except Exception as e:
            print(f"Error analyzing table health: {e}")
            return health_metrics

class QueryGenerator:
    """Generates SQL queries from natural language."""
    
    def __init__(self):
        # Load query templates
        self.query_templates = self._load_templates()
    
    def _load_templates(self):
        """Load query templates for common question types."""
        return {
            'trend': {
                'patterns': ['trend', 'over time', 'by month', 'by day', 'timeseries'],
                'template': "SELECT {time_column}, {agg_func}({value_column}) as value FROM {table} GROUP BY {time_column} ORDER BY {time_column};"
            },
            'distribution': {
                'patterns': ['distribution', 'breakdown', 'by category', 'percentage', 'proportion'],
                'template': "SELECT {category_column}, COUNT(*) as count FROM {table} GROUP BY {category_column} ORDER BY count DESC;"
            },
            'top': {
                'patterns': ['top', 'highest', 'most', 'best'],
                'template': "SELECT {entity_column}, {agg_func}({value_column}) as value FROM {table} GROUP BY {entity_column} ORDER BY value DESC LIMIT {limit};"
            },
            'comparison': {
                'patterns': ['compare', 'versus', 'against', 'difference'],
                'template': "SELECT {category_column}, {agg_func}({value_column}) as value FROM {table} GROUP BY {category_column} ORDER BY value DESC;"
            },
            'aggregate': {
                'patterns': ['total', 'sum', 'average', 'count', 'min', 'max'],
                'template': "SELECT {agg_func}({value_column}) as value FROM {table} WHERE {where_clause};"
            }
        }
    
    def generate_sql(self, query_text, schema):
        """
        Generate SQL from natural language query.
        
        Args:
            query_text (str): Natural language query
            schema (dict): Database schema information
            
        Returns:
            dict: SQL query information
        """
        # Convert query to lowercase for pattern matching
        query_lower = query_text.lower()
        
        # Identify query type
        query_type = None
        for qtype, info in self.query_templates.items():
            if any(pattern in query_lower for pattern in info['patterns']):
                query_type = qtype
                break
        
        # If no pattern matched, use generic approach
        if query_type is None:
            return self._generate_generic_query(query_text, schema)
        
        # For each query type, implement specific logic
        if query_type == 'trend':
            return self._generate_trend_query(query_text, schema)
        elif query_type == 'distribution':
            return self._generate_distribution_query(query_text, schema)
        elif query_type == 'top':
            return self._generate_top_query(query_text, schema)
        elif query_type == 'comparison':
            return self._generate_comparison_query(query_text, schema)
        elif query_type == 'aggregate':
            return self._generate_aggregate_query(query_text, schema)
        
        # Fallback to generic approach
        return self._generate_generic_query(query_text, schema)
    
    def _identify_relevant_table(self, query, schema):
        """Identify the most relevant table for the query."""
        # Simple approach: check for table name mentions
        for table_name in schema.keys():
            # Convert to lowercase and remove underscores for matching
            table_search = table_name.lower().replace('_', ' ')
            
            if table_search in query.lower():
                return table_name
        
        # If no direct mention, try to match based on column names
        # Count column mentions for each table
        table_scores = {table: 0 for table in schema.keys()}
        
        for table_name, table_info in schema.items():
            for col in table_info['columns']:
                col_search = col['name'].lower().replace('_', ' ')
                if col_search in query.lower():
                    table_scores[table_name] += 1
        
        # Return the table with the most column mentions
        if any(score > 0 for score in table_scores.values()):
            return max(table_scores.items(), key=lambda x: x[1])[0]
        
        # If still no match, return the table with the most rows (most significant)
        if schema:
            return max(schema.items(), key=lambda x: x[1]['row_count'])[0]
        
        return None
    
    def _identify_time_column(self, schema_info):
        """Identify a timestamp/date column in a table."""
        time_keywords = ['time', 'date', 'day', 'month', 'year', 'created', 'updated', 'at']
        
        for col in schema_info['columns']:
            col_name = col['name'].lower()
            if any(keyword in col_name for keyword in time_keywords):
                return col['name']
        
        return None
    
    def _identify_category_column(self, schema_info):
        """Identify a categorical column in a table."""
        # Look for textual columns that aren't timestamps
        time_keywords = ['time', 'date', 'day', 'month', 'year', 'created', 'updated', 'at']
        category_keywords = ['type', 'category', 'name', 'status', 'level', 'tier', 'group', 'department']
        
        # First try columns with category-like names
        for col in schema_info['columns']:
            col_name = col['name'].lower()
            if any(keyword in col_name for keyword in category_keywords):
                return col['name']
        
        # Then try any text column that's not a timestamp
        for col in schema_info['columns']:
            col_name = col['name'].lower()
            col_type = col['type'].lower()
            
            if ('char' in col_type or 'text' in col_type) and not any(keyword in col_name for keyword in time_keywords):
                return col['name']
        
        # Fallback to any non-primary key
        for col in schema_info['columns']:
            if not col['pk']:
                return col['name']
        
        # Last resort: first column
        return schema_info['columns'][0]['name'] if schema_info['columns'] else None
    
    def _identify_numeric_column(self, schema_info):
        """Identify a numeric column in a table."""
        # Look for numeric columns with value-like names
        value_keywords = ['count', 'amount', 'value', 'price', 'cost', 'quantity', 'num', 'total', 'sum']
        
        # First try columns with value-like names
        for col in schema_info['columns']:
            col_name = col['name'].lower()
            if any(keyword in col_name for keyword in value_keywords):
                return col['name']
        
        # Then try any numeric column that's not a primary key
        for col in schema_info['columns']:
            col_type = col['type'].lower()
            
            if ('int' in col_type or 'float' in col_type or 'double' in col_type or 'real' in col_type or 'numeric' in col_type) and not col['pk']:
                return col['name']
        
        # Fallback to COUNT(*)
        return '*'
    
    def _identify_entity_column(self, schema_info, query):
        """Identify an entity column based on query."""
        entity_keywords = ['user', 'customer', 'product', 'employee', 'account', 'item', 'client']
        
        # First check if any entity keywords are in the query
        query_entities = [kw for kw in entity_keywords if kw in query.lower()]
        
        if query_entities:
            # Look for columns matching the entities found in query
            for entity in query_entities:
                for col in schema_info['columns']:
                    col_name = col['name'].lower()
                    if entity in col_name:
                        return col['name']
        
        # Fallback to any ID column that references these entities
        for entity in entity_keywords:
            for col in schema_info['columns']:
                col_name = col['name'].lower()
                if f"{entity}_id" in col_name:
                    return col['name']
        
        # If no specific entity found, return a name or description column
        for col in schema_info['columns']:
            col_name = col['name'].lower()
            if 'name' in col_name or 'title' in col_name or 'description' in col_name:
                return col['name']
        
        # Fallback to the primary key
        for col in schema_info['columns']:
            if col['pk']:
                return col['name']
        
        # Last resort: first column
        return schema_info['columns'][0]['name'] if schema_info['columns'] else None
    
    def _identify_aggregation_function(self, query):
        """Identify the aggregation function to use based on query."""
        if 'average' in query.lower() or 'avg' in query.lower():
            return 'AVG'
        elif 'sum' in query.lower() or 'total' in query.lower():
            return 'SUM'
        elif 'minimum' in query.lower() or 'min' in query.lower():
            return 'MIN'
        elif 'maximum' in query.lower() or 'max' in query.lower():
            return 'MAX'
        elif 'count' in query.lower() or 'how many' in query.lower() or 'number of' in query.lower():
            return 'COUNT'
        
        # Default to COUNT
        return 'COUNT'
    
    def _extract_limit(self, query):
        """Extract a LIMIT value from the query."""
        # Look for patterns like "top 10" or "5 highest"
        number_pattern = r'(?:top|first|highest|best)\s+(\d+)|(\d+)\s+(?:top|first|highest|best)'
        match = re.search(number_pattern, query.lower())
        
        if match:
            # Return the first non-None capturing group
            for group in match.groups():
                if group:
                    return int(group)
        
        # Default to 10 for top queries
        if any(kw in query.lower() for kw in ['top', 'highest', 'best']):
            return 10
        
        return None
    
    def _generate_trend_query(self, query, schema):
        """Generate a trend analysis query."""
        # Identify relevant table
        table_name = self._identify_relevant_table(query, schema)
        
        if not table_name:
            return {
                'success': False,
                'message': 'Could not identify a relevant table for this query.'
            }
        
        table_info = schema[table_name]
        
        # Identify time column
        time_column = self._identify_time_column(table_info)
        
        if not time_column:
            return {
                'success': False,
                'message': 'Could not identify a time-based column for trend analysis.'
            }
        
        # Identify value column
        value_column = self._identify_numeric_column(table_info)
        agg_func = self._identify_aggregation_function(query)
        
        # Format the time column for grouping (if it's a timestamp/date)
        time_format = time_column
        if any(kw in time_column.lower() for kw in ['time', 'date', '_at']):
            time_format = f"strftime('%Y-%m', {time_column})"
        
        # Build the query
        sql_query = f"""
        SELECT {time_format} as time_period, {agg_func}({value_column}) as value 
        FROM {table_name} 
        WHERE {time_column} IS NOT NULL
        GROUP BY time_period 
        ORDER BY time_period;
        """
        
        return {
            'success': True,
            'query': sql_query,
            'visualization_type': 'line',
            'explanation': f"Analyzing {agg_func.lower()} of {value_column} over time from {table_name}",
            'table': table_name,
            'x_axis': 'time_period',
            'y_axis': 'value'
        }
    
    def _generate_distribution_query(self, query, schema):
        """Generate a distribution analysis query."""
        # Identify relevant table
        table_name = self._identify_relevant_table(query, schema)
        
        if not table_name:
            return {
                'success': False,
                'message': 'Could not identify a relevant table for this query.'
            }
        
        table_info = schema[table_name]
        
        # Identify category column
        category_column = self._identify_category_column(table_info)
        
        if not category_column:
            return {
                'success': False,
                'message': 'Could not identify a categorical column for distribution analysis.'
            }
        
        # Build the query
        sql_query = f"""
        SELECT {category_column}, COUNT(*) as count 
        FROM {table_name} 
        WHERE {category_column} IS NOT NULL
        GROUP BY {category_column} 
        ORDER BY count DESC;
        """
        
        return {
            'success': True,
            'query': sql_query,
            'visualization_type': 'pie' if 'percentage' in query.lower() else 'bar',
            'explanation': f"Analyzing distribution of {category_column} in {table_name}",
            'table': table_name,
            'x_axis': category_column,
            'y_axis': 'count'
        }
    
    def _generate_top_query(self, query, schema):
        """Generate a top N query."""
        # Identify relevant table
        table_name = self._identify_relevant_table(query, schema)
        
        if not table_name:
            return {
                'success': False,
                'message': 'Could not identify a relevant table for this query.'
            }
        
        table_info = schema[table_name]
        
        # Identify entity column
        entity_column = self._identify_entity_column(table_info, query)
        
        if not entity_column:
            return {
                'success': False,
                'message': 'Could not identify an entity column for this query.'
            }
        
        # Identify value column and aggregation
        value_column = self._identify_numeric_column(table_info)
        agg_func = self._identify_aggregation_function(query)
        
        # Extract limit
        limit = self._extract_limit(query)
        
        # Build the query
        sql_query = f"""
        SELECT {entity_column}, {agg_func}({value_column}) as value 
        FROM {table_name} 
        GROUP BY {entity_column} 
        ORDER BY value DESC
        LIMIT {limit or 10};
        """
        
        return {
            'success': True,
            'query': sql_query,
            'visualization_type': 'bar',
            'explanation': f"Finding top {limit or 10} {entity_column} by {agg_func.lower()} of {value_column} in {table_name}",
            'table': table_name,
            'x_axis': entity_column,
            'y_axis': 'value'
        }
    
    def _generate_comparison_query(self, query, schema):
        """Generate a comparison query."""
        # Identify relevant table
        table_name = self._identify_relevant_table(query, schema)
        
        if not table_name:
            return {
                'success': False,
                'message': 'Could not identify a relevant table for this query.'
            }
        
        table_info = schema[table_name]
        
        # Identify category column
        category_column = self._identify_category_column(table_info)
        
        if not category_column:
            return {
                'success': False,
                'message': 'Could not identify a categorical column for comparison.'
            }
        
        # Identify value column and aggregation
        value_column = self._identify_numeric_column(table_info)
        agg_func = self._identify_aggregation_function(query)
        
        # Build the query
        sql_query = f"""
        SELECT {category_column}, {agg_func}({value_column}) as value 
        FROM {table_name} 
        GROUP BY {category_column} 
        ORDER BY value DESC;
        """
        
        return {
            'success': True,
            'query': sql_query,
            'visualization_type': 'bar',
            'explanation': f"Comparing {category_column} by {agg_func.lower()} of {value_column} in {table_name}",
            'table': table_name,
            'x_axis': category_column,
            'y_axis': 'value'
        }
    
    def _generate_aggregate_query(self, query, schema):
        """Generate an aggregate query."""
        # Identify relevant table
        table_name = self._identify_relevant_table(query, schema)
        
        if not table_name:
            return {
                'success': False,
                'message': 'Could not identify a relevant table for this query.'
            }