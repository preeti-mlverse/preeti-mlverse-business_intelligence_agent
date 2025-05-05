import pandas as pd
import numpy as np

def table_health_analysis(conn, table_name, schema_info):
    '''
    Analyze the health of a database table.
    
    Args:
        conn: Database connection
        table_name (str): Name of the table to analyze
        schema_info (dict): Schema information for the table
        
    Returns:
        dict: Health metrics for the table
    '''
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