import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag

# Download NLTK resources if not already downloaded
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
    
try:
    nltk.data.find('taggers/averaged_perceptron_tagger')
except LookupError:
    nltk.download('averaged_perceptron_tagger')

def nlp_to_sql(query, schema):
    '''
    Convert natural language query to SQL using simple pattern matching.
    
    Args:
        query (str): Natural language query
        schema (dict): Database schema information
        
    Returns:
        dict: SQL query information
    '''
    query_lower = query.lower()
    
    # Identify keyword patterns
    is_trend_query = any(kw in query_lower for kw in ['trend', 'over time', 'by month', 'by day', 'timeseries'])
    is_distribution_query = any(kw in query_lower for kw in ['distribution', 'breakdown', 'by category', 'percentage', 'proportion'])
    is_top_query = any(kw in query_lower for kw in ['top', 'highest', 'most', 'best'])
    is_comparison_query = any(kw in query_lower for kw in ['compare', 'versus', 'against', 'difference'])
    is_aggregate_query = any(kw in query_lower for kw in ['total', 'sum', 'average', 'count', 'min', 'max'])
    
    # Determine query type
    if is_trend_query:
        return generate_trend_query(query, schema)
    elif is_distribution_query:
        return generate_distribution_query(query, schema)
    elif is_top_query:
        return generate_top_query(query, schema)
    elif is_comparison_query:
        return generate_comparison_query(query, schema)
    elif is_aggregate_query:
        return generate_aggregate_query(query, schema)
    else:
        return generate_generic_query(query, schema)
    
def identify_relevant_table(query, schema):
    '''Identify the most relevant table for the query.'''
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

def identify_time_column(schema_info):
    '''Identify a timestamp/date column in a table.'''
    time_keywords = ['time', 'date', 'day', 'month', 'year', 'created', 'updated', 'at']
    
    for col in schema_info['columns']:
        col_name = col['name'].lower()
        if any(keyword in col_name for keyword in time_keywords):
            return col['name']
    
    return None

def identify_category_column(schema_info):
    '''Identify a categorical column in a table.'''
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

def identify_numeric_column(schema_info):
    '''Identify a numeric column in a table.'''
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

def identify_aggregation_function(query):
    '''Identify the aggregation function to use based on query.'''
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

def extract_limit(query):
    '''Extract a LIMIT value from the query.'''
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

def generate_trend_query(query, schema):
    '''Generate a trend analysis query.'''
    # Identify relevant table
    table_name = identify_relevant_table(query, schema)
    
    if not table_name:
        return {
            'success': False,
            'message': 'Could not identify a relevant table for this query.'
        }
    
    table_info = schema[table_name]
    
    # Identify time column
    time_column = identify_time_column(table_info)
    
    if not time_column:
        return {
            'success': False,
            'message': 'Could not identify a time-based column for trend analysis.'
        }
    
    # Identify value column
    value_column = identify_numeric_column(table_info)
    agg_func = identify_aggregation_function(query)
    
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

def generate_distribution_query(query, schema):
    '''Generate a distribution analysis query.'''
    # Identify relevant table
    table_name = identify_relevant_table(query, schema)
    
    if not table_name:
        return {
            'success': False,
            'message': 'Could not identify a relevant table for this query.'
        }
    
    table_info = schema[table_name]
    
    # Identify category column
    category_column = identify_category_column(table_info)
    
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

def generate_top_query(query, schema):
    '''Generate a top N query.'''
    # Implementation details omitted for brevity
    # Similar structure to the functions above
    pass

def generate_comparison_query(query, schema):
    '''Generate a comparison query.'''
    # Implementation details omitted for brevity
    pass

def generate_aggregate_query(query, schema):
    '''Generate an aggregate query.'''
    # Implementation details omitted for brevity
    pass

def generate_generic_query(query, schema):
    '''Generate a generic query when no specific pattern is matched.'''
    # Implementation details omitted for brevity
    pass