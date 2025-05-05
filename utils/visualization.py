import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def create_visualization(data, visualization_type, config=None):
    '''
    Create a visualization based on data and type.
    
    Args:
        data (DataFrame): Data to visualize
        visualization_type (str): Type of visualization (line, bar, pie, table, etc.)
        config (dict, optional): Additional configuration parameters
        
    Returns:
        object: Plotly figure object
    '''
    if config is None:
        config = {}
    
    fig = None
    
    # Extract axis information from config
    x_axis = config.get('x_axis', data.columns[0] if not data.empty else None)
    y_axis = config.get('y_axis', data.columns[1] if len(data.columns) > 1 and not data.empty else None)
    title = config.get('title', '')
    
    if visualization_type == 'line':
        fig = px.line(
            data, 
            x=x_axis, 
            y=y_axis, 
            title=title,
            markers=config.get('markers', True)
        )
        
    elif visualization_type == 'bar':
        fig = px.bar(
            data, 
            x=x_axis, 
            y=y_axis, 
            title=title,
            color=config.get('color')
        )
        
    elif visualization_type == 'pie':
        fig = px.pie(
            data, 
            names=x_axis, 
            values=y_axis, 
            title=title
        )
        
    elif visualization_type == 'scatter':
        fig = px.scatter(
            data, 
            x=x_axis, 
            y=y_axis, 
            title=title,
            color=config.get('color'),
            size=config.get('size')
        )
        
    elif visualization_type == 'heatmap':
        # For correlation matrices or other heatmap data
        fig = px.imshow(
            data,
            title=title
        )
        
    elif visualization_type == 'area':
        fig = px.area(
            data, 
            x=x_axis, 
            y=y_axis, 
            title=title
        )
        
    elif visualization_type == 'box':
        fig = px.box(
            data, 
            x=x_axis, 
            y=y_axis, 
            title=title
        )
        
    elif visualization_type == 'histogram':
        fig = px.histogram(
            data, 
            x=x_axis, 
            title=title
        )
    
    # Add common layout adjustments
    if fig:
        fig.update_layout(
            template='plotly_white',
            margin=dict(l=40, r=40, t=60, b=40)
        )
    
    return fig