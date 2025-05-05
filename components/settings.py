"""
Agentic BI Platform - Settings Component
--------------------------------------
Renders the settings page with user profile and management options.
"""

import streamlit as st
import pandas as pd
import uuid
from models.user import User, UserManager

def render_settings_page():
    '''
    Render the settings page with user profile and management options.
    '''
    st.markdown('<h1 class="section-header">⚙️ Settings & User Management</h1>', unsafe_allow_html=True)
    
    # Initialize or get user manager
    if 'user_manager' not in st.session_state:
        st.session_state.user_manager = UserManager()
        
        # Add default user if none exists
        if not st.session_state.user_manager.users:
            default_user = User(
                id="1",
                username="dufus",
                display_name="Dufus",
                email="dufus@example.com",
                role="admin"
            )
            st.session_state.user_manager.add_user(default_user)
    
    # Tabs for settings sections
    tab1, tab2 = st.tabs(["👤 My Profile", "👥 Manage Users"])
    
    with tab1:
        st.markdown("### Update Your Profile")
        
        # Get current user (for demo, use the first user)
        current_user = next(iter(st.session_state.user_manager.users.values()))
        
        display_name = st.text_input("Display Name", value=current_user.display_name)
        email = st.text_input("Email", value=current_user.email)
        
        role_options = ["Admin", "Analyst", "Viewer"]
        role_index = role_options.index(current_user.role.capitalize()) if current_user.role.capitalize() in role_options else 0
        role = st.selectbox("Role", role_options, index=role_index)
        
        if st.button("💾 Save Profile"):
            # Update user
            current_user.display_name = display_name
            current_user.email = email
            current_user.role = role.lower()
            st.success("Profile updated successfully!")
        
        # Theme settings
        st.markdown("### Theme Settings")
        
        theme_mode = st.radio(
            "Display Mode",
            ["Light", "Dark", "System Default"],
            index=2
        )
        
        accent_color = st.color_picker(
            "Accent Color",
            "#ef4444"
        )
        
        if st.button("Save Theme Settings"):
            # In a real app, this would save these settings to user preferences
            st.success("Theme settings saved successfully!")
    
    with tab2:
        st.markdown("### User Management")
        
        # Get all users
        users = list(st.session_state.user_manager.users.values())
        
        # Format for display
        users_data = [
            {
                "ID": user.id,
                "Name": user.display_name,
                "Email": user.email,
                "Role": user.role.capitalize(),
                "Last Active": user.last_login.split('T')[0] if user.last_login else "Never"
            }
            for user in users
        ]
        
        users_df = pd.DataFrame(users_data)
        st.dataframe(users_df)
        
    