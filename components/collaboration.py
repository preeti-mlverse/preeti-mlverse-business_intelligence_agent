"""
Agentic BI Platform - Collaboration Component
-------------------------------------------
Renders the collaboration page with tools integration options.
"""

import streamlit as st

def render_collaboration_page():
    '''
    Render the collaboration page with tools integration options.
    '''
    st.markdown('<h1 class="section-header">🔗 App & Tool Integrations</h1>', unsafe_allow_html=True)
    
    st.markdown("### 🔌 Connect New Tool")
    
    # Tool selection
    tools = ["Slack", "Microsoft Teams", "Email", "Jira", "Trello", "GitHub", "Notion", "Google Drive"]
    selected_tool = st.selectbox("Choose an app or tool", tools)
    
    # Display connection form based on selected tool
    if selected_tool == "Slack":
        st.text_input("Webhook URL")
        
        if st.button("🔒 Save Connection"):
            st.success("Slack integration saved successfully!")
    
    elif selected_tool == "Email":
        col1, col2 = st.columns(2)
        with col1:
            smtp_server = st.text_input("SMTP Server")
            smtp_port = st.text_input("SMTP Port")
        with col2:
            email_address = st.text_input("Email Address")
            password = st.text_input("Password", type="password")
        
        if st.button("🔒 Save Connection"):
            st.success("Email integration saved successfully!")
    
    elif selected_tool == "GitHub":
        github_token = st.text_input("GitHub Personal Access Token")
        repository = st.text_input("Repository URL (optional)")
        
        if st.button("🔒 Save Connection"):
            st.success("GitHub integration saved successfully!")
    
    elif selected_tool == "Microsoft Teams":
        teams_webhook = st.text_input("Teams Webhook URL")
        
        if st.button("🔒 Save Connection"):
            st.success("Microsoft Teams integration saved successfully!")
    
    elif selected_tool == "Jira":
        col1, col2 = st.columns(2)
        with col1:
            jira_url = st.text_input("Jira URL")
            project_key = st.text_input("Project Key")
        with col2:
            username = st.text_input("Username")
            api_token = st.text_input("API Token", type="password")
        
        if st.button("🔒 Save Connection"):
            st.success("Jira integration saved successfully!")
    
    elif selected_tool == "Trello":
        col1, col2 = st.columns(2)
        with col1:
            api_key = st.text_input("API Key")
        with col2:
            token = st.text_input("Token", type="password")
        
        board_name = st.text_input("Board Name (optional)")
        
        if st.button("🔒 Save Connection"):
            st.success("Trello integration saved successfully!")
    
    elif selected_tool == "Notion":
        notion_token = st.text_input("Notion Integration Token")
        database_id = st.text_input("Database ID (optional)")
        
        if st.button("🔒 Save Connection"):
            st.success("Notion integration saved successfully!")
    
    elif selected_tool == "Google Drive":
        st.info("Google Drive integration requires OAuth authentication.")
        
        if st.button("🔒 Connect to Google Drive"):
            st.success("Redirecting to Google authentication...")
    
    # Existing integrations
    st.markdown("---")
    st.markdown("### Existing Integrations")
    
    # Dummy data for demonstration
    st.info("No integrations configured yet.")
    
    # Integration capabilities
    st.markdown("---")
    st.markdown("### Available Integration Actions")
    
    st.markdown("""
    #### Share Reports
    Automatically share reports and insights with your team through connected tools.
    
    #### Schedule Updates
    Schedule regular updates and alerts based on data changes.
    
    #### Collaborative Analysis
    Allow team members to comment and collaborate on reports and insights.
    
    #### Workflow Automation
    Trigger actions in other tools based on insights from your data.
    """)