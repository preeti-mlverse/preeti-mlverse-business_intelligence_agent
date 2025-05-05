import streamlit as st

def render_home_page():
    '''
    Render the home page with welcome message and quick access buttons.
    '''
    st.markdown('<h1 class="section-header">🏠 Home Dashboard</h1>', unsafe_allow_html=True)
    
    st.markdown('<div class="card">Welcome to Agentic BI. Select a tab to proceed.</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3>👉 Steps to use Agentic BI:</h3>', unsafe_allow_html=True)
        
        st.markdown('''
        1. Go to **Data Source** to upload your SQLite database.
        2. Head to the **Analyzer** tab to define your goal and output types.
        3. Ask questions, generate dashboards or insights.
        4. Save important results to the **Reports Dashboard**.
        5. Collaborate on findings from the **Collaboration** tab.
        ''')
        
        if not st.session_state.connected_dbs:
            st.warning("Upload a SQLite DB file from the Data Source tab to continue.")
    
    with col2:
        st.markdown('<h3>Quick Access</h3>', unsafe_allow_html=True)
        
        if st.button("🔍 Start Analyzing Data", key="quick_analyze"):
            st.session_state.current_page = 'Analyzer'
            st.experimental_rerun()
        
        if st.button("📊 View Reports", key="quick_reports"):
            st.session_state.current_page = 'Reports Dashboard'
            st.experimental_rerun()