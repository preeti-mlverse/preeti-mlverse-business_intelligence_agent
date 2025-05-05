import streamlit as st

def render_navigation():
    '''
    Render the navigation bar at the top of the page.
    '''
    pages = ['Home', 'Data Source', 'Data Health', 'Analyzer', 'Reports Dashboard', 'Collaboration', 'Settings', 'Logout']
    
    cols = st.columns(len(pages))
    for i, page in enumerate(pages):
        with cols[i]:
            if st.session_state.current_page == page:
                st.markdown(f'<div class="tab-button active">● {page}</div>', unsafe_allow_html=True)
            else:
                if st.button(f'○ {page}', key=f'nav_{page}'):
                    st.session_state.current_page = page
                    st.rerun()