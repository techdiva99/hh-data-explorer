#!/usr/bin/env python3
"""
Main entry point for Streamlit app deployment.
This file is required for Streamli#### Create tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Dashboard", 
    "🔍 Find a Provider", 
    "Coverage Deserts", 
    "🏢 Provider Networks", 
    "⭐ Quality Metrics", 
    "Market Analysis", 
    "About"
])reate tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Dashboard", "🔍 Find a Provider", "Coverage Deserts", "🏢 Provider Networks", "⭐ Quality Metrics", "Market Analysis", "About"])reate tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Dashboard", "🔍 Find a Provider", "Coverage Deserts", "🏢 Provider Networks", "⭐ Quality Metrics", "Market Analysis", "About"])reate tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Dashboard", "Find a Provider", "Coverage Deserts", "Provider Networks", "Quality Metrics", "Market Analysis", "About"])Community Cloud deployment.
"""

import streamlit as st
import os

# Set Streamlit page config for wide layout
st.set_page_config(
    page_title='Home Health Finder & Market Explorer',
    layout='wide',
    page_icon=None
)

# --- BRAND COLORS ---
BRAND_COLORS = {
    'primary_blue': '#00B4D8',
    'primary_green': '#7CB342',
    'secondary_blue': '#0077BE',
    'secondary_green': '#4CAF50',
    'accent_blue': '#E1F5FE',
    'accent_green': '#E8F5E8',
    'dark_blue': '#003F5C',
    'dark_green': '#2E7D32',
    'white': '#FFFFFF',
    'light_gray': '#F5F5F5',
    'warning': '#FF9800',
    'error': '#F44336',
    'success': '#4CAF50'
}

# Custom CSS for styling
st.markdown(f"""
<style>
    /* Header styling */
    .main-header {{
        background: linear-gradient(90deg, {BRAND_COLORS['primary_blue']} 0%, {BRAND_COLORS['secondary_blue']} 100%);
        padding: 1rem 2rem;
        margin: -1rem -1rem 2rem -1rem;
        border-radius: 0 0 10px 10px;
    }}
    
    .main-header h1 {{
        color: {BRAND_COLORS['white']};
        margin: 0;
        font-size: 2.5rem;
        font-weight: 600;
    }}
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 20px;
        background-color: {BRAND_COLORS['light_gray']};
        padding: 10px;
        border-radius: 10px;
        margin-bottom: 20px;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        height: 50px;
        padding: 0px 24px;
        background-color: {BRAND_COLORS['white']};
        border-radius: 8px;
        color: {BRAND_COLORS['dark_blue']};
        font-weight: 500;
        border: 2px solid transparent;
    }}
    
    .stTabs [aria-selected="true"] {{
        background-color: {BRAND_COLORS['primary_blue']};
        color: {BRAND_COLORS['white']};
        border: 2px solid {BRAND_COLORS['secondary_blue']};
    }}
    
    /* Footer styling */
    .footer {{
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: {BRAND_COLORS['dark_blue']};
        color: {BRAND_COLORS['white']};
        text-align: center;
        padding: 10px 0;
        z-index: 999;
        font-size: 14px;
    }}
    
    /* Adjust main content to account for footer */
    .main .block-container {{
        padding-bottom: 60px;
    }}
</style>
""", unsafe_allow_html=True)

# Header with logo
LOGO_PATH = os.path.join(os.path.dirname(__file__), 'assets', 'img', 'logo.png')
if os.path.exists(LOGO_PATH):
    col1, col2 = st.columns([1, 8])
    with col1:
        st.image(LOGO_PATH, width=80)
    with col2:
        st.markdown('<div class="main-header"><h1>Home Health Finder & Market Explorer</h1></div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="main-header"><h1>Home Health Finder & Market Explorer</h1></div>', unsafe_allow_html=True)

# Create tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["� Dashboard", "🔍 Find a Provider", "�🗺️ Coverage Deserts", "🏢 Provider Networks", "⭐ Quality Metrics", "� Market Analysis", "ℹ️ About"])

# Import tab modules
from src.ui.dashboard_tab import render_dashboard_tab
from src.ui.find_provider_tab import render_find_provider_tab
from src.ui.coverage_deserts_tab import render_coverage_deserts_tab
from src.ui.provider_networks_tab import render_provider_networks_tab
from src.ui.quality_metrics_tab import render_quality_metrics_tab
from src.ui.market_analysis_tab import render_market_analysis_tab
from src.ui.about_tab import render_about_tab

# Render content based on selected tab
with tab1:
    render_dashboard_tab()

with tab2:
    render_find_provider_tab()

with tab3:
    render_coverage_deserts_tab()

with tab4:
    render_provider_networks_tab()

with tab5:
    render_quality_metrics_tab()

with tab6:
    render_market_analysis_tab()

with tab7:
    render_about_tab()

# Footer
st.markdown("""
<div class="footer">
    Contact: info@techdiva.io
</div>
""", unsafe_allow_html=True)
