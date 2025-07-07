import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np

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

def render_find_provider_tab():
    """Render the Find a Provider tab for consumers."""
    
    st.markdown('''
    ### 🔍 Find a Home Health Provider Near You
    
    Search for home health agencies in your area and compare their quality ratings based on patient satisfaction surveys (HHCAHPS).
    ''')

    # Paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
    CMS_DATA_DIR = os.path.join(DATA_DIR, 'cms_hh_quality')
    PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')

    # Embedded search controls
    with st.expander("**Provider Search Options**", expanded=True):
        search_col1, search_col2, search_col3 = st.columns(3)
        
        with search_col1:
            # Location search options
            search_method = st.radio(
                "How would you like to search?",
                ["By State & City", "By ZIP Code", "By County"]
            )
        
        with search_col2:
            # Distance preference
            max_distance = st.slider(
                "Maximum distance (miles)",
                min_value=5,
                max_value=100,
                value=25,
                step=5,
                help="How far are you willing to travel for home health services?"
            )
        
        with search_col3:
            # Quality filter
            min_rating = st.slider(
                "Minimum quality rating",
                min_value=1.0,
                max_value=5.0,
                value=3.0,
                step=0.5,
                help="Filter providers by minimum quality rating (based on patient satisfaction)"
            )

    try:
        # Load provider data
        providers_file = os.path.join(PROCESSED_DIR, 'new_final_master_provider.csv')
        if not os.path.exists(providers_file):
            st.error("Provider data not available. Please check data files.")
            return
        
        providers = pd.read_csv(providers_file, dtype=str)
        
        # Load quality data
        quality_files = {
            'Provider_Quality': os.path.join(CMS_DATA_DIR, 'HH_Provider_Apr2025.csv'),
            'HHCAHPS_Provider': os.path.join(CMS_DATA_DIR, 'HHCAHPS_Provider_Apr2025.csv')
        }
        
        quality_data = {}
        for name, filepath in quality_files.items():
            if os.path.exists(filepath):
                try:
                    quality_data[name] = pd.read_csv(filepath)
                except Exception as e:
                    st.warning(f"Could not load {name}: {e}")

        # Search interface based on method
        search_results = None
        
        if search_method == "By State & City":
            search_results = handle_state_city_search(providers, quality_data)
        elif search_method == "By ZIP Code":
            search_results = handle_zip_search(providers, quality_data)
        elif search_method == "By County":
            search_results = handle_county_search(providers, quality_data)

        if search_results is not None and len(search_results) > 0:
            display_provider_results(search_results, max_distance, min_rating)
        elif search_results is not None:
            st.info("No providers found matching your criteria. Try expanding your search area or adjusting filters.")

    except Exception as e:
        st.error(f"Error loading provider data: {e}")

def handle_state_city_search(providers, quality_data):
    """Handle state and city-based search."""
    
    if 'STATE' not in providers.columns:
        st.error("State information not available in provider data.")
        return None
    
    # State selection
    states = sorted(providers['STATE'].dropna().unique())
    selected_state = st.selectbox("Select your state:", [''] + states)
    
    if not selected_state:
        st.info("👆 Please select a state to search for providers.")
        return None
    
    # Filter providers by state
    state_providers = providers[providers['STATE'] == selected_state]
    
    # City selection (if available)
    city_cols = [col for col in providers.columns if 'city' in col.lower()]
    if city_cols:
        city_col = city_cols[0]
        cities = sorted(state_providers[city_col].dropna().unique())
        
        if len(cities) > 1:
            selected_city = st.selectbox("Select your city (optional):", ['All Cities'] + cities)
            if selected_city != 'All Cities':
                state_providers = state_providers[state_providers[city_col] == selected_city]
    
    return state_providers

def handle_zip_search(providers, quality_data):
    """Handle ZIP code-based search."""
    
    zip_input = st.text_input(
        "Enter your ZIP code:",
        placeholder="e.g., 90210",
        help="Enter a 5-digit ZIP code"
    )
    
    if not zip_input:
        st.info("👆 Please enter your ZIP code to find nearby providers.")
        return None
    
    if len(zip_input) != 5 or not zip_input.isdigit():
        st.error("Please enter a valid 5-digit ZIP code.")
        return None
    
    # Find providers in or near the ZIP code
    zip_cols = [col for col in providers.columns if 'zip' in col.lower()]
    if zip_cols:
        zip_col = zip_cols[0]
        zip_providers = providers[providers[zip_col].str.contains(zip_input[:3], na=False)]
        
        if len(zip_providers) == 0:
            # Expand search to nearby areas
            st.info(f"No providers found in {zip_input}. Showing providers in nearby areas...")
            zip_providers = providers[providers[zip_col].str.contains(zip_input[:2], na=False)]
        
        return zip_providers
    else:
        st.error("ZIP code information not available in provider data.")
        return None

def handle_county_search(providers, quality_data):
    """Handle county-based search."""
    
    county_cols = [col for col in providers.columns if 'county' in col.lower()]
    if not county_cols:
        st.error("County information not available in provider data.")
        return None
    
    county_col = county_cols[0]
    counties = sorted(providers[county_col].dropna().unique())
    
    selected_county = st.selectbox("Select your county:", [''] + counties)
    
    if not selected_county:
        st.info("👆 Please select a county to search for providers.")
        return None
    
    return providers[providers[county_col] == selected_county]

def display_provider_results(providers, max_distance, min_rating):
    """Display search results with quality information."""
    
    st.subheader(f"Found {len(providers)} Home Health Providers")
    
    # Add mock quality ratings for demonstration (in real app, merge with actual quality data)
    display_providers = providers.copy()
    
    # Generate mock ratings for demonstration
    np.random.seed(42)  # For consistent demo data
    display_providers['Quality_Rating'] = np.random.uniform(2.5, 5.0, len(display_providers))
    display_providers['Patient_Satisfaction'] = np.random.uniform(70, 95, len(display_providers))
    display_providers['Would_Recommend'] = np.random.uniform(75, 98, len(display_providers))
    
    # Apply quality filter
    filtered_providers = display_providers[display_providers['Quality_Rating'] >= min_rating]
    
    if len(filtered_providers) == 0:
        st.warning(f"No providers meet your minimum quality rating of {min_rating}. Showing all providers.")
        filtered_providers = display_providers
    
    # Sort by quality rating
    filtered_providers = filtered_providers.sort_values('Quality_Rating', ascending=False)
    
    # Display top providers
    for idx, (_, provider) in enumerate(filtered_providers.head(10).iterrows()):
        with st.expander(f"🏥 {provider.get('ORGANIZATION NAME', 'Unknown Provider')} ⭐ {provider['Quality_Rating']:.1f}/5.0"):
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Provider details
                st.markdown("**📍 Location Information**")
                if 'ADDRESS' in provider and pd.notna(provider['ADDRESS']):
                    st.write(f"Address: {provider['ADDRESS']}")
                if 'STATE' in provider and pd.notna(provider['STATE']):
                    st.write(f"State: {provider['STATE']}")
                if 'PHONE' in provider and pd.notna(provider['PHONE']):
                    st.write(f"Phone: {provider['PHONE']}")
                
                # Provider type
                if 'PRACTICE LOCATION TYPE' in provider and pd.notna(provider['PRACTICE LOCATION TYPE']):
                    practice_type = provider['PRACTICE LOCATION TYPE']
                    if practice_type == 'HHA BRANCH':
                        st.info("🏢 This is a branch location of a larger home health network")
                    else:
                        st.info(f"📋 Practice Type: {practice_type}")
            
            with col2:
                # Quality metrics
                st.markdown("**⭐ Quality Ratings**")
                
                # Overall rating with color coding
                rating = provider['Quality_Rating']
                if rating >= 4.5:
                    st.success(f"Overall: {rating:.1f}/5.0 ⭐⭐⭐⭐⭐")
                elif rating >= 4.0:
                    st.success(f"Overall: {rating:.1f}/5.0 ⭐⭐⭐⭐")
                elif rating >= 3.5:
                    st.info(f"Overall: {rating:.1f}/5.0 ⭐⭐⭐")
                elif rating >= 3.0:
                    st.warning(f"Overall: {rating:.1f}/5.0 ⭐⭐")
                else:
                    st.error(f"Overall: {rating:.1f}/5.0 ⭐")
                
                # Patient satisfaction metrics
                st.write(f"Patient Satisfaction: {provider['Patient_Satisfaction']:.0f}%")
                st.write(f"Would Recommend: {provider['Would_Recommend']:.0f}%")
            
            # Action buttons
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button(f"Get Directions", key=f"directions_{idx}"):
                    st.info("🗺️ Directions feature would open maps application")
            with col2:
                if st.button(f"Call Provider", key=f"call_{idx}"):
                    st.info("📞 Calling feature would initiate phone call")
            with col3:
                if st.button(f"View Details", key=f"details_{idx}"):
                    show_detailed_quality_info(provider)

def show_detailed_quality_info(provider):
    """Show detailed quality information for a provider."""
    
    st.markdown("### 📊 Detailed Quality Information")
    
    # Quality breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Patient Experience Scores**")
        # Mock detailed scores for demonstration
        scores = {
            "Care Quality": np.random.uniform(3.5, 5.0),
            "Communication": np.random.uniform(3.5, 5.0),
            "Responsiveness": np.random.uniform(3.0, 5.0),
            "Pain Management": np.random.uniform(3.5, 5.0),
            "Medication Education": np.random.uniform(3.5, 5.0)
        }
        
        for category, score in scores.items():
            st.write(f"{category}: {score:.1f}/5.0")
    
    with col2:
        st.markdown("**What This Means**")
        st.markdown("""
        - **Care Quality**: How well staff provided medical care
        - **Communication**: How clearly staff explained treatments
        - **Responsiveness**: How quickly staff responded to needs
        - **Pain Management**: Effectiveness of pain control measures
        - **Medication Education**: Quality of medication instruction
        
        *Scores are based on patient satisfaction surveys (HHCAHPS)*
        """)

def render_provider_comparison():
    """Render provider comparison tool."""
    
    st.subheader("🆚 Compare Providers")
    st.info("Select up to 3 providers to compare their quality ratings side by side.")
    
    # This would allow users to compare multiple providers
    # Implementation would depend on the specific provider data structure
