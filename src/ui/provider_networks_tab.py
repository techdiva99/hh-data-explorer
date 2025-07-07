import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
from datetime import datetime

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

def render_provider_networks_tab():
    """Render the Provider Networks tab content."""
    
    # Paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data/processed')
    
    st.markdown('''
    ### Provider Networks Analysis
    
    Explore home health agency networks, organizational structures, and geographic distribution patterns.
    ''')

    try:
        # Load provider data
        providers_file = os.path.join(DATA_DIR, 'new_final_master_provider.csv')
        if os.path.exists(providers_file):
            providers = pd.read_csv(providers_file, dtype=str)
        else:
            st.error("Provider data not found.")
            return

        # Load network data if available
        networks_file = os.path.join(DATA_DIR, 'provider_networks.json')
        networks_flat_file = os.path.join(DATA_DIR, 'provider_networks_flat.csv')
        
        networks_data = None
        if os.path.exists(networks_file):
            with open(networks_file, 'r') as f:
                networks_data = json.load(f)
        
        networks_flat = None
        if os.path.exists(networks_flat_file):
            networks_flat = pd.read_csv(networks_flat_file)

        # Create subtabs for Provider Networks analysis
        network_tabs = st.tabs([
            "Network Overview", 
            "Geographic Distribution", 
            "Organization Size", 
            "Network Connections"
        ])

        with network_tabs[0]:
            render_network_overview(providers, networks_data, networks_flat)
        
        with network_tabs[1]:
            render_geographic_distribution(providers)
        
        with network_tabs[2]:
            render_organization_size(providers)
        
        with network_tabs[3]:
            render_network_connections(providers, networks_data, networks_flat)

    except Exception as e:
        st.error(f"Error loading data: {e}")

def render_network_overview(providers, networks_data, networks_flat):
    """Render network overview analysis."""
    
    st.subheader("Provider Network Overview")
    
    # Calculate basic statistics
    total_providers = len(providers)
    unique_orgs = providers['ORGANIZATION NAME'].nunique() if 'ORGANIZATION NAME' in providers.columns else 0
    
    # Provider type distribution
    if 'PRACTICE LOCATION TYPE' in providers.columns:
        type_dist = providers['PRACTICE LOCATION TYPE'].value_counts()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Providers", total_providers)
        with col2:
            st.metric("Unique Organizations", unique_orgs)
        with col3:
            if 'HHA BRANCH' in type_dist.index:
                st.metric("HHA Branches", type_dist['HHA BRANCH'])
            else:
                st.metric("HHA Branches", 0)

        # Provider type pie chart
        fig = px.pie(
            values=type_dist.values,
            names=type_dist.index,
            title="Provider Types Distribution",
            color_discrete_sequence=[BRAND_COLORS['primary_blue'], BRAND_COLORS['primary_green'], 
                                   BRAND_COLORS['secondary_blue'], BRAND_COLORS['warning']]
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

    # Organization size distribution
    if 'ORGANIZATION NAME' in providers.columns:
        org_sizes = providers.groupby('ORGANIZATION NAME').size().reset_index(name='locations')
        
        # Create size categories
        org_sizes['size_category'] = pd.cut(
            org_sizes['locations'],
            bins=[0, 1, 5, 10, 50, float('inf')],
            labels=['Single Location', '2-5 Locations', '6-10 Locations', '11-50 Locations', '50+ Locations']
        )
        
        size_dist = org_sizes['size_category'].value_counts()
        
        fig = px.bar(
            x=size_dist.index,
            y=size_dist.values,
            title="Organization Size Distribution",
            labels={'x': 'Organization Size', 'y': 'Number of Organizations'},
            color=size_dist.values,
            color_continuous_scale='Blues'
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Top organizations by location count
    if 'ORGANIZATION NAME' in providers.columns:
        top_orgs = providers.groupby('ORGANIZATION NAME').size().reset_index(name='locations').sort_values('locations', ascending=False).head(10)
        
        st.subheader("Top 10 Organizations by Number of Locations")
        fig = px.bar(
            top_orgs,
            x='locations',
            y='ORGANIZATION NAME',
            orientation='h',
            title="Largest Provider Networks",
            color='locations',
            color_continuous_scale='Blues'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

def render_geographic_distribution(providers):
    """Render geographic distribution analysis."""
    
    st.subheader("Geographic Distribution of Providers")
    
    # State distribution
    if 'STATE' in providers.columns:
        state_counts = providers['STATE'].value_counts().head(20)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                x=state_counts.values,
                y=state_counts.index,
                orientation='h',
                title="Top 20 States by Provider Count",
                labels={'x': 'Number of Providers', 'y': 'State'},
                color=state_counts.values,
                color_continuous_scale='Blues'
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # State metrics
            total_states = providers['STATE'].nunique()
            top_state = state_counts.index[0]
            top_state_count = state_counts.iloc[0]
            
            st.metric("States with Providers", total_states)
            st.metric("Top State", f"{top_state}")
            st.metric("Providers in Top State", top_state_count)

    # Map visualization if lat/lon available
    lat_col = lon_col = None
    for c in providers.columns:
        if c.lower() in ['lat', 'latitude']:
            lat_col = c
        if c.lower() in ['lon', 'lng', 'longitude']:
            lon_col = c

    if lat_col and lon_col:
        # Convert to numeric and remove invalid coordinates
        providers_map = providers.copy()
        providers_map[lat_col] = pd.to_numeric(providers_map[lat_col], errors='coerce')
        providers_map[lon_col] = pd.to_numeric(providers_map[lon_col], errors='coerce')
        providers_map = providers_map.dropna(subset=[lat_col, lon_col])
        
        if len(providers_map) > 0:
            st.subheader("Provider Locations Map")
            
            # Sample data if too many points
            if len(providers_map) > 5000:
                providers_map = providers_map.sample(n=5000)
                st.info("Showing a sample of 5,000 providers for performance.")
            
            fig = px.scatter_mapbox(
                providers_map,
                lat=lat_col,
                lon=lon_col,
                hover_data=['ORGANIZATION NAME', 'STATE'] if all(col in providers_map.columns for col in ['ORGANIZATION NAME', 'STATE']) else None,
                color='PRACTICE LOCATION TYPE' if 'PRACTICE LOCATION TYPE' in providers_map.columns else None,
                zoom=3,
                center={'lat': 37.8, 'lon': -96},
                mapbox_style='carto-positron',
                title="Home Health Provider Locations"
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)

def render_organization_size(providers):
    """Render organization size analysis."""
    
    st.subheader("Organization Size Analysis")
    
    if 'ORGANIZATION NAME' not in providers.columns:
        st.error("Organization name data not available.")
        return
    
    # Calculate organization sizes
    org_sizes = providers.groupby('ORGANIZATION NAME').agg({
        'ORGANIZATION NAME': 'size',
        'STATE': 'nunique' if 'STATE' in providers.columns else lambda x: 1
    }).rename(columns={'ORGANIZATION NAME': 'total_locations', 'STATE': 'states_present'})
     # Add organization info
    org_info = providers.groupby('ORGANIZATION NAME').first()[['STATE', 'PRACTICE LOCATION TYPE']].fillna('Unknown')
    org_sizes = org_sizes.join(org_info, rsuffix='_first')
    
    # Embedded filters for organization analysis
    with st.expander("**Organization Filters**", expanded=True):
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            # Filter by minimum size
            min_size = st.slider("Minimum Locations", 1, org_sizes['total_locations'].max(), 1)
        
        with filter_col2:
            # Filter by states present
            min_states = st.slider("Minimum States", 1, org_sizes['states_present'].max(), 1)
        
        with filter_col3:
            # Filter by organization type
            if 'PRACTICE LOCATION TYPE' in org_sizes.columns:
                org_types = st.multiselect(
                    "Organization Types",
                    org_sizes['PRACTICE LOCATION TYPE'].unique(),
                    default=org_sizes['PRACTICE LOCATION TYPE'].unique()
                )
            else:
                org_types = None
    
    # Apply filters
    filtered_orgs = org_sizes[
        (org_sizes['total_locations'] >= min_size) &
        (org_sizes['states_present'] >= min_states)
    ]
    
    if org_types:
        filtered_orgs = filtered_orgs[filtered_orgs['PRACTICE LOCATION TYPE'].isin(org_types)]

    # Size vs States scatter plot
    col1, col2 = st.columns(2)
    
    with col1:
        if len(filtered_orgs) > 0:
            fig = px.scatter(
                filtered_orgs,
                x='total_locations',
                y='states_present',
                hover_data=['STATE', 'PRACTICE LOCATION TYPE'],
                title="Organization Size vs Geographic Spread",
                labels={'total_locations': 'Total Locations', 'states_present': 'States Present'},
                color='PRACTICE LOCATION TYPE' if 'PRACTICE LOCATION TYPE' in filtered_orgs.columns else None
            )
            fig.update_traces(marker=dict(size=8, opacity=0.7))
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Summary metrics
        if len(filtered_orgs) > 0:
            st.metric("Organizations Meeting Criteria", len(filtered_orgs))
            st.metric("Average Locations", f"{filtered_orgs['total_locations'].mean():.1f}")
            st.metric("Max States Covered", filtered_orgs['states_present'].max())
            
            # Largest multi-state organizations
            multi_state = filtered_orgs[filtered_orgs['states_present'] > 1].sort_values('total_locations', ascending=False).head(5)
            if len(multi_state) > 0:
                st.subheader("Top Multi-State Organizations")
                for idx, row in multi_state.iterrows():
                    st.write(f"**{idx}**: {row['total_locations']} locations across {row['states_present']} states")

def render_network_connections(providers, networks_data, networks_flat):
    """Render network connections analysis."""
    
    st.subheader("Network Connections Analysis")
    
    if networks_data:
        st.success("Network data available - showing detailed analysis")
        
        # Handle both dictionary and list formats
        if isinstance(networks_data, dict):
            # Network statistics for dictionary format
            num_networks = len(networks_data)
            
            # Calculate connections safely for dictionary format
            total_connections = 0
            network_sizes = []
            
            for network in networks_data.values():
                if isinstance(network, dict) and 'members' in network:
                    size = len(network['members'])
                elif isinstance(network, list):
                    size = len(network)
                else:
                    size = 1
                
                total_connections += size
                network_sizes.append(size)
        
        elif isinstance(networks_data, list):
            # Network statistics for list format
            num_networks = len(networks_data)
            
            # Calculate connections safely for list format
            total_connections = 0
            network_sizes = []
            
            for network in networks_data:
                if isinstance(network, dict) and 'members' in network:
                    size = len(network['members'])
                elif isinstance(network, list):
                    size = len(network)
                else:
                    size = 1
                
                total_connections += size
                network_sizes.append(size)
        
        else:
            st.error("Unexpected network data format")
            return
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Identified Networks", num_networks)
        with col2:
            st.metric("Total Network Connections", total_connections)
        with col3:
            avg_size = total_connections / num_networks if num_networks > 0 else 0
            st.metric("Average Network Size", f"{avg_size:.1f}")
        
        # Network size distribution chart
        if network_sizes:
            fig = px.histogram(
                x=network_sizes,
                title="Network Size Distribution",
                labels={'x': 'Network Size (Number of Members)', 'y': 'Number of Networks'},
                nbins=20
            )
            fig.update_traces(marker_color=BRAND_COLORS['primary_blue'])
            st.plotly_chart(fig, use_container_width=True)
    
    elif networks_flat is not None:
        st.info("Using flattened network data")
        
        # Show network relationships from flat file
        st.subheader("Network Relationships")
        st.dataframe(networks_flat.head(100), use_container_width=True)
    
    else:
        st.warning("No network connection data available.")
        
        # Alternative analysis: Find potential networks based on organization names
        st.subheader("Potential Network Analysis")
        st.info("Analyzing organization name patterns to identify potential networks...")
        
        if 'ORGANIZATION NAME' in providers.columns:
            # Find organizations with similar names (basic pattern matching)
            org_names = providers['ORGANIZATION NAME'].dropna().unique()
            
            # Group by first word (common network pattern)
            first_words = {}
            for name in org_names:
                first_word = name.split()[0].upper() if name else ""
                if len(first_word) > 3:  # Ignore short words
                    if first_word not in first_words:
                        first_words[first_word] = []
                    first_words[first_word].append(name)
            
            # Find potential networks (groups with multiple organizations)
            potential_networks = {k: v for k, v in first_words.items() if len(v) > 1}
            
            if potential_networks:
                st.write(f"Found {len(potential_networks)} potential network groups:")
                
                for network_name, orgs in list(potential_networks.items())[:10]:  # Show top 10
                    with st.expander(f"{network_name} Network ({len(orgs)} organizations)"):
                        for org in orgs:
                            st.write(f"• {org}")
            else:
                st.info("No obvious network patterns detected in organization names.")
