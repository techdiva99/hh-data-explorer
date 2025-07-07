import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np
from datetime import datetime

# --- BRAND COLORS ---
BRAND_COLORS = {
    'primary_blue        with st.expander("**HHCAHPS Measure Selection & Filters**", expanded=True):: '#00B4D8',
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

def render_quality_metrics_tab():
    """Render the Quality Metrics tab content."""
    
    # Paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    CMS_DATA_DIR = os.path.join(PROJECT_ROOT, 'data/cms_hh_quality')
    
    st.markdown('''
    ### Home Health Quality Metrics
    
    Analyze quality performance data from CMS including HHCAHPS (patient satisfaction) scores 
    and other quality measures for home health agencies.
    ''')

    try:
        # Load available quality data files
        quality_files = {
            'National': os.path.join(CMS_DATA_DIR, 'HH_National_Apr2025.csv'),
            'Provider': os.path.join(CMS_DATA_DIR, 'HH_Provider_Apr2025.csv'),
            'Zip': os.path.join(CMS_DATA_DIR, 'HH_Zip_Apr2025.csv'),
            'HHCAHPS_Provider': os.path.join(CMS_DATA_DIR, 'HHCAHPS_Provider_Apr2025.csv'),
            'HHCAHPS_State': os.path.join(CMS_DATA_DIR, 'HHCAHPS_State_Apr2025.csv')
        }
        
        # Check which files exist
        available_data = {}
        for name, filepath in quality_files.items():
            if os.path.exists(filepath):
                try:
                    available_data[name] = pd.read_csv(filepath)
                except Exception as e:
                    st.warning(f"Could not load {name} data: {e}")

        if not available_data:
            st.error("No quality data files found in the cms_hh_quality directory.")
            return

        # Create subtabs for Quality Analysis types
        quality_tabs = st.tabs([
            "Quality Overview", 
            "HHCAHPS Scores", 
            "State Comparisons", 
            "Provider Performance"
        ])

        with quality_tabs[0]:
            render_quality_overview(available_data)
        
        with quality_tabs[1]:
            render_hhcahps_analysis(available_data)
        
        with quality_tabs[2]:
            render_state_comparisons(available_data)
        
        with quality_tabs[3]:
            render_provider_performance(available_data)

    except Exception as e:
        st.error(f"Error loading quality data: {e}")

def render_quality_overview(available_data):
    """Render quality metrics overview."""
    
    st.subheader("Quality Metrics Overview")
    
    # Show available datasets
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Available Data Sources:**")
        for name, df in available_data.items():
            st.write(f"• {name}: {len(df)} records")
    
    with col2:
        # Summary statistics from national data if available
        if 'National' in available_data:
            national_data = available_data['National']
            st.markdown("**National Overview:**")
            st.metric("Total Records", len(national_data))
            
            # Look for common quality columns
            quality_cols = [col for col in national_data.columns if any(keyword in col.lower() 
                          for keyword in ['quality', 'score', 'rating', 'measure', 'performance'])]
            
            if quality_cols:
                st.write(f"Quality Measures Available: {len(quality_cols)}")

    # Provider data analysis if available
    if 'Provider' in available_data:
        provider_data = available_data['Provider']
        
        st.subheader("Provider Quality Distribution")
        
        # Look for star rating or quality score columns
        rating_cols = [col for col in provider_data.columns if any(keyword in col.lower() 
                      for keyword in ['star', 'rating', 'quality', 'score'])]
        
        if rating_cols:
            selected_metric = st.selectbox("Select Quality Metric", rating_cols)
            
            if selected_metric in provider_data.columns:
                # Convert to numeric if possible
                metric_data = pd.to_numeric(provider_data[selected_metric], errors='coerce')
                metric_data = metric_data.dropna()
                
                if len(metric_data) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Distribution histogram
                        fig = px.histogram(
                            x=metric_data,
                            title=f"Distribution of {selected_metric}",
                            labels={'x': selected_metric, 'y': 'Number of Providers'},
                            nbins=20
                        )
                        fig.update_traces(marker_color=BRAND_COLORS['primary_blue'])
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        # Summary statistics
                        st.metric("Average Score", f"{metric_data.mean():.2f}")
                        st.metric("Median Score", f"{metric_data.median():.2f}")
                        st.metric("Standard Deviation", f"{metric_data.std():.2f}")
                        st.metric("Providers with Data", len(metric_data))

    # HHCAHPS overview if available
    if 'HHCAHPS_Provider' in available_data:
        hhcahps_data = available_data['HHCAHPS_Provider']
        
        st.subheader("HHCAHPS (Patient Satisfaction) Overview")
        
        # Look for satisfaction measures
        satisfaction_cols = [col for col in hhcahps_data.columns if any(keyword in col.lower() 
                           for keyword in ['satisfaction', 'recommend', 'rating', 'score'])]
        
        if satisfaction_cols:
            # Show key satisfaction metrics
            col1, col2, col3 = st.columns(3)
            
            for i, col in enumerate(satisfaction_cols[:3]):
                with [col1, col2, col3][i]:
                    values = pd.to_numeric(hhcahps_data[col], errors='coerce').dropna()
                    if len(values) > 0:
                        st.metric(
                            col.replace('_', ' ').title()[:20] + "...",
                            f"{values.mean():.1f}%"
                        )

def render_hhcahps_analysis(available_data):
    """Render HHCAHPS-specific analysis."""
    
    st.subheader("HHCAHPS Patient Satisfaction Analysis")
    
    if 'HHCAHPS_Provider' not in available_data and 'HHCAHPS_State' not in available_data:
        st.error("No HHCAHPS data available.")
        return

    # Embedded HHCAHPS data level selection
    with st.expander("**HHCAHPS Analysis Settings**", expanded=True):
        setting_col1, setting_col2 = st.columns(2)
        
        with setting_col1:
            # Data level selection
            data_options = []
            if 'HHCAHPS_Provider' in available_data:
                data_options.append('Provider Level')
            if 'HHCAHPS_State' in available_data:
                data_options.append('State Level')
            
            data_level = st.selectbox("Data Level", data_options)
        
        with setting_col2:
            st.write("")  # Placeholder for additional controls

    # Select appropriate dataset
    if data_level == 'Provider Level' and 'HHCAHPS_Provider' in available_data:
        hhcahps_data = available_data['HHCAHPS_Provider']
        level_type = 'Provider'
    elif data_level == 'State Level' and 'HHCAHPS_State' in available_data:
        hhcahps_data = available_data['HHCAHPS_State']
        level_type = 'State'
    else:
        st.error("Selected data level not available.")
        return

    # Find HHCAHPS measures
    measure_cols = [col for col in hhcahps_data.columns if any(keyword in col.lower() 
                   for keyword in ['care', 'recommend', 'communication', 'medication', 'safety'])]
    
    if not measure_cols:
        # Try to find any percentage or score columns
        measure_cols = [col for col in hhcahps_data.columns if any(keyword in col.lower() 
                       for keyword in ['percent', 'score', 'rating'])]

    if measure_cols:
        # Embedded HHCAHPS measure filters
        with st.expander("� **HHCAHPS Measure Selection & Filters**", expanded=True):
            filter_col1, filter_col2 = st.columns(2)
            
            with filter_col1:
                selected_measures = st.multiselect(
                    "Select HHCAHPS Measures",
                    measure_cols,
                    default=measure_cols[:3] if len(measure_cols) >= 3 else measure_cols
                )
            
            with filter_col2:
                # State filter for provider level data
                if level_type == 'Provider' and 'State' in hhcahps_data.columns:
                    states = sorted(hhcahps_data['State'].dropna().unique())
                    selected_states = st.multiselect("Select States", states, default=states[:5])
                else:
                    selected_states = None

        # Filter data
        if selected_states and level_type == 'Provider':
            filtered_data = hhcahps_data[hhcahps_data['State'].isin(selected_states)]
        else:
            filtered_data = hhcahps_data

        # Display selected measures
        for measure in selected_measures:
            if measure in filtered_data.columns:
                st.subheader(f"{measure.replace('_', ' ').title()}")
                
                # Convert to numeric
                values = pd.to_numeric(filtered_data[measure], errors='coerce').dropna()
                
                if len(values) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Box plot
                        fig = px.box(
                            y=values,
                            title=f"{measure} Distribution",
                            labels={'y': measure}
                        )
                        fig.update_traces(marker_color=BRAND_COLORS['primary_blue'])
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        # Top performers
                        if level_type == 'Provider' and 'Provider Name' in filtered_data.columns:
                            top_data = filtered_data.nlargest(10, measure)
                            st.write("**Top 10 Performers:**")
                            for idx, row in top_data.iterrows():
                                provider_name = row.get('Provider Name', 'Unknown')[:30]
                                score = row[measure]
                                st.write(f"• {provider_name}: {score}")
                        
                        elif level_type == 'State':
                            # State rankings
                            state_col = 'State' if 'State' in filtered_data.columns else filtered_data.columns[0]
                            top_states = filtered_data.nlargest(10, measure)
                            st.write("**Top 10 States:**")
                            for idx, row in top_states.iterrows():
                                state = row.get(state_col, 'Unknown')
                                score = row[measure]
                                st.write(f"• {state}: {score}")

        # Correlation analysis if multiple measures selected
        if len(selected_measures) > 1:
            st.subheader("Measure Correlations")
            
            # Create correlation matrix
            numeric_data = filtered_data[selected_measures].apply(pd.to_numeric, errors='coerce')
            correlation_matrix = numeric_data.corr()
            
            fig = px.imshow(
                correlation_matrix,
                title="HHCAHPS Measure Correlations",
                color_continuous_scale='RdBu',
                aspect='auto'
            )
            st.plotly_chart(fig, use_container_width=True)

def render_state_comparisons(available_data):
    """Render state-level comparisons."""
    
    st.subheader("State-Level Quality Comparisons")
    
    # Use state-level data if available, otherwise aggregate provider data
    if 'HHCAHPS_State' in available_data:
        state_data = available_data['HHCAHPS_State']
        data_source = "HHCAHPS State Data"
    elif 'Provider' in available_data and 'State' in available_data['Provider'].columns:
        # Aggregate provider data by state
        provider_data = available_data['Provider']
        numeric_cols = provider_data.select_dtypes(include=[np.number]).columns
        state_data = provider_data.groupby('State')[numeric_cols].mean().reset_index()
        data_source = "Aggregated Provider Data"
    else:
        st.error("No state-level data available for comparison.")
        return

    st.info(f"Using: {data_source}")

    # Find quality measures
    quality_cols = [col for col in state_data.columns if col != 'State' and 
                   state_data[col].dtype in ['float64', 'int64']]
    
    if quality_cols:
        # Embedded state comparison controls
        with st.expander("**State Comparison Settings**", expanded=True):
            control_col1, control_col2 = st.columns(2)
            
            with control_col1:
                selected_metric = st.selectbox("Select Quality Metric", quality_cols)
            
            with control_col2:
                # Number of states to show
                num_states = st.slider("Number of States to Display", 5, min(50, len(state_data)), 15)

        if selected_metric:
            # Sort states by selected metric
            sorted_states = state_data.nlargest(num_states, selected_metric)
            
            # Bar chart of top states
            fig = px.bar(
                sorted_states,
                x='State' if 'State' in sorted_states.columns else sorted_states.index,
                y=selected_metric,
                title=f"Top {num_states} States - {selected_metric}",
                color=selected_metric,
                color_continuous_scale='Blues'
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary statistics
            col1, col2, col3 = st.columns(3)
            
            metric_values = pd.to_numeric(state_data[selected_metric], errors='coerce').dropna()
            
            with col1:
                st.metric("National Average", f"{metric_values.mean():.2f}")
            with col2:
                st.metric("Best State Score", f"{metric_values.max():.2f}")
            with col3:
                st.metric("Worst State Score", f"{metric_values.min():.2f}")

def render_provider_performance(available_data):
    """Render individual provider performance analysis."""
    
    st.subheader("Individual Provider Performance")
    
    if 'Provider' not in available_data:
        st.error("Provider-level data not available.")
        return

    provider_data = available_data['Provider']
    
    # Embedded provider search and filters
    with st.expander("**Provider Search & Filters**", expanded=True):
        search_col1, search_col2 = st.columns(2)
        
        with search_col1:
            # Provider search
            if 'Provider Name' in provider_data.columns:
                provider_names = provider_data['Provider Name'].dropna().unique()
                search_term = st.text_input("Search Provider Name")
                
                if search_term:
                    matching_providers = [name for name in provider_names 
                                        if search_term.lower() in name.lower()]
                    if matching_providers:
                        selected_provider = st.selectbox("Select Provider", matching_providers)
                    else:
                        st.warning("No providers found matching search term.")
                        selected_provider = None
                else:
                    selected_provider = st.selectbox("Select Provider", provider_names[:100])  # Limit for performance
            else:
                selected_provider = None
        
        with search_col2:
            # State filter
            if 'State' in provider_data.columns:
                states = sorted(provider_data['State'].dropna().unique())
                selected_states_perf = st.multiselect("Filter by State", states)
            else:
                selected_states_perf = []

    # Filter data
    filtered_providers = provider_data.copy()
    if selected_states_perf:
        filtered_providers = filtered_providers[filtered_providers['State'].isin(selected_states_perf)]

    # Individual provider analysis
    if selected_provider:
        provider_info = provider_data[provider_data['Provider Name'] == selected_provider]
        
        if not provider_info.empty:
            st.success(f"**Provider:** {selected_provider}")
            
            # Display provider metrics
            numeric_cols = provider_info.select_dtypes(include=[np.number]).columns
            
            if len(numeric_cols) > 0:
                # Create metrics grid
                cols = st.columns(min(4, len(numeric_cols)))
                
                for i, col in enumerate(numeric_cols[:8]):  # Show up to 8 metrics
                    with cols[i % 4]:
                        value = provider_info[col].iloc[0]
                        if pd.notna(value):
                            st.metric(col.replace('_', ' ').title()[:15], f"{value:.2f}")
                
                # Compare to state and national averages
                if 'State' in provider_info.columns:
                    provider_state = provider_info['State'].iloc[0]
                    state_avg_data = filtered_providers[filtered_providers['State'] == provider_state]
                    
                    st.subheader("Performance Comparison")
                    
                    comparison_data = []
                    for col in numeric_cols[:5]:  # Compare top 5 metrics
                        provider_val = provider_info[col].iloc[0]
                        state_avg = state_avg_data[col].mean()
                        national_avg = provider_data[col].mean()
                        
                        if pd.notna(provider_val):
                            comparison_data.append({
                                'Metric': col.replace('_', ' ').title(),
                                'Provider': provider_val,
                                'State Avg': state_avg,
                                'National Avg': national_avg
                            })
                    
                    if comparison_data:
                        comp_df = pd.DataFrame(comparison_data)
                        
                        # Melt for plotting
                        comp_melted = comp_df.melt(
                            id_vars=['Metric'],
                            value_vars=['Provider', 'State Avg', 'National Avg'],
                            var_name='Level',
                            value_name='Score'
                        )
                        
                        fig = px.bar(
                            comp_melted,
                            x='Metric',
                            y='Score',
                            color='Level',
                            barmode='group',
                            title="Provider vs State vs National Averages"
                        )
                        fig.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig, use_container_width=True)

    # Provider rankings
    st.subheader("Provider Rankings")
    
    numeric_cols = filtered_providers.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        ranking_metric = st.selectbox("Rank by Metric", numeric_cols, key="ranking")
        
        if ranking_metric:
            # Top providers
            top_providers = filtered_providers.nlargest(20, ranking_metric)
            
            if 'Provider Name' in top_providers.columns:
                # Create ranking table
                ranking_data = top_providers[['Provider Name', 'State', ranking_metric]].copy()
                ranking_data['Rank'] = range(1, len(ranking_data) + 1)
                ranking_data = ranking_data[['Rank', 'Provider Name', 'State', ranking_metric]]
                
                st.dataframe(ranking_data, use_container_width=True, hide_index=True)
