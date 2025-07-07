import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np
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

def clean_numeric_column(df, column_name):
    """Clean and convert a column to numeric, handling percentages and commas."""
    if column_name not in df.columns:
        return df
    
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Convert to string first to handle mixed types
    col_data = df[column_name].astype(str)
    
    # Handle percentage strings (e.g., "61.52%" -> 0.6152)
    if col_data.str.contains('%', na=False).any():
        col_data = col_data.str.replace('%', '').str.strip()
        # Convert to numeric and divide by 100 for percentages
        df[column_name] = pd.to_numeric(col_data, errors='coerce') / 100
    else:
        # Handle comma-separated numbers (e.g., "14,940" -> 14940)
        col_data = col_data.str.replace(',', '').str.strip()
        df[column_name] = pd.to_numeric(col_data, errors='coerce')
    
    return df

def load_data_with_encoding_fallback(filepath):
    """Load CSV data with multiple encoding attempts and data cleaning."""
    if not os.path.exists(filepath):
        return None
        
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            
            # Clean common numeric columns
            numeric_keywords = ['penetration', 'saturation', 'enrollment', 'cost', 'revenue', 'expense', 'charge', 'payment', 'eligible']
            for col in df.columns:
                if any(keyword in col.lower() for keyword in numeric_keywords):
                    df = clean_numeric_column(df, col)
            
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            st.warning(f"Error loading {os.path.basename(filepath)} with {encoding}: {e}")
            continue
    
    return None

def render_market_analysis_tab():
    """Render the Market Analysis tab content."""
    
    # Paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
    
    st.markdown('''
    ### Market Analysis & Business Intelligence
    
    Analyze market saturation, cost data, enrollment patterns, and business opportunities 
    in the home health sector.
    ''')

    try:
        # Load available market data
        data_files = {
            'Market_Saturation': os.path.join(DATA_DIR, 'market_potential/State_County_Penetration_MA_2025_06.csv'),
            'Cost_Report': os.path.join(DATA_DIR, 'hh_cost/HHA_Cost_Report_2022.csv'),
            'Enrollment': os.path.join(DATA_DIR, 'hh_enrollment/HHA_Enrollments_Q2_2025.csv'),
            'Provider_Master': os.path.join(DATA_DIR, 'processed/new_final_master_provider.csv'),
            'Market_Saturation_LatLon': os.path.join(DATA_DIR, 'processed/State_County_Penetration_MA_latlon.csv'),
            'Provider_Market_Data': os.path.join(DATA_DIR, 'processed/masterprovider_with_penetration.csv')
        }
        
        # Load available data
        available_data = {}
        for name, filepath in data_files.items():
            if os.path.exists(filepath):
                try:
                    df = load_data_with_encoding_fallback(filepath)
                    if df is not None:
                        available_data[name] = df
                    else:
                        st.warning(f"Could not load {name} data: All encoding attempts failed")
                except Exception as e:
                    st.warning(f"Could not load {name} data: {e}")
                    continue

        if not available_data:
            st.error("No market analysis data files found.")
            return

        # Create subtabs for Market Analysis types
        analysis_tabs = st.tabs([
            "Market Overview", 
            "Market Saturation", 
            "Cost Analysis", 
            "Enrollment Trends", 
            "Opportunity Assessment"
        ])

        with analysis_tabs[0]:
            render_market_overview(available_data)
        
        with analysis_tabs[1]:
            render_market_saturation_analysis(available_data)
        
        with analysis_tabs[2]:
            render_cost_analysis(available_data)
        
        with analysis_tabs[3]:
            render_enrollment_analysis(available_data)
        
        with analysis_tabs[4]:
            render_opportunity_assessment(available_data)

    except Exception as e:
        st.error(f"Error loading market data: {e}")

def render_market_overview(available_data):
    """Render market overview analysis."""
    
    st.subheader("Home Health Market Overview")
    
    # Show available datasets
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Available Market Data:**")
        for name, df in available_data.items():
            st.write(f"• {name}: {len(df):,} records")
    
    with col2:
        # Key market metrics from penetration data
        if 'Market_Saturation' in available_data or 'Market_Saturation_LatLon' in available_data:
            pen_data = available_data.get('Market_Saturation', available_data.get('Market_Saturation_LatLon'))
            
            st.markdown("**Market Summary:**")
            
            # Look for relevant columns
            ma_cols = [col for col in pen_data.columns if 'MA' in col.upper()]
            enrollment_cols = [col for col in pen_data.columns if 'enrollment' in col.lower()]
            saturation_cols = [col for col in pen_data.columns if 'saturation' in col.lower()]
            
            if enrollment_cols:
                enrollment_col = enrollment_cols[0]
                
                # Clean enrollment column to ensure it's numeric
                pen_data_clean = pen_data.copy()
                if pen_data_clean[enrollment_col].dtype == 'object':
                    pen_data_clean = clean_numeric_column(pen_data_clean, enrollment_col)
                
                total_enrollment = pen_data_clean[enrollment_col].sum() if len(pen_data_clean) > 0 else 0
                st.metric("Total Medicare Advantage Enrollment", f"{total_enrollment:,.0f}")
            
            if saturation_cols:
                # Ensure saturation column is numeric (should be cleaned by load function)
                sat_col = saturation_cols[0]
                if pen_data[sat_col].dtype == 'object':
                    pen_data = clean_numeric_column(pen_data, sat_col)
                
                avg_saturation = pen_data[sat_col].mean() if len(pen_data) > 0 else 0
                st.metric("Average Market Saturation Rate", f"{avg_saturation:.1%}")
            
            st.metric("Geographic Markets", len(pen_data))

    # Market size visualization
    if 'Market_Saturation' in available_data or 'Market_Saturation_LatLon' in available_data:
        pen_data = available_data.get('Market_Saturation', available_data.get('Market_Saturation_LatLon'))
        
        st.subheader("Market Size Distribution")
        
        # Find enrollment column
        enrollment_cols = [col for col in pen_data.columns if 'enrollment' in col.lower()]
        if enrollment_cols:
            enrollment_col = enrollment_cols[0]
            
            # Clean enrollment column to ensure it's numeric before aggregation
            pen_data_viz = pen_data.copy()
            if pen_data_viz[enrollment_col].dtype == 'object':
                pen_data_viz = clean_numeric_column(pen_data_viz, enrollment_col)
            
            # State-level aggregation if state column exists
            state_cols = [col for col in pen_data_viz.columns if 'state' in col.lower()]
            if state_cols:
                state_col = state_cols[0]
                
                # Remove rows with invalid enrollment data before grouping
                pen_data_viz = pen_data_viz.dropna(subset=[enrollment_col])
                
                if len(pen_data_viz) > 0:
                    state_data = pen_data_viz.groupby(state_col)[enrollment_col].sum().sort_values(ascending=False).head(20)
                    
                    if len(state_data) > 0:
                        fig = px.bar(
                            x=state_data.values,
                            y=state_data.index,
                            orientation='h',
                            title="Top 20 States by Medicare Advantage Enrollment",
                            labels={'x': 'Total Enrollment', 'y': 'State'},
                            color=state_data.values,
                            color_continuous_scale='Blues'
                        )
                        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No valid enrollment data available for state visualization")
                else:
                    st.info("No valid enrollment data found after cleaning")
            else:
                st.info("No state column found for geographic aggregation")

    # Provider market presence
    if 'Provider_Master' in available_data:
        providers = available_data['Provider_Master']
        
        st.subheader("Provider Market Presence")
        
        if 'STATE' in providers.columns:
            state_provider_counts = providers['STATE'].value_counts().head(15)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(
                    values=state_provider_counts.values,
                    names=state_provider_counts.index,
                    title="Provider Distribution by State (Top 15)"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Market concentration metrics
                total_providers = len(providers)
                top_5_states = state_provider_counts.head(5).sum()
                concentration = top_5_states / total_providers
                
                st.metric("Total Providers", f"{total_providers:,}")
                st.metric("States with Providers", len(state_provider_counts))
                st.metric("Top 5 States Concentration", f"{concentration:.1%}")

def render_market_saturation_analysis(available_data):
    """Render market saturation analysis."""
    
    st.subheader("Medicare Advantage Market Saturation Analysis")
    
    # Use market saturation data
    sat_data = None
    if 'Market_Saturation_LatLon' in available_data:
        sat_data = available_data['Market_Saturation_LatLon']
    elif 'Market_Saturation' in available_data:
        sat_data = available_data['Market_Saturation']
    
    if sat_data is None:
        st.error("No market saturation data available.")
        return

    # Embedded market saturation analysis filters
    with st.expander("**Market Saturation Analysis Filters**", expanded=True):
        filter_col1, filter_col2 = st.columns(2)
        
        with filter_col1:
            # Find relevant columns (check both saturation and legacy penetration terms)
            saturation_cols = [col for col in sat_data.columns if 'saturation' in col.lower() or 'penetration' in col.lower()]
            enrollment_cols = [col for col in sat_data.columns if 'enrollment' in col.lower()]
            state_cols = [col for col in sat_data.columns if 'state' in col.lower()]
            
            # Metric selection
            available_metrics = saturation_cols + enrollment_cols
            if available_metrics:
                selected_metric = st.selectbox("Primary Metric", available_metrics)
            else:
                st.error("No market saturation or enrollment metrics found.")
                return
        
        with filter_col2:
            # State filter
            if state_cols:
                state_col = state_cols[0]
                states = sorted(sat_data[state_col].dropna().unique())
                selected_states = st.multiselect("Filter by State", states, default=states[:10])
            else:
                selected_states = None
                state_col = None

    # Apply filters
    if selected_states and state_col:
        filtered_data = sat_data[sat_data[state_col].isin(selected_states)]
    else:
        filtered_data = sat_data

    # Ensure selected metric is numeric and clean
    if selected_metric in filtered_data.columns:
        filtered_data = filtered_data.copy()
        
        # Clean the selected metric if it's not already numeric
        if filtered_data[selected_metric].dtype == 'object':
            filtered_data = clean_numeric_column(filtered_data, selected_metric)
        
        # Remove rows where the metric couldn't be converted to numeric
        filtered_data = filtered_data.dropna(subset=[selected_metric])
        
        if len(filtered_data) == 0:
            st.error(f"No valid numeric data found for {selected_metric}")
            return

    # Market saturation rate distribution
    if selected_metric in filtered_data.columns:
        values = filtered_data[selected_metric].dropna()
        
        if len(values) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribution histogram
                fig = px.histogram(
                    x=values,
                    title=f"Distribution of {selected_metric}",
                    labels={'x': selected_metric, 'y': 'Number of Counties'},
                    nbins=30
                )
                fig.update_traces(marker_color=BRAND_COLORS['primary_blue'])
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Box plot by state if available
                if state_col and len(selected_states) <= 10:
                    fig = px.box(
                        filtered_data,
                        x=state_col,
                        y=selected_metric,
                        title=f"{selected_metric} by State"
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    # Summary statistics
                    st.metric("Average", f"{values.mean():.2f}")
                    st.metric("Median", f"{values.median():.2f}")
                    st.metric("75th Percentile", f"{values.quantile(0.75):.2f}")
                    st.metric("Markets Analyzed", len(values))

    # Geographic visualization if lat/lon available
    if 'Market_Saturation_LatLon' in available_data:
        sat_latlon = available_data['Market_Saturation_LatLon']
        
        # Check for lat/lon columns
        lat_col = lon_col = None
        for col in sat_latlon.columns:
            if col.lower() in ['lat', 'latitude']:
                lat_col = col
            if col.lower() in ['lon', 'lng', 'longitude']:
                lon_col = col
        
        if lat_col and lon_col and selected_metric in sat_latlon.columns:
            st.subheader("Geographic Market Saturation Map")
            
            # Clean data and ensure proper data types
            map_data = sat_latlon.copy()
            map_data[lat_col] = pd.to_numeric(map_data[lat_col], errors='coerce')
            map_data[lon_col] = pd.to_numeric(map_data[lon_col], errors='coerce')
            
            # Clean the selected metric if it's not already numeric
            if map_data[selected_metric].dtype == 'object':
                map_data = clean_numeric_column(map_data, selected_metric)
            
            map_data = map_data.dropna(subset=[lat_col, lon_col, selected_metric])
            
            # Apply state filter
            if selected_states and state_col and state_col in map_data.columns:
                map_data = map_data[map_data[state_col].isin(selected_states)]
            
            if len(map_data) > 0:
                # Sample if too many points
                if len(map_data) > 2000:
                    map_data = map_data.sample(n=2000)
                    st.info("Showing a sample of 2,000 counties for performance.")
                
                fig = px.scatter_mapbox(
                    map_data,
                    lat=lat_col,
                    lon=lon_col,
                    color=selected_metric,
                    size=selected_metric,
                    hover_data=[state_col] if state_col else None,
                    color_continuous_scale='Viridis',
                    zoom=3,
                    center={'lat': 37.8, 'lon': -96},
                    mapbox_style='carto-positron',
                    title=f"{selected_metric} Geographic Distribution"
                )
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)

    # Top/Bottom performers
    if selected_metric in filtered_data.columns:
        # Data should already be cleaned and numeric from earlier processing
        ranking_data = filtered_data.copy()
        
        # Double-check that data is numeric
        if ranking_data[selected_metric].dtype == 'object':
            ranking_data = clean_numeric_column(ranking_data, selected_metric)
            ranking_data = ranking_data.dropna(subset=[selected_metric])
        
        if len(ranking_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Top Performers")
                county_col = None
                for col in ['county', 'County', 'COUNTY', 'County Name']:
                    if col in ranking_data.columns:
                        county_col = col
                        break
                
                if county_col:
                    try:
                        top_counties = ranking_data.nlargest(10, selected_metric)
                        
                        for idx, row in top_counties.iterrows():
                            county = row.get(county_col, 'Unknown')
                            state = row.get(state_col, 'Unknown') if state_col else 'Unknown'
                            value = row[selected_metric]
                            if pd.notna(value):
                                if 'saturation' in selected_metric.lower():
                                    st.write(f"• {county}, {state}: {value:.1%}")
                                else:
                                    st.write(f"• {county}, {state}: {value:,.0f}")
                    except Exception as e:
                        st.warning(f"Could not rank top performers: {e}")
                else:
                    st.info("County information not available for ranking")
            
            with col2:
                st.subheader("Improvement Opportunities")
                if county_col:
                    try:
                        bottom_counties = ranking_data.nsmallest(10, selected_metric)
                        
                        for idx, row in bottom_counties.iterrows():
                            county = row.get(county_col, 'Unknown')
                            state = row.get(state_col, 'Unknown') if state_col else 'Unknown'
                            value = row[selected_metric]
                            if pd.notna(value):
                                if 'saturation' in selected_metric.lower():
                                    st.write(f"• {county}, {state}: {value:.1%}")
                                else:
                                    st.write(f"• {county}, {state}: {value:,.0f}")
                    except Exception as e:
                        st.warning(f"Could not rank improvement opportunities: {e}")
                else:
                    st.info("County information not available for ranking")
        else:
            st.warning(f"No valid numeric data available for ranking by {selected_metric}")

def render_cost_analysis(available_data):
    """Render cost analysis."""
    
    st.subheader("Home Health Cost Analysis")
    
    if 'Cost_Report' not in available_data:
        st.error("Cost report data not available.")
        return

    cost_data = available_data['Cost_Report']
    
    # Embedded cost analysis filters
    with st.expander("**Cost Analysis Filters**", expanded=True):
        filter_col1, filter_col2 = st.columns(2)
        
        with filter_col1:
            # Find cost-related columns
            cost_cols = [col for col in cost_data.columns if any(keyword in col.lower() 
                        for keyword in ['cost', 'expense', 'revenue', 'charge', 'payment'])]
            
            if cost_cols:
                cost_metric = st.selectbox("Cost Metric", cost_cols)
            else:
                st.error("No cost metrics found in the data.")
                return
        
        with filter_col2:
            # State filter if available
            if 'State' in cost_data.columns:
                states = sorted(cost_data['State'].dropna().unique())
                selected_cost_states = st.multiselect("Filter by State", states, default=states[:10])
            else:
                selected_cost_states = None

    # Apply filters
    if selected_cost_states:
        filtered_cost = cost_data[cost_data['State'].isin(selected_cost_states)]
    else:
        filtered_cost = cost_data

    # Cost distribution analysis
    if cost_metric in filtered_cost.columns:
        # Ensure the cost metric is numeric using our cleaning function
        filtered_cost_clean = filtered_cost.copy()
        if filtered_cost_clean[cost_metric].dtype == 'object':
            filtered_cost_clean = clean_numeric_column(filtered_cost_clean, cost_metric)
        
        cost_values = filtered_cost_clean[cost_metric].dropna()
        
        if len(cost_values) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Cost distribution
                fig = px.histogram(
                    x=cost_values,
                    title=f"Distribution of {cost_metric}",
                    labels={'x': cost_metric, 'y': 'Number of Providers'},
                    nbins=30
                )
                fig.update_traces(marker_color=BRAND_COLORS['primary_green'])
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Summary statistics
                st.metric("Average Cost", f"${cost_values.mean():,.0f}")
                st.metric("Median Cost", f"${cost_values.median():,.0f}")
                st.metric("95th Percentile", f"${cost_values.quantile(0.95):,.0f}")
                st.metric("Providers Analyzed", f"{len(cost_values):,}")

    # State-level cost comparison
    if 'State' in filtered_cost.columns and selected_cost_states and cost_metric in filtered_cost.columns:
        # Ensure numeric data for aggregation using our cleaning function
        cost_data_numeric = filtered_cost.copy()
        if cost_data_numeric[cost_metric].dtype == 'object':
            cost_data_numeric = clean_numeric_column(cost_data_numeric, cost_metric)
        
        cost_data_numeric = cost_data_numeric.dropna(subset=[cost_metric])
        
        if len(cost_data_numeric) > 0:
            state_costs = cost_data_numeric.groupby('State')[cost_metric].agg(['mean', 'median', 'count']).reset_index()
            state_costs.columns = ['State', 'Average_Cost', 'Median_Cost', 'Provider_Count']
            
            # Filter for states with enough data
            state_costs = state_costs[state_costs['Provider_Count'] >= 3]
            
            if len(state_costs) > 0:
                fig = px.bar(
                    state_costs,
                    x='State',
                    y='Average_Cost',
                    title=f"Average {cost_metric} by State",
                    color='Average_Cost',
                    color_continuous_scale='Greens'
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No states have sufficient data for comparison (minimum 3 providers required)")
        else:
            st.warning(f"No valid numeric data found for {cost_metric}")

def render_enrollment_analysis(available_data):
    """Render enrollment trends analysis."""
    
    st.subheader("Home Health Enrollment Analysis")
    
    if 'Enrollment' not in available_data:
        st.error("Enrollment data not available.")
        return

    enrollment_data = available_data['Enrollment']
    
    # Analyze enrollment patterns
    st.write(f"**Dataset Overview:** {len(enrollment_data):,} enrollment records")
    
    # Show data structure
    with st.expander("View Data Structure"):
        st.write("**Columns in enrollment data:**")
        for col in enrollment_data.columns:
            st.write(f"• {col}")
        
        st.write("**Sample data:**")
        st.dataframe(enrollment_data.head(10), use_container_width=True)

    # Basic enrollment statistics
    numeric_cols = enrollment_data.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 0:
        # Embedded enrollment analysis controls
        with st.expander("**Enrollment Analysis Controls**", expanded=True):
            enrollment_metric = st.selectbox("Enrollment Metric", numeric_cols)
        
        if enrollment_metric in enrollment_data.columns:
            values = enrollment_data[enrollment_metric].dropna()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Enrollments", f"{values.sum():,.0f}")
            with col2:
                st.metric("Average per Record", f"{values.mean():.1f}")
            with col3:
                st.metric("Records with Data", f"{len(values):,}")
            
            # Distribution chart
            fig = px.histogram(
                x=values,
                title=f"Distribution of {enrollment_metric}",
                nbins=30
            )
            fig.update_traces(marker_color=BRAND_COLORS['secondary_blue'])
            st.plotly_chart(fig, use_container_width=True)

def render_opportunity_assessment(available_data):
    """Render market opportunity assessment."""
    
    st.subheader("Market Opportunity Assessment")
    
    # Combine multiple data sources for opportunity analysis
    opportunities = []
    
    # Penetration-based opportunities
    if 'Market_Saturation' in available_data or 'Market_Saturation_LatLon' in available_data:
        pen_data = available_data.get('Market_Saturation', available_data.get('Market_Saturation_LatLon'))
        
        penetration_cols = [col for col in pen_data.columns if 'penetration' in col.lower()]
        enrollment_cols = [col for col in pen_data.columns if 'enrollment' in col.lower()]
        
        if penetration_cols and enrollment_cols:
            pen_col = penetration_cols[0]
            enroll_col = enrollment_cols[0]
            
            # High enrollment, low penetration = opportunity
            pen_data_clean = pen_data.copy()
            
            # Ensure numeric data types using our cleaning function
            if pen_data_clean[pen_col].dtype == 'object':
                pen_data_clean = clean_numeric_column(pen_data_clean, pen_col)
            if pen_data_clean[enroll_col].dtype == 'object':
                pen_data_clean = clean_numeric_column(pen_data_clean, enroll_col)
            
            pen_data_clean = pen_data_clean.dropna(subset=[pen_col, enroll_col])
            
            if len(pen_data_clean) == 0:
                st.warning("No valid numeric data available for opportunity analysis")
                return
            
            # Define opportunity criteria
            high_enrollment = pen_data_clean[enroll_col] > pen_data_clean[enroll_col].quantile(0.75)
            low_penetration = pen_data_clean[pen_col] < pen_data_clean[pen_col].quantile(0.25)
            
            opportunity_markets = pen_data_clean[high_enrollment & low_penetration]
            
            st.subheader("High-Opportunity Markets")
            st.write("Markets with high Medicare Advantage enrollment but low penetration rates")
            
            if len(opportunity_markets) > 0:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Opportunity scatter plot
                    fig = px.scatter(
                        pen_data_clean,
                        x=pen_col,
                        y=enroll_col,
                        title="Market Opportunity Matrix",
                        labels={pen_col: 'Penetration Rate', enroll_col: 'Enrollment'},
                        color=np.where(
                            (pen_data_clean[enroll_col] > pen_data_clean[enroll_col].quantile(0.75)) & 
                            (pen_data_clean[pen_col] < pen_data_clean[pen_col].quantile(0.25)),
                            'High Opportunity', 'Standard'
                        )
                    )
                    # Add quadrant lines
                    fig.add_hline(y=pen_data_clean[enroll_col].quantile(0.75), line_dash="dash", line_color="red")
                    fig.add_vline(x=pen_data_clean[pen_col].quantile(0.25), line_dash="dash", line_color="red")
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.metric("High-Opportunity Markets", len(opportunity_markets))
                    st.metric("Total Market Potential", f"{opportunity_markets[enroll_col].sum():,.0f}")
                    
                    # Top opportunities
                    st.write("**Top 5 Opportunity Markets:**")
                    top_opps = opportunity_markets.nlargest(5, enroll_col)
                    
                    state_col = None
                    for col in opportunity_markets.columns:
                        if 'state' in col.lower():
                            state_col = col
                            break
                    
                    for idx, row in top_opps.iterrows():
                        location = row.get(state_col, 'Unknown') if state_col else 'Unknown'
                        enrollment = row[enroll_col]
                        penetration = row[pen_col]
                        st.write(f"• {location}: {enrollment:,.0f} enrollment, {penetration:.1%} penetration")

    # Coverage desert opportunities
    if 'Provider_Master' in available_data:
        st.subheader("Coverage Desert Opportunities")
        
        # Load coverage desert data if available
        desert_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
                                  'data/processed/hh_coverage_deserts_severity.csv')
        
        if os.path.exists(desert_file):
            try:
                desert_data = pd.read_csv(desert_file)
                severe_deserts = desert_data[desert_data['desert_severity'] == 'severe']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Severe Coverage Deserts", len(severe_deserts))
                    
                    if 'Enrolled' in severe_deserts.columns:
                        total_underserved = severe_deserts['Enrolled'].sum()
                        st.metric("Underserved Population", f"{total_underserved:,.0f}")
                
                with col2:
                    if 'closest_provider_distance' in severe_deserts.columns:
                        avg_distance = severe_deserts['closest_provider_distance'].mean()
                        st.metric("Avg Distance to Provider", f"{avg_distance:.1f} miles")
                
                # Show severe desert locations
                if len(severe_deserts) > 0:
                    st.write("**Counties with Severe Coverage Gaps:**")
                    desert_sample = severe_deserts.head(10)
                    display_cols = ['FIPS', 'severity_reason', 'Enrolled', 'provider_count', 'closest_provider_distance']
                    available_cols = [col for col in display_cols if col in desert_sample.columns]
                    
                    if available_cols:
                        st.dataframe(desert_sample[available_cols], use_container_width=True)
                
            except Exception as e:
                st.warning(f"Could not load coverage desert data: {e}")

    # Investment priority scoring
    st.subheader("Investment Priority Scoring")
    st.info("This section would combine multiple factors to create investment priority scores:")
    
    scoring_factors = [
        "Market size (Medicare Advantage enrollment)",
        "Current market saturation rate (lower = higher opportunity)",
        "Provider density (gaps in coverage)",
        "Population growth trends",
        "Competitive landscape",
        "Regulatory environment"
    ]
    
    for factor in scoring_factors:
        st.write(f"• {factor}")
    
    st.write("**Next Steps for Implementation:**")
    st.write("1. Integrate additional data sources (demographics, economics)")
    st.write("2. Develop weighted scoring algorithm")
    st.write("3. Create interactive opportunity dashboard")
    st.write("4. Add competitive intelligence data")
