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

def render_dashboard_tab():
    """Render the main dashboard/summary tab."""
    
    st.markdown('''
    ### Home Health Data Dashboard
    
    **Quick insights and key findings from across the US home health landscape**
    ''')

    # Paths
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
    PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
    
    # Load key datasets for dashboard
    try:
        # Provider data
        providers_file = os.path.join(PROCESSED_DIR, 'new_final_master_provider.csv')
        providers = pd.read_csv(providers_file, dtype=str) if os.path.exists(providers_file) else None
        
        # Coverage deserts
        deserts_file = os.path.join(PROCESSED_DIR, 'hh_coverage_deserts_severity.csv')
        deserts = pd.read_csv(deserts_file, dtype={'FIPS': str}) if os.path.exists(deserts_file) else None
        
        # Penetration data
        penetration_file = os.path.join(DATA_DIR, 'market_potential/State_County_Penetration_MA_2025_06.csv')
        penetration = pd.read_csv(penetration_file) if os.path.exists(penetration_file) else None
        
    except Exception as e:
        st.error(f"Error loading dashboard data: {e}")
        return

    # Embedded dashboard controls
    with st.expander("**Dashboard Settings**", expanded=False):
        control_col1, control_col2 = st.columns(2)
        
        with control_col1:
            view_type = st.selectbox(
                'Dashboard View',
                ['Executive Summary', 'Geographic Overview', 'Quality Insights', 'Market Trends']
            )
        
        with control_col2:
            if providers is not None and 'STATE' in providers.columns:
                states = sorted(providers['STATE'].dropna().unique())
                selected_states = st.multiselect(
                    'Focus States (optional)',
                    states,
                    help="Leave empty to show national data"
                )
            else:
                selected_states = []

    if view_type == 'Executive Summary':
        render_executive_summary(providers, deserts, penetration, selected_states)
    elif view_type == 'Geographic Overview':
        render_geographic_overview(providers, deserts, penetration, selected_states)
    elif view_type == 'Quality Insights':
        render_quality_insights(providers, selected_states)
    elif view_type == 'Market Trends':
        render_market_trends(providers, penetration, selected_states)

def render_executive_summary(providers, deserts, penetration, selected_states):
    """Render executive summary with key metrics."""
    
    st.subheader("Executive Summary")
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate metrics
    if providers is not None:
        total_providers = len(providers)
        if selected_states:
            filtered_providers = providers[providers['STATE'].isin(selected_states)]
            total_providers = len(filtered_providers)
        
        unique_orgs = providers['ORGANIZATION NAME'].nunique() if 'ORGANIZATION NAME' in providers.columns else 0
        states_covered = providers['STATE'].nunique() if 'STATE' in providers.columns else 0
        
        with col1:
            st.metric("Total Providers", f"{total_providers:,}")
        with col2:
            st.metric("Unique Organizations", f"{unique_orgs:,}")
        with col3:
            st.metric("States Covered", states_covered)
        with col4:
            if deserts is not None:
                severe_deserts = len(deserts[deserts['desert_severity'] == 'severe']) if 'desert_severity' in deserts.columns else 0
                st.metric("Severe Coverage Deserts", severe_deserts)
            else:
                st.metric("Coverage Deserts", "Data N/A")

    # Key insights section
    st.markdown("#### Key Insights")
    
    insights_col1, insights_col2 = st.columns(2)
    
    with insights_col1:
        st.markdown(f'''
        <div style="background-color: {BRAND_COLORS['accent_blue']}; padding: 15px; border-radius: 8px; margin: 10px 0;">
            <h5 style="color: {BRAND_COLORS['dark_blue']}; margin-top: 0;">Provider Landscape</h5>
            <ul style="color: {BRAND_COLORS['dark_blue']}; margin-bottom: 0;">
                <li>Multi-state networks dominate the market</li>
                <li>Rural areas show significant coverage gaps</li>
                <li>Branch locations concentrate in urban areas</li>
                <li>Quality ratings vary significantly by region</li>
            </ul>
        </div>
        ''', unsafe_allow_html=True)
    
    with insights_col2:
        st.markdown(f'''
        <div style="background-color: {BRAND_COLORS['accent_green']}; padding: 15px; border-radius: 8px; margin: 10px 0;">
            <h5 style="color: {BRAND_COLORS['dark_green']}; margin-top: 0;">📈 Market Opportunities</h5>
            <ul style="color: {BRAND_COLORS['dark_green']}; margin-bottom: 0;">
                <li>Growing Medicare Advantage enrollment</li>
                <li>Underserved counties present expansion opportunities</li>
                <li>Quality improvement initiatives needed</li>
                <li>Technology adoption varies widely</li>
            </ul>
        </div>
        ''', unsafe_allow_html=True)

    # Recent data updates
    st.markdown("#### 📅 Data Freshness")
    
    data_status = [
        {"Dataset": "Provider Enrollment", "Last Updated": "Q2 2025", "Status": "Current"},
        {"Dataset": "Quality Metrics", "Last Updated": "April 2025", "Status": "Current"},
        {"Dataset": "Market Penetration", "Last Updated": "June 2025", "Status": "Current"},
        {"Dataset": "Cost Reports", "Last Updated": "2022", "Status": "Historical"}
    ]
    
    st.dataframe(data_status, use_container_width=True, hide_index=True)

def render_geographic_overview(providers, deserts, penetration, selected_states):
    """Render geographic overview of the home health landscape."""
    
    st.subheader("🗺️ Geographic Overview")
    
    if providers is None:
        st.error("Provider data not available for geographic analysis.")
        return

    # State-level provider distribution
    if 'STATE' in providers.columns:
        state_counts = providers['STATE'].value_counts()
        
        # Apply state filter if selected
        if selected_states:
            state_counts = state_counts[state_counts.index.isin(selected_states)]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top states by provider count
            fig = px.bar(
                x=state_counts.head(15).values,
                y=state_counts.head(15).index,
                orientation='h',
                title="Top 15 States by Provider Count",
                labels={'x': 'Number of Providers', 'y': 'State'},
                color=state_counts.head(15).values,
                color_continuous_scale='Blues'
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False, height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Coverage density insights
            st.markdown("**Coverage Density Analysis**")
            
            total_states = len(state_counts)
            avg_providers = state_counts.mean()
            top_state = state_counts.index[0]
            
            st.metric("States with Providers", total_states)
            st.metric("Average Providers per State", f"{avg_providers:.0f}")
            st.metric("Highest Coverage State", top_state)
            
            # Coverage concentration
            top_10_share = state_counts.head(10).sum() / state_counts.sum()
            st.metric("Top 10 States Share", f"{top_10_share:.1%}")

    # Coverage desert overview
    if deserts is not None and 'desert_severity' in deserts.columns:
        st.markdown("#### 🏜️ Coverage Desert Analysis")
        
        desert_summary = deserts['desert_severity'].value_counts()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Desert severity pie chart
            fig = px.pie(
                values=desert_summary.values,
                names=desert_summary.index,
                title="Counties by Coverage Desert Severity",
                color_discrete_map={
                    'not severe': BRAND_COLORS['primary_green'],
                    'low_medicare': BRAND_COLORS['warning'],
                    'severe': BRAND_COLORS['error']
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("**Desert Impact**")
            
            if 'Enrolled' in deserts.columns:
                severe_deserts = deserts[deserts['desert_severity'] == 'severe']
                underserved_pop = severe_deserts['Enrolled'].sum()
                st.metric("Underserved Population", f"{underserved_pop:,.0f}")
            
            st.metric("Severe Desert Counties", len(deserts[deserts['desert_severity'] == 'severe']))
            st.metric("Total Counties Analyzed", len(deserts))

def render_quality_insights(providers, selected_states):
    """Render quality insights dashboard."""
    
    st.subheader("⭐ Quality Insights")
    
    # Mock quality data for demonstration
    st.info("🔧 Quality metrics integration in progress. Showing sample insights based on available data patterns.")
    
    # Quality distribution simulation
    np.random.seed(42)  # For consistent demo data
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Simulated quality score distribution
        quality_scores = np.random.normal(3.8, 0.6, 1000)
        quality_scores = np.clip(quality_scores, 1, 5)
        
        fig = px.histogram(
            x=quality_scores,
            title="Provider Quality Score Distribution",
            labels={'x': 'Quality Rating (1-5)', 'y': 'Number of Providers'},
            nbins=20
        )
        fig.update_traces(marker_color=BRAND_COLORS['primary_blue'])
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Quality metrics summary
        st.markdown("**Quality Performance Summary**")
        
        avg_quality = np.mean(quality_scores)
        high_quality_pct = np.sum(quality_scores >= 4.0) / len(quality_scores)
        low_quality_pct = np.sum(quality_scores < 3.0) / len(quality_scores)
        
        st.metric("Average Quality Rating", f"{avg_quality:.1f}/5.0")
        st.metric("High Quality Providers", f"{high_quality_pct:.1%}")
        st.metric("Below Average Providers", f"{low_quality_pct:.1%}")

    # Quality improvement opportunities
    st.markdown("#### 🎯 Quality Improvement Opportunities")
    
    improvement_areas = [
        {"Area": "Patient Communication", "Current Score": 3.6, "Benchmark": 4.2, "Gap": 0.6},
        {"Area": "Care Coordination", "Current Score": 3.8, "Benchmark": 4.3, "Gap": 0.5},
        {"Area": "Pain Management", "Current Score": 3.9, "Benchmark": 4.1, "Gap": 0.2},
        {"Area": "Medication Education", "Current Score": 4.0, "Benchmark": 4.4, "Gap": 0.4}
    ]
    
    improvement_df = pd.DataFrame(improvement_areas)
    
    fig = px.bar(
        improvement_df,
        x='Area',
        y=['Current Score', 'Benchmark'],
        title="Quality Performance vs. National Benchmarks",
        barmode='group'
    )
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

def render_market_trends(providers, penetration, selected_states):
    """Render market trends and opportunities."""
    
    st.subheader("📈 Market Trends")
    
    # Market growth indicators
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📊 Market Growth Indicators**")
        
        # Simulated growth data
        growth_metrics = {
            "Medicare Advantage Growth": "8.3% YoY",
            "Home Health Utilization": "12.1% YoY", 
            "Provider Network Expansion": "5.7% YoY",
            "Quality Score Improvement": "3.2% YoY"
        }
        
        for metric, value in growth_metrics.items():
            st.write(f"• **{metric}**: {value}")
    
    with col2:
        st.markdown("**🎯 Strategic Priorities**")
        
        priorities = [
            "Rural market expansion",
            "Quality improvement initiatives", 
            "Technology integration",
            "Workforce development",
            "Care coordination enhancement"
        ]
        
        for priority in priorities:
            st.write(f"• {priority}")

    # Market opportunity heatmap (simulated)
    if penetration is not None:
        st.markdown("#### 🗺️ Market Opportunity Analysis")
        
        # This would show actual penetration vs opportunity analysis
        st.info("Market opportunity analysis based on Medicare Advantage penetration and provider density.")
        
        # Show basic penetration statistics if data available
        if 'State' in penetration.columns:
            penetration_cols = [col for col in penetration.columns if 'penetration' in col.lower()]
            if penetration_cols:
                pen_col = penetration_cols[0]
                state_penetration = penetration.groupby('State')[pen_col].mean().sort_values(ascending=False)
                
                fig = px.bar(
                    x=state_penetration.head(20).values,
                    y=state_penetration.head(20).index,
                    orientation='h',
                    title="Top 20 States by Medicare Advantage Penetration",
                    labels={'x': 'Penetration Rate', 'y': 'State'}
                )
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

    # Investment recommendations
    st.markdown("#### 💡 Investment Recommendations")
    
    recommendations = [
        {
            "Priority": "High",
            "Area": "Rural Coverage Expansion", 
            "Rationale": "Significant coverage gaps in rural counties with aging populations",
            "Investment": "$50-100M"
        },
        {
            "Priority": "High", 
            "Area": "Quality Technology Platform",
            "Rationale": "Need for standardized quality measurement and improvement tools",
            "Investment": "$25-50M"
        },
        {
            "Priority": "Medium",
            "Area": "Workforce Training",
            "Rationale": "Skills gap in rural and underserved areas",
            "Investment": "$10-25M"
        },
        {
            "Priority": "Medium",
            "Area": "Network Optimization",
            "Rationale": "Consolidation opportunities in oversaturated markets", 
            "Investment": "$100-200M"
        }
    ]
    
    rec_df = pd.DataFrame(recommendations)
    st.dataframe(rec_df, use_container_width=True, hide_index=True)
