import streamlit as st
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

def render_about_tab():
    """Render the About tab content."""
    
    st.markdown('''
    ### About the Home Health Finder & Market Explorer
    
    This comprehensive platform provides deep insights into the US home health industry through 
    interactive analysis of coverage patterns, quality metrics, provider networks, and market opportunities.
    ''')

    # Create subtabs for About sections
    about_tabs = st.tabs([
        "Project Overview", 
        "Data Sources", 
        "Data Catalog", 
        "Technical Docs", 
        "Contact"
    ])

    with about_tabs[0]:
        render_project_overview()
    
    with about_tabs[1]:
        render_data_sources()
    
    with about_tabs[2]:
        render_data_catalog()
    
    with about_tabs[3]:
        render_technical_documentation()
    
    with about_tabs[4]:
        render_contact_information()

def render_project_overview():
    """Render project overview section."""
    
    st.subheader("Project Overview")
    
    st.markdown(f'''
    <div style="background-color: {BRAND_COLORS['accent_blue']}; padding: 20px; border-radius: 10px; margin: 20px 0;">
        <h4 style="color: {BRAND_COLORS['dark_blue']}; margin-top: 0;">Mission Statement</h4>
        <p style="color: {BRAND_COLORS['dark_blue']}; margin-bottom: 0;">
            To provide comprehensive analysis and visualization tools for understanding home health service 
            accessibility, quality, and market dynamics across the United States, enabling data-driven 
            decision making for healthcare providers, policymakers, researchers, and consumers.
        </p>
    </div>
    ''', unsafe_allow_html=True)

    # Target audiences section
    st.markdown("#### 👥 Who Can Use This Tool?")
    
    audiences = [
        {
            "audience": "🏠 Consumers & Families",
            "description": "Find and compare home health providers in your area",
            "use_cases": [
                "Search for nearby home health agencies",
                "Compare provider quality ratings (HHCAHPS scores)",
                "View patient satisfaction scores and reviews",
                "Find contact information and get directions"
            ],
            "primary_tabs": ["Find a Provider", "Quality Metrics"]
        },
        {
            "audience": "Healthcare Organizations", 
            "description": "Strategic planning and competitive intelligence",
            "use_cases": [
                "Market opportunity assessment and expansion planning",
                "Competitive analysis and benchmarking",
                "Network optimization and partnership opportunities",
                "Quality performance comparison with peers"
            ],
            "primary_tabs": ["Market Analysis", "Provider Networks", "Dashboard"]
        },
        {
            "audience": "Policymakers & Regulators",
            "description": "Healthcare access and policy development",
            "use_cases": [
                "Identify coverage gaps and healthcare deserts",
                "Monitor quality performance across regions",
                "Evaluate Medicare Advantage market penetration",
                "Support resource allocation decisions"
            ],
            "primary_tabs": ["Coverage Deserts", "Quality Metrics", "Dashboard"]
        },
        {
            "audience": "🔬 Researchers & Analysts",
            "description": "Academic research and data analysis",
            "use_cases": [
                "Study geographic patterns in healthcare access",
                "Analyze relationships between quality and market factors",
                "Research provider network structures and consolidation",
                "Examine healthcare disparities and equity"
            ],
            "primary_tabs": ["All tabs", "Technical Documentation"]
        },
        {
            "audience": "💼 Healthcare Consultants",
            "description": "Client advisory and strategic consulting",
            "use_cases": [
                "Develop market entry strategies for clients",
                "Perform due diligence for acquisitions",
                "Benchmark client performance against markets",
                "Identify partnership and network opportunities"
            ],
            "primary_tabs": ["Market Analysis", "Provider Networks", "Quality Metrics"]
        }
    ]

    for aud in audiences:
        with st.expander(f"{aud['audience']} - {aud['description']}"):
            st.markdown("**How to use this tool:**")
            for use_case in aud['use_cases']:
                st.write(f"• {use_case}")
            st.markdown(f"**Recommended tabs:** {', '.join(aud['primary_tabs'])}")

    # How to use the tool
    st.markdown("#### 📖 How to Use This Tool")
    
    usage_col1, usage_col2 = st.columns(2)
    
    with usage_col1:
        st.markdown(f'''
        <div style="background-color: {BRAND_COLORS['accent_green']}; padding: 15px; border-radius: 8px;">
            <h5 style="color: {BRAND_COLORS['dark_green']}; margin-top: 0;">Getting Started (Consumers)</h5>
            <ol style="color: {BRAND_COLORS['dark_green']};">
                <li><strong>Start with "Find a Provider"</strong> - Enter your location to find nearby agencies</li>
                <li><strong>Review quality ratings</strong> - Compare HHCAHPS scores and patient satisfaction</li>
                <li><strong>Check coverage areas</strong> - Use "Coverage Deserts" to understand service availability</li>
                <li><strong>Get contact info</strong> - Call providers directly or get directions</li>
            </ol>
        </div>
        ''', unsafe_allow_html=True)
    
    with usage_col2:
        st.markdown(f'''
        <div style="background-color: {BRAND_COLORS['light_gray']}; padding: 15px; border-radius: 8px;">
            <h5 style="color: {BRAND_COLORS['dark_blue']}; margin-top: 0;">Getting Started (Business Users)</h5>
            <ol style="color: {BRAND_COLORS['dark_blue']};">
                <li><strong>Review the Dashboard</strong> - Get overview of key market insights</li>
                <li><strong>Analyze market opportunities</strong> - Use "Market Analysis" for penetration data</li>
                <li><strong>Study provider networks</strong> - Understand competitive landscape</li>
                <li><strong>Benchmark quality</strong> - Compare performance metrics</li>
            </ol>
        </div>
        ''', unsafe_allow_html=True)

    # Tool effectiveness
    st.markdown("#### Is This a Good Tool?")
    
    effectiveness_col1, effectiveness_col2 = st.columns(2)
    
    with effectiveness_col1:
        st.markdown("**Strengths**")
        strengths = [
            "**Comprehensive data integration** - Multiple authoritative sources (CMS, NBER, HUD)",
            "**Multi-audience design** - Serves consumers, businesses, and researchers",
            "**Interactive visualizations** - Maps, charts, and filtering capabilities", 
            "**Real-time analysis** - Dynamic filtering and exploration",
            "**Geographic focus** - County-level granularity across the US",
            "**Quality emphasis** - Patient satisfaction and performance metrics"
        ]
        for strength in strengths:
            st.markdown(f"• {strength}")
    
    with effectiveness_col2:
        st.markdown("**Areas for Enhancement**")
        improvements = [
            "**Real-time data updates** - Currently uses quarterly/annual data",
            "**Provider contact integration** - Direct booking and communication features",
            "**Mobile optimization** - Enhanced mobile-friendly interface",
            "**Predictive analytics** - Forecasting market trends and opportunities",
            "**User personalization** - Saved searches and custom dashboards",
            "**API access** - Programmatic access for developers"
        ]
        for improvement in improvements:
            st.markdown(f"• {improvement}")

    # Value proposition
    st.markdown(f'''
    <div style="background-color: {BRAND_COLORS['primary_blue']}; color: white; padding: 20px; border-radius: 10px; margin: 20px 0;">
        <h4 style="margin-top: 0; color: white;">💡 Why This Tool Matters</h4>
        <p style="margin-bottom: 0;">
            Home health services are critical for aging populations and patients with chronic conditions. 
            This tool democratizes access to complex healthcare data, enabling better decisions for 
            individuals seeking care and organizations planning services. By combining multiple data 
            sources into an intuitive interface, we bridge the gap between raw government data and 
            actionable insights.
        </p>
    </div>
    ''', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Key Objectives")
        st.markdown("""
        - **Identify Coverage Deserts**: Map areas with limited home health access
        - **Analyze Quality Metrics**: Evaluate provider performance using CMS data
        - **Network Analysis**: Understand organizational structures and relationships
        - **Market Intelligence**: Assess penetration rates and business opportunities
        - **Geographic Insights**: Visualize patterns across counties, states, and regions
        """)

    with col2:
        st.markdown("#### Key Features")
        st.markdown("""
        - **Interactive Maps**: County-level visualization with FIPS lookup
        - **Quality Dashboards**: HHCAHPS scores and performance comparisons
        - **Provider Networks**: Organizational analysis and geographic distribution
        - **Market Analysis**: Penetration rates, cost data, and opportunity assessment
        - **Real-time Filtering**: Dynamic data exploration across all modules
        """)

    st.markdown("#### Analysis Capabilities")
    
    capabilities = [
        {
            "title": "Coverage Desert Analysis",
            "description": "Identifies counties with insufficient home health provider coverage based on Medicare enrollment and provider density",
            "data_sources": "Provider enrollment, geographic data, Medicare penetration"
        },
        {
            "title": "Quality Performance Tracking", 
            "description": "Analyzes HHCAHPS patient satisfaction scores and CMS quality measures across providers and states",
            "data_sources": "CMS quality data, HHCAHPS surveys, provider characteristics"
        },
        {
            "title": "Network Mapping",
            "description": "Maps organizational relationships and geographic presence of home health networks",
            "data_sources": "Provider enrollment data, organizational hierarchies"
        },
        {
            "title": "Market Opportunity Assessment",
            "description": "Combines penetration rates, demographics, and provider data to identify expansion opportunities",
            "data_sources": "Medicare Advantage penetration, cost reports, enrollment data"
        }
    ]

    for cap in capabilities:
        with st.expander(f"🔹 {cap['title']}"):
            st.write(f"**Description:** {cap['description']}")
            st.write(f"**Data Sources:** {cap['data_sources']}")

def render_data_sources():
    """Render data sources section based on README content."""
    
    st.subheader("Data Sources")
    
    st.markdown("""
    This platform integrates data from multiple authoritative sources to provide comprehensive 
    home health industry analysis.
    """)

    # Primary data sources from README
    data_sources = [
        {
            "category": "Geographic Data",
            "sources": [
                {
                    "name": "NBER CBSA-FIPS Crosswalk",
                    "url": "https://www.nber.org/research/data/census-core-based-statistical-area-cbsa-federal-information-processing-series-fips-county-crosswalk",
                    "description": "Core Based Statistical Area to FIPS county mapping"
                },
                {
                    "name": "HUD Datasets",
                    "url": "https://www.huduser.gov/portal/pdrdatas_landing.html",
                    "description": "Housing and urban development geographic data"
                },
                {
                    "name": "SimpleMaps US ZIP Codes",
                    "url": "https://simplemaps.com/data/us-zips",
                    "description": "Comprehensive ZIP code geographic information"
                }
            ]
        },
        {
            "category": "Home Health Provider Data",
            "sources": [
                {
                    "name": "CMS Quality Data",
                    "url": "https://data.cms.gov/provider-data/search?theme=Home%20health%20services",
                    "description": "Comprehensive home health quality metrics and performance data"
                },
                {
                    "name": "Home Health Agency Enrollments",
                    "url": "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/home-health-agency-enrollments",
                    "description": "Provider enrollment and characteristics data"
                },
                {
                    "name": "HHA Cost Reports",
                    "url": "https://data.cms.gov/provider-compliance/cost-report/home-health-agency-cost-report",
                    "description": "Financial and operational cost data (2022 data available)"
                }
            ]
        },
        {
            "category": "Market & Penetration Data",
            "sources": [
                {
                    "name": "Medicare Advantage Penetration",
                    "url": "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-state/county-penetration/ma-state/county-penetration-2025-06",
                    "description": "State and county-level Medicare Advantage market penetration rates"
                },
                {
                    "name": "CMS Program Statistics",
                    "url": "https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-service-type-reports/cms-program-statistics-medicare-home-health-agency",
                    "description": "Medicare home health agency program statistics (2021 data)"
                }
            ]
        }
    ]

    for category_data in data_sources:
        st.markdown(f"### {category_data['category']}")
        
        for source in category_data['sources']:
            with st.expander(f"📖 {source['name']}"):
                st.write(f"**Description:** {source['description']}")
                st.write(f"**URL:** [{source['name']}]({source['url']})")
                
                # Add data quality indicators
                if "2022" in source['description']:
                    st.warning("Contains 2022 data - some records may have gaps")
                elif "2021" in source['description']:
                    st.info("Contains 2021 data")
                else:
                    st.success("Current/recent data")

    st.markdown("#### 🔄 Data Integration Process")
    st.markdown("""
    1. **Master Provider Directory**: Created comprehensive provider database with quality metrics
    2. **Geographic Enrichment**: Added CBSA and county information to all provider records
    3. **Coverage Analysis**: Analyzed ZIP codes and counties for Medicare enrollment vs provider presence
    4. **Desert Identification**: Mapped home health coverage deserts and nearest high-quality providers
    5. **Quality Integration**: Merged provider characteristics with CMS quality performance data
    """)

def render_data_catalog():
    """Render data catalog from datamap.json."""
    
    st.subheader("📁 Data Catalog")
    
    # Load datamap.json
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    datamap_path = os.path.join(PROJECT_ROOT, 'data', 'datamap.json')
    
    try:
        with open(datamap_path, 'r') as f:
            datamap = json.load(f)
        
        st.markdown("""
        This catalog shows the current data files available in the system and their purposes.
        All file paths are relative to the project root directory.
        """)

        # Organize data by category
        data_categories = {
            "Quality Metrics": [
                "hhcahps_provider", "hhcahps_state", "hhcahps_national", "hh_provider", "hh_zip"
            ],
            "Geographic Data": [
                "zip_county", "zip_cbsa", "simplemaps_zip_geo", "cbsa_fips"
            ],
            "Market Data": [
                "state_county_penetration", "hh_enrollment"
            ]
        }

        for category, keys in data_categories.items():
            st.markdown(f"### {category}")
            
            category_files = []
            for key in keys:
                if key in datamap:
                    filepath = datamap[key]
                    full_path = os.path.join(PROJECT_ROOT, filepath)
                    
                    # Check if file exists and get info
                    exists = os.path.exists(full_path)
                    file_size = ""
                    if exists:
                        try:
                            size_bytes = os.path.getsize(full_path)
                            if size_bytes > 1024*1024:
                                file_size = f"{size_bytes/(1024*1024):.1f} MB"
                            else:
                                file_size = f"{size_bytes/1024:.1f} KB"
                        except:
                            file_size = "Unknown"
                    
                    category_files.append({
                        "Dataset": key.replace('_', ' ').title(),
                        "File Path": filepath,
                        "Status": "Available" if exists else "Missing",
                        "Size": file_size if exists else "N/A"
                    })
            
            if category_files:
                st.dataframe(category_files, use_container_width=True, hide_index=True)
        
        # Additional processed files
        st.markdown("### Processed/Derived Data")
        processed_dir = os.path.join(PROJECT_ROOT, 'data', 'processed')
        if os.path.exists(processed_dir):
            processed_files = []
            for filename in os.listdir(processed_dir):
                if filename.endswith('.csv'):
                    filepath = os.path.join(processed_dir, filename)
                    try:
                        size_bytes = os.path.getsize(filepath)
                        if size_bytes > 1024*1024:
                            file_size = f"{size_bytes/(1024*1024):.1f} MB"
                        else:
                            file_size = f"{size_bytes/1024:.1f} KB"
                    except:
                        file_size = "Unknown"
                    
                    processed_files.append({
                        "File Name": filename,
                        "Type": "Processed Dataset",
                        "Size": file_size
                    })
            
            if processed_files:
                st.dataframe(processed_files[:10], use_container_width=True, hide_index=True)
                if len(processed_files) > 10:
                    st.info(f"Showing 10 of {len(processed_files)} processed files")

    except FileNotFoundError:
        st.error("Data map file (datamap.json) not found.")
    except json.JSONDecodeError:
        st.error("Error reading data map file.")

def render_technical_documentation():
    """Render technical documentation."""
    
    st.subheader("🛠️ Technical Documentation")
    
    # Architecture overview
    st.markdown("#### 🏗️ System Architecture")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Frontend Framework**
        - Streamlit for interactive web interface
        - Plotly for data visualization
        - Custom CSS for branding and styling
        
        **Data Processing**
        - Pandas for data manipulation
        - GeoPy for geocoding operations
        - NumPy for numerical computations
        """)
    
    with col2:
        st.markdown("""
        **Application Structure**
        - Modular tab-based architecture
        - Separate modules for each analysis type
        - Shared utilities and styling components
        
        **Data Pipeline**
        - Automated data loading and validation
        - Geographic enrichment processes
        - Quality metric integration
        """)

    # File structure
    st.markdown("#### 📂 Project Structure")
    
    project_structure = """
    ```
    hh-data-explorer/
    ├── app.py                    # Main application entry point
    ├── requirements.txt          # Python dependencies
    ├── data/
    │   ├── datamap.json         # Data file catalog
    │   ├── cms_hh_quality/      # CMS quality datasets
    │   ├── geo/                 # Geographic reference data
    │   ├── hh_cost/            # Cost report data
    │   ├── hh_enrollment/      # Enrollment data
    │   ├── market_potential/   # Market analysis data
    │   └── processed/          # Derived/processed datasets
    ├── src/
    │   ├── data/               # Data processing scripts
    │   └── ui/                 # User interface modules
    │       ├── coverage_deserts_tab.py
    │       ├── provider_networks_tab.py
    │       ├── quality_metrics_tab.py
    │       ├── market_analysis_tab.py
    │       └── about_tab.py
    └── assets/
        └── img/                # Images and logos
    ```
    """
    
    st.code(project_structure, language='text')

    # Development information
    st.markdown("#### 🚀 Development & Deployment")
    
    deployment_info = [
        {
            "Component": "Web Framework",
            "Technology": "Streamlit",
            "Version": "≥1.28.0",
            "Purpose": "Interactive web application framework"
        },
        {
            "Component": "Visualization",
            "Technology": "Plotly",
            "Version": "≥5.20.0",
            "Purpose": "Interactive charts and maps"
        },
        {
            "Component": "Data Processing",
            "Technology": "Pandas",
            "Version": "≥1.5.0",
            "Purpose": "Data manipulation and analysis"
        },
        {
            "Component": "Geocoding",
            "Technology": "GeoPy",
            "Version": "≥2.3.0",
            "Purpose": "Address geocoding and geographic calculations"
        }
    ]
    
    st.dataframe(deployment_info, use_container_width=True, hide_index=True)

    # Usage instructions
    st.markdown("#### 📖 Usage Instructions")
    
    with st.expander("Running the Application Locally"):
        st.code("""
# 1. Clone the repository
git clone <repository-url>
cd hh-data-explorer

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the application
streamlit run app.py
        """, language='bash')
    
    with st.expander("Data Processing Workflow"):
        st.markdown("""
        1. **Data Acquisition**: Download datasets from CMS and other sources
        2. **Geographic Enrichment**: Add CBSA, county, and coordinate information
        3. **Quality Integration**: Merge provider data with quality metrics
        4. **Coverage Analysis**: Identify service gaps and coverage deserts
        5. **Network Analysis**: Map organizational relationships and hierarchies
        6. **Visualization**: Generate interactive maps and analytical dashboards
        """)

def render_contact_information():
    """Render contact and support information."""
    
    st.subheader("📞 Contact Information")
    
    # Contact details
    st.markdown(f'''
    <div style="background-color: {BRAND_COLORS['accent_green']}; padding: 20px; border-radius: 10px; margin: 20px 0;">
        <h4 style="color: {BRAND_COLORS['dark_green']}; margin-top: 0;">Get in Touch</h4>
        <p style="color: {BRAND_COLORS['dark_green']}; font-size: 18px; margin-bottom: 0;">
            📧 <strong>Email:</strong> info@techdiva.io
        </p>
    </div>
    ''', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🤝 Support & Collaboration")
        st.markdown("""
        - **Technical Support**: Data processing and analysis questions
        - **Partnership Opportunities**: Healthcare organizations and researchers
        - **Custom Analysis**: Tailored insights for specific regions or use cases
        - **Data Integration**: Adding new data sources or metrics
        """)

    with col2:
        st.markdown("#### 🎯 Use Cases")
        st.markdown("""
        - **Healthcare Systems**: Market analysis and expansion planning
        - **Policy Makers**: Coverage gap identification and resource allocation
        - **Researchers**: Academic studies on healthcare accessibility
        - **Consultants**: Client-specific market intelligence
        """)

    # Version and build information
    st.markdown("#### ℹ️ Application Information")
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    app_info = [
        {"Property": "Application Name", "Value": "Home Health Data Explorer"},
        {"Property": "Version", "Value": "2.0.0"},
        {"Property": "Build Date", "Value": current_date},
        {"Property": "Framework", "Value": "Streamlit"},
        {"Property": "Data Sources", "Value": "CMS, NBER, HUD, SimpleMaps"},
        {"Property": "Geographic Coverage", "Value": "United States (County-level)"}
    ]
    
    st.dataframe(app_info, use_container_width=True, hide_index=True)

    # Acknowledgments
    st.markdown("#### 🙏 Acknowledgments")
    st.markdown("""
    This application was built using data from the Centers for Medicare & Medicaid Services (CMS), 
    the National Bureau of Economic Research (NBER), the Department of Housing and Urban Development (HUD), 
    and other authoritative sources. We acknowledge the efforts of these organizations in making 
    healthcare data publicly available for research and analysis.
    """)
    
    st.markdown("---")
    st.markdown(f"*Last updated: {current_date}*")
