
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import os
import json

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

# Set Streamlit page config for wide layout and add logo to header
st.set_page_config(
    page_title='US Home Health Coverage Deserts',
    layout='wide',
    page_icon='🗺️'
)

# Path to logo (user should place logo file in this folder)
LOGO_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'assets', 'img', 'logo.png')
if os.path.exists(LOGO_PATH):
    st.markdown(f"""
        <div style='display: flex; align-items: center;'>
            <img src='file://{LOGO_PATH}' style='height:60px;margin-right:20px;'>
            <h1 style='color:{BRAND_COLORS['dark_blue']};margin-bottom:0;'>US Home Health Coverage Deserts & HHA Branches</h1>
        </div>
        <hr style='margin-top:0;margin-bottom:1.5em;border:1px solid {BRAND_COLORS['primary_blue']};'>
    """, unsafe_allow_html=True)
else:
    st.title('US Home Health Coverage Deserts & HHA Branches')

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data/processed')
RAW_DIR = os.path.join(PROJECT_ROOT, 'data/raw')
os.makedirs(RAW_DIR, exist_ok=True)
DESERTS_CSV = os.path.join(DATA_DIR, 'hh_coverage_deserts_severity.csv')
PROVIDERS_CSV = os.path.join(DATA_DIR, 'new_final_master_provider.csv')
GEOJSON_RAW_PATH = os.path.join(RAW_DIR, 'us_counties_fips.geojson')
GEOJSON_DEFAULT_PATH = os.path.join(DATA_DIR, 'us_counties_fips.geojson')

# Download US counties FIPS GeoJSON if not present in raw
GEOJSON_URL = 'https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json'
geojson_path = GEOJSON_RAW_PATH
if not os.path.exists(GEOJSON_RAW_PATH):
    try:
        st.info('Downloading US counties FIPS GeoJSON...')
        r = requests.get(GEOJSON_URL, timeout=10)
        r.raise_for_status()
        with open(GEOJSON_RAW_PATH, 'w') as f:
            json.dump(r.json(), f)
    except Exception as e:
        st.warning(f'Could not download GeoJSON: {e}. Using default if available.')
        geojson_path = GEOJSON_DEFAULT_PATH
        if not os.path.exists(geojson_path):
            st.error('No GeoJSON file available. Please download manually.')
            st.stop()

# Load data
st.title('US Home Health Coverage Deserts & HHA Branches')
deserts = pd.read_csv(DESERTS_CSV, dtype={'FIPS': str})
providers = pd.read_csv(PROVIDERS_CSV, dtype=str)

# Ensure lat/lon columns exist and are numeric
lat_col = None
lon_col = None
for c in providers.columns:
    if c.lower() in ['lat', 'latitude']:
        lat_col = c
    if c.lower() in ['lon', 'lng', 'longitude']:
        lon_col = c
if lat_col and lon_col:
    providers[lat_col] = pd.to_numeric(providers[lat_col], errors='coerce')
    providers[lon_col] = pd.to_numeric(providers[lon_col], errors='coerce')
    branches = providers[providers['PRACTICE LOCATION TYPE'].str.upper() == 'HHA BRANCH']
else:
    st.warning('Latitude/Longitude columns not found in provider file.')
    branches = pd.DataFrame()

# Load GeoJSON
with open(geojson_path) as f:
    geojson = json.load(f)


# Custom color mapping for severity

# Custom color mapping for severity using brand colors
# Make 'severe' much bolder/darker (use dark red or dark blue for max contrast)
severity_color_map = {
    'not severe': BRAND_COLORS['primary_green'],      # green
    'low_medicare': BRAND_COLORS['accent_blue'],      # light blue
    'severe': '#B71C1C',  # dark, bold red for severe
}
# Ensure desert_severity is categorical for color mapping and fill missing
deserts['desert_severity'] = deserts['desert_severity'].fillna('not severe')
deserts['desert_severity'] = pd.Categorical(deserts['desert_severity'], categories=['not severe', 'low_medicare', 'severe'])
deserts['severity_color'] = deserts['desert_severity'].map(severity_color_map).fillna(BRAND_COLORS['primary_green'])

st.markdown('''\
- Hover over a county to see summary info.\
- Red dots show HHA Branches.\
- For more interactivity (click/select), use sidebar filters below.''')


# --- Sidebar Filters ---
st.sidebar.header('Filter Map Data')

# Severity filter
severity_options = [cat for cat in ['not severe', 'low_medicare', 'severe'] if cat in deserts['desert_severity'].cat.categories]
severity_selected = st.sidebar.multiselect('Desert Severity', severity_options, default=severity_options)

# State filter (if state column exists)
state_col = None
for c in deserts.columns:
    if c.lower() in ['state', 'state_abbr', 'state_code']:
        state_col = c
        break
if state_col:
    state_options = sorted(deserts[state_col].dropna().unique())
    state_selected = st.sidebar.multiselect('State', state_options, default=state_options)
else:
    state_selected = None

# Enrollment strata filter
enroll_col = None
for c in deserts.columns:
    if 'enroll' in c.lower() and 'strata' in c.lower():
        enroll_col = c
        break
if enroll_col:
    enroll_options = sorted(deserts[enroll_col].dropna().unique())
    enroll_selected = st.sidebar.multiselect('Enrollment Strata', enroll_options, default=enroll_options)
else:
    enroll_selected = None

# Penetration strata filter
pen_col = None
for c in deserts.columns:
    if 'penetration' in c.lower() and 'strata' in c.lower():
        pen_col = c
        break
if pen_col:
    pen_options = sorted(deserts[pen_col].dropna().unique())
    pen_selected = st.sidebar.multiselect('Penetration Strata', pen_options, default=pen_options)
else:
    pen_selected = None

# Provider count strata filter
prov_col = None
for c in deserts.columns:
    if 'provider' in c.lower() and 'strata' in c.lower():
        prov_col = c
        break
if prov_col:
    prov_options = sorted(deserts[prov_col].dropna().unique())
    prov_selected = st.sidebar.multiselect('Provider Count Strata', prov_options, default=prov_options)
else:
    prov_selected = None

# Apply filters to deserts DataFrame
filtered_deserts = deserts[deserts['desert_severity'].isin(severity_selected)]
if state_selected is not None and state_col:
    filtered_deserts = filtered_deserts[filtered_deserts[state_col].isin(state_selected)]
if enroll_selected is not None and enroll_col:
    filtered_deserts = filtered_deserts[filtered_deserts[enroll_col].isin(enroll_selected)]
if pen_selected is not None and pen_col:
    filtered_deserts = filtered_deserts[filtered_deserts[pen_col].isin(pen_selected)]
if prov_selected is not None and prov_col:
    filtered_deserts = filtered_deserts[filtered_deserts[prov_col].isin(prov_selected)]

# --- FIPS lookup ---
fips_selected = st.sidebar.text_input('Enter FIPS code for details:', '')
if fips_selected:
    fips_selected = fips_selected.zfill(5)
    fips_info = deserts[deserts['FIPS'] == fips_selected]
    if not fips_info.empty:
        st.subheader(f'Details for FIPS {fips_selected}')
        st.write(fips_info.T.astype(str))
        # Providers in this FIPS
        provs = providers[providers['FIPS'] == fips_selected] if 'FIPS' in providers.columns else pd.DataFrame()
        if not provs.empty:
            st.write('Providers in this FIPS:')
            st.dataframe(provs.astype(str))
        else:
            # Closest provider info if available
            st.write('No providers in this FIPS. Closest provider info:')
            st.write(fips_info[['closest_provider_distance']].astype(str))
    else:
        st.warning('FIPS code not found in coverage desert file.')

# --- Map rendering and filtered data table (after filters are defined) ---
try:
    fig = px.choropleth_map(
        filtered_deserts,
        geojson=geojson,
        locations='FIPS',
        color='desert_severity',
        color_discrete_map=severity_color_map,
        category_orders={'desert_severity': ['not severe', 'low_medicare', 'severe']},
        hover_data=['FIPS', 'severity_reason', 'Enrolled', 'provider_count', 'closest_provider_distance'],
        map_style='carto-positron',  # more blocky, clear state/county boundaries
        zoom=3, center={'lat': 37.8, 'lon': -96},
        opacity=0.8,
        featureidkey='properties.GEOID'
    )
    # County lines: thin, brand accent blue
    fig.update_traces(marker_line_width=0.3, marker_line_color=BRAND_COLORS['accent_blue'])
except Exception as e:
    st.error('Plotly choropleth_map is not available or failed to render. Please upgrade plotly to >=5.20.0.\nError: {}'.format(e))
    st.stop()

# Add HHA Branches as brand blue dots
if not branches.empty:
    fig.add_scattermapbox(
        lat=branches[lat_col],
        lon=branches[lon_col],
        mode='markers',
        marker=dict(size=7, color=BRAND_COLORS['primary_blue'], opacity=0.85),
        text=branches['ORGANIZATION NAME'],
        hoverinfo='text',
        name='HHA Branches'
    )

# Add state lines as brand dark blue (using mapbox layers)
fig.update_layout(
    mapbox_layers=[
        dict(
            sourcetype='vector',
            source='mapbox://mapbox.boundaries-adm1-v4',
            type='line',
            color=BRAND_COLORS['dark_blue'],
            line=dict(width=4),  # thicker state lines
            below='traces',
            opacity=0.95  # more visible
        )
    ]
)

st.plotly_chart(fig, use_container_width=True)

# --- Show filtered data table below map ---
with st.expander('Show Filtered Data Table'):
    st.dataframe(filtered_deserts.astype(str), use_container_width=True)
