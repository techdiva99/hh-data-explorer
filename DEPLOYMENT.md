# 🗺️ Home Health Coverage Deserts Explorer

A Streamlit web application for visualizing US Home Health coverage deserts and provider networks.

## 🚀 Live Demo

**Deploy on Streamlit Community Cloud (Free & Easy):**

1. **Push to GitHub** (if not already done)
2. **Visit** [share.streamlit.io](https://share.streamlit.io)
3. **Connect your GitHub** account
4. **Deploy this repository** with:
   - **Repository**: `your-username/hh-data-explorer`
   - **Branch**: `main` 
   - **Main file path**: `app.py`

## 📋 Alternative Deployment Options

### Option 1: Streamlit Community Cloud (Recommended)
- ✅ **Free hosting**
- ✅ **Automatic updates** from GitHub
- ✅ **Custom domain** support
- ✅ **Easy setup**

### Option 2: Heroku
```bash
# Install Heroku CLI, then:
heroku create your-app-name
git push heroku main
```

### Option 3: Railway
```bash
# Connect your GitHub repo at railway.app
# Automatic deployment on push
```

### Option 4: Render
```bash
# Connect your GitHub repo at render.com
# Free tier available
```

### Option 5: Local Development
```bash
# Clone and run locally
git clone https://github.com/your-username/hh-data-explorer.git
cd hh-data-explorer
pip install -r requirements.txt
streamlit run app.py
```

## 📁 File Structure for Deployment

```
hh-data-explorer/
├── app.py                 # Main entry point (required for deployment)
├── requirements.txt       # Python dependencies
├── .streamlit/
│   └── config.toml       # Streamlit configuration
├── src/
│   └── ui/
│       └── streamlit_coverage_deserts.py  # Main app logic
├── data/                 # Data files
├── assets/               # Images, logos
└── README.md            # This file
```

## 🔧 Why GitHub Pages Won't Work

GitHub Pages only serves **static files** (HTML/CSS/JS) and cannot run:
- Python servers
- Streamlit applications  
- Backend processing
- Real-time data filtering

**Solution**: Use Streamlit Community Cloud instead - it's specifically designed for Streamlit apps!

## 📊 Features

- 🗺️ **Interactive choropleth map** of US counties
- 🔍 **Advanced filtering** by severity, state, enrollment, etc.
- 📈 **Real-time data table** updates
- 🎨 **Custom brand colors** and professional styling
- 📱 **Responsive design** for all devices

## 🛠️ Technical Stack

- **Frontend**: Streamlit
- **Mapping**: Plotly + GeoJSON
- **Data**: Pandas + CSV processing
- **Deployment**: Streamlit Community Cloud

## 📝 Next Steps

1. **Push your code** to GitHub
2. **Visit** [share.streamlit.io](https://share.streamlit.io)  
3. **Deploy with one click** using `app.py`
4. **Share your live app URL** with stakeholders!

---

**Note**: The `app.py` file in the root directory is specifically created for deployment platforms that expect the main application file in the root folder.
