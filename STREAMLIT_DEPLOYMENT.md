# 🚀 Streamlit Community Cloud Deployment Guide

## **Auto-Deploy Setup (One-Time)**

### **1. Visit Streamlit Community Cloud**
Go to: **https://share.streamlit.io/**

### **2. Sign In with GitHub**
- Click "Continue with GitHub"
- Authorize Streamlit to access your repositories

### **3. Deploy Your App**
- Click "New app"
- Select your repository: `techdiva99/hh-data-explorer`
- Set these deployment settings:
  ```
  Branch: main
  Main file path: app.py
  App URL: (choose a custom name like "hh-coverage-deserts")
  ```

### **4. Click "Deploy!"**
Streamlit will automatically:
- Install dependencies from `requirements.txt`
- Configure settings from `.streamlit/config.toml`
- Launch your app at a public URL

## **✅ Auto-Deployment Features**

Once set up, your app will **automatically redeploy** whenever you:
- Push commits to the `main` branch
- Update any files in your repository
- Modify dependencies in `requirements.txt`

## **📁 Required Files (Already in Repo)**

- ✅ `app.py` - Entry point for Streamlit
- ✅ `requirements.txt` - Python dependencies
- ✅ `.streamlit/config.toml` - App configuration
- ✅ `src/ui/streamlit_coverage_deserts.py` - Main app code
- ✅ `data/` - All your data files
- ✅ `assets/` - Logo and images

## **🔧 App Configuration**

Your app is configured with:
- **Wide layout** for better map display
- **Custom theming** with your brand colors
- **Automatic data loading** from CSV files
- **Responsive design** for all screen sizes

## **📊 Features Deployed**

Your live app will include:
- Interactive US coverage desert map
- Sidebar filters (severity, state, enrollment, etc.)
- HHA branch locations overlay
- Filtered data table
- FIPS code lookup
- Professional branding with logo

## **🌐 Access Your App**

After deployment, you'll get a URL like:
`https://your-app-name.streamlit.app`

Share this URL with anyone - no login required!

## **💡 Pro Tips**

1. **Custom Domain**: Add your own domain in Streamlit settings
2. **Analytics**: Enable usage analytics in your app settings
3. **Secrets**: Store API keys in Streamlit's secrets management
4. **Performance**: Large datasets auto-cache for faster loading

## **🔄 Update Process**

To update your app:
1. Make changes to your code locally
2. Commit: `git commit -m "Update message"`
3. Push: `git push origin main`
4. ✨ App automatically redeploys in ~2 minutes!

## **📞 Support**

- Streamlit Docs: https://docs.streamlit.io/
- Community Forum: https://discuss.streamlit.io/
- GitHub Issues: Create issues in your repo for tracking
