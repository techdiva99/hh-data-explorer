#!/bin/bash
# Deploy Home Health Provider Network Explorer - No AI Version
# Use this script for environments without AI/LLM connectivity

echo "🚀 Deploying Home Health Explorer (No AI)..."

# Set environment variables for no AI
export AI_ENABLED=false
export ENVIRONMENT=production
export DEBUG=false

# Install base requirements only (no AI dependencies)
echo "📦 Installing base requirements..."
pip install -r requirements.txt

# Start the application
echo "🌟 Starting application without AI features..."
streamlit run app.py --server.headless true --server.port ${PORT:-8501}
