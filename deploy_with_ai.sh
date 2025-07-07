#!/bin/bash
# Deploy Home Health Provider Network Explorer - Full AI Version
# Use this script for environments with AI/LLM connectivity

echo "🤖 Deploying Home Health Explorer (With AI)..."

# Check for required API key
if [ -z "$OPENAI_API_KEY" ]; then
    echo "❌ Error: OPENAI_API_KEY environment variable not set"
    echo "Please set your API key: export OPENAI_API_KEY=your_key_here"
    exit 1
fi

# Set environment variables for AI enabled
export AI_ENABLED=true
export ENVIRONMENT=production
export DEBUG=false
export AI_PROVIDER=openai
export AI_MODEL=gpt-4

# Install AI requirements
echo "📦 Installing AI requirements..."
pip install langchain>=0.1.0
pip install langchain-community>=0.0.20
pip install langchain-openai>=0.0.5
pip install chromadb>=0.4.0
pip install tiktoken>=0.5.0
pip install sentence-transformers>=2.2.0

# Install base requirements
pip install -r requirements.txt

# Start the application
echo "🌟 Starting application with AI features..."
streamlit run app.py --server.headless true --server.port ${PORT:-8501}
