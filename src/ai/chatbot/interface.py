"""
AI Chat Interface for the Home Health Provider Network Explorer.
This module provides the chat UI when AI features are enabled.
"""

import streamlit as st
from typing import Optional
from src.config import config_manager

def render_ai_chat_tab():
    """Render the AI Assistant chat interface."""
    
    st.markdown("## 🤖 AI Assistant")
    st.markdown("*Intelligent insights for home health data exploration*")
    
    # Check if AI is properly configured
    if not config_manager.is_ai_enabled():
        st.warning(f"""
        ⚠️ **AI Assistant Unavailable**
        
        The AI Assistant is currently disabled or not properly configured.
        
        **To enable AI features:**
        1. Set `AI_ENABLED=true` in your environment
        2. Provide a valid API key (e.g., `GOOGLE_API_KEY`)
        3. Restart the application
        
        **Current Configuration:**
        - AI Enabled: {config_manager.config.ai.enabled}
        - Provider: {config_manager.config.ai.provider}
        - API Key Configured: {"✅ Yes" if config_manager.config.ai.api_key else "❌ No"}
        """)
        return
    
    # AI is enabled - show chat interface
    st.success("🎉 **AI Assistant Ready!**")
    
    # Show current configuration
    with st.expander("🔧 AI Configuration", expanded=False):
        st.info(f"""
        **Provider:** {config_manager.config.ai.provider}  
        **Model:** {config_manager.config.ai.model}  
        **Temperature:** {config_manager.config.ai.temperature}  
        **Max Tokens:** {config_manager.config.ai.max_tokens}
        """)
    
    # Chat history in session state
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    # Display chat history
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.write(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask me about home health providers, quality metrics, or market insights..."):
        # Add user message to history
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Generate AI response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = generate_ai_response(prompt)
                st.write(response)
                
        # Add AI response to history
        st.session_state.chat_history.append({"role": "assistant", "content": response})

def generate_ai_response(prompt: str) -> str:
    """Generate AI response using the configured LLM."""
    try:
        from src.ai.utils.llm_client import LLMClient
        
        llm_client = LLMClient()
        if not llm_client.is_available():
            return get_fallback_response(prompt)
        
        system_prompt = """You are an AI assistant specializing in home health care data analysis. 
        You help users understand provider networks, quality metrics, coverage areas, 
        and market trends in the home health industry.
        
        Provide helpful, accurate responses about:
        - Home health provider information
        - Quality ratings and metrics
        - Geographic coverage and accessibility
        - Market analysis and trends
        - Regulatory and compliance topics
        
        Keep responses professional, informative, and actionable.
        If you don't have specific data to reference, provide general guidance and suggest what data would be helpful."""
        
        response = llm_client.generate_response(prompt, system_prompt)
        
        provider_info = llm_client.get_provider_info()
        footer = f"\n\n*Powered by {provider_info['provider'].title()} ({provider_info['model']})*"
        
        return response + footer
        
    except Exception as e:
        return get_fallback_response(prompt, error=str(e))

def get_fallback_response(prompt: str, error: Optional[str] = None) -> str:
    """Fallback response when AI is not available."""
    error_msg = f"\n\n*Error: {error}*" if error else ""
    
    return f"""🤖 **AI Assistant (Demo Mode)**

You asked: "{prompt}"

The AI assistant is being set up. Currently showing demo responses.

**When fully operational, I'll provide:**
- Intelligent analysis of your home health data
- Provider recommendations based on your criteria  
- Quality insights and market trends
- Data visualizations and actionable insights

*The AI Agent with RAG capabilities is currently under development.*{error_msg}
"""
