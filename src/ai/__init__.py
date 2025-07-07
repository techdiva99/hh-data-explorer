"""
AI module initialization with feature flag support.
Only imports AI dependencies when AI is enabled.
"""

from src.config import config_manager

# Initialize AI components only if enabled
ai_enabled = config_manager.is_ai_enabled()

if ai_enabled:
    try:
        # Import AI components when enabled
        from .chatbot.interface import render_ai_chat_tab
        from .utils.llm_client import LLMClient
        
        # Initialize components
        llm_client = LLMClient()
        
        def get_ai_components():
            """Get AI components if available."""
            return {
                'llm_client': llm_client,
                'chat_interface': render_ai_chat_tab,
                'enabled': True
            }
            
    except ImportError as e:
        print(f"Warning: AI dependencies not available: {e}")
        ai_enabled = False
        
        def get_ai_components():
            """Return disabled AI components."""
            return {
                'llm_client': None,
                'chat_interface': None,
                'enabled': False,
                'error': str(e)
            }
else:
    def get_ai_components():
        """Return disabled AI components."""
        return {
            'llm_client': None,
            'chat_interface': None,
            'enabled': False,
            'reason': 'AI features disabled in configuration'
        }

# Export what's available
__all__ = ['get_ai_components', 'ai_enabled']
