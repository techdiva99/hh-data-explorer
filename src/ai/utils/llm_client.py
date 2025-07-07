"""
LLM Client wrapper for different AI providers.
Supports OpenAI, Anthropic, Google Gemini, and Azure.
"""

from typing import Optional, Dict, Any
from src.config import config_manager

class LLMClient:
    """Generic LLM client that adapts to different providers."""
    
    def __init__(self):
        self.config = config_manager.get_ai_config()
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the appropriate LLM client based on configuration."""
        try:
            if self.config.provider == "openai":
                self._init_openai()
            elif self.config.provider == "anthropic":
                self._init_anthropic()
            elif self.config.provider in ["gemini", "google"]:
                self._init_gemini()
            elif self.config.provider == "azure":
                self._init_azure()
            else:
                raise ValueError(f"Unsupported AI provider: {self.config.provider}")
                
        except ImportError as e:
            raise ImportError(f"Required AI libraries not installed for {self.config.provider}: {e}")
    
    def _init_openai(self):
        """Initialize OpenAI client."""
        from langchain_openai import ChatOpenAI
        self.client = ChatOpenAI(
            api_key=self.config.api_key,
            model=self.config.model,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
    
    def _init_anthropic(self):
        """Initialize Anthropic Claude client."""
        from langchain_anthropic import ChatAnthropic
        self.client = ChatAnthropic(
            api_key=self.config.api_key,
            model=self.config.model,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
    
    def _init_gemini(self):
        """Initialize Google Gemini client."""
        from langchain_google_genai import ChatGoogleGenerativeAI
        self.client = ChatGoogleGenerativeAI(
            google_api_key=self.config.api_key,
            model=self.config.model,
            temperature=self.config.temperature,
            max_output_tokens=self.config.max_tokens
        )
    
    def _init_azure(self):
        """Initialize Azure OpenAI client."""
        from langchain_openai import AzureChatOpenAI
        self.client = AzureChatOpenAI(
            api_key=self.config.api_key,
            model=self.config.model,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens
        )
    
    def generate_response(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Generate a response from the LLM."""
        try:
            if system_prompt:
                from langchain_core.messages import SystemMessage, HumanMessage
                messages = [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=prompt)
                ]
                response = self.client.invoke(messages)
            else:
                from langchain_core.messages import HumanMessage
                messages = [HumanMessage(content=prompt)]
                response = self.client.invoke(messages)
            
            return response.content
            
        except Exception as e:
            return f"Error generating response: {str(e)}"
    
    def is_available(self) -> bool:
        """Check if the LLM client is properly initialized."""
        return self.client is not None
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about the current provider."""
        return {
            "provider": self.config.provider,
            "model": self.config.model,
            "available": self.is_available(),
            "api_key_configured": bool(self.config.api_key)
        }
