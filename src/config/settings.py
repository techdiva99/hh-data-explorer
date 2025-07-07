#!/usr/bin/env python3
"""
Configuration settings for the Home Health Provider Network Explorer.
Handles feature flags and environment-specific configurations.
"""

import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class AIConfig:
    """AI/Chatbot configuration settings."""
    enabled: bool = False
    provider: str = "openai"  # openai, anthropic, gemini, azure
    api_key: str = ""
    model: str = "gpt-4"
    max_tokens: int = 2000
    temperature: float = 0.7
    enable_rag: bool = True
    enable_agents: bool = True
    vector_store: str = "chroma"  # chroma, pinecone, faiss
    
@dataclass
class AppConfig:
    """Main application configuration."""
    debug: bool = False
    environment: str = "development"  # development, staging, production
    ai: AIConfig = None
    
    def __post_init__(self):
        if self.ai is None:
            self.ai = AIConfig()

class ConfigManager:
    """Manages application configuration from environment variables and config files."""
    
    def __init__(self):
        self.config = self._load_config()
    
    def _load_config(self) -> AppConfig:
        """Load configuration from environment variables."""
        
        # AI Configuration
        ai_config = AIConfig(
            enabled=self._get_bool_env("AI_ENABLED", False),
            provider=os.getenv("AI_PROVIDER", "openai"),
            api_key=self._get_api_key_by_provider(os.getenv("AI_PROVIDER", "openai")),
            model=os.getenv("AI_MODEL", "gpt-4"),
            max_tokens=int(os.getenv("AI_MAX_TOKENS", "2000")),
            temperature=float(os.getenv("AI_TEMPERATURE", "0.7")),
            enable_rag=self._get_bool_env("AI_ENABLE_RAG", True),
            enable_agents=self._get_bool_env("AI_ENABLE_AGENTS", True),
            vector_store=os.getenv("AI_VECTOR_STORE", "chroma")
        )
        
        # Main App Configuration
        app_config = AppConfig(
            debug=self._get_bool_env("DEBUG", False),
            environment=os.getenv("ENVIRONMENT", "development"),
            ai=ai_config
        )
        
        return app_config
    
    def _get_bool_env(self, key: str, default: bool = False) -> bool:
        """Convert environment variable to boolean."""
        value = os.getenv(key, str(default)).lower()
        return value in ("true", "1", "yes", "on")
    
    def _get_api_key_by_provider(self, provider: str) -> str:
        """Get the appropriate API key based on the provider."""
        if provider == "openai":
            return os.getenv("OPENAI_API_KEY", "")
        elif provider == "anthropic":
            return os.getenv("ANTHROPIC_API_KEY", "")
        elif provider == "gemini" or provider == "google":
            return os.getenv("GOOGLE_API_KEY", "")
        elif provider == "azure":
            return os.getenv("AZURE_OPENAI_KEY", "")
        else:
            return os.getenv("OPENAI_API_KEY", "")  # Default fallback
    
    def is_ai_enabled(self) -> bool:
        """Check if AI features are enabled."""
        return (
            self.config.ai.enabled and 
            self.config.ai.api_key and 
            len(self.config.ai.api_key.strip()) > 0
        )
    
    def get_ai_config(self) -> AIConfig:
        """Get AI configuration."""
        return self.config.ai
    
    def get_app_config(self) -> AppConfig:
        """Get application configuration."""
        return self.config

# Global configuration instance
config_manager = ConfigManager()
