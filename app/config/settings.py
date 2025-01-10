from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    # Configuración del servicio
    PROJECT_NAME: str = "LinkedIn Job Scraper"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"

    # ScrapeGraph
    SGAI_API_KEY: Optional[str] = None

    # Configuración de scraping
    SYSTEM_USER_ID: str = "LINKEDIN_SCRAPER"
    MAX_JOBS_PER_QUERY: int = 50
    DEFAULT_LOCATION: str = "España"

    # Configuración de la base de datos
    MS_JOB_API_URL: str = "http://localhost:8080/api/v1"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    # Database Configuration
    #DB_HOST: str = "mi_postgres"
    DB_PORT: str = "5432"
    DB_NAME: str = "jobs_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "123456"

    # MongoDB Configuration
    MONGO_URI: str = "mongodb://admin:password@mongodb:27017/scraper_db?authSource=admin"
    MONGO_DB: str = "scraper_db"
    MONGO_COLLECTION_NAME: str = "jobs"
    MONGO_USER: str = "admin"
    MONGO_PASSWORD: str = "password"
    MONGO_AUTH_DB: str = "admin"
    MONGO_HOST: str = "mongodb"
    MONGO_PORT: int = 27017

    # LangSmith Configuration
    LANGCHAIN_TRACING_V2: str = "true"
    LANGCHAIN_ENDPOINT: str = "https://api.smith.langchain.com"
    LANGCHAIN_API_KEY: str = "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"
    LANGCHAIN_PROJECT: str = "JOB-IA"

    # Configuración de Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "123456"

    class Config:
        case_sensitive = True
        env_file = ".env"


settings = Settings()
