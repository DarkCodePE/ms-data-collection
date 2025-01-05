from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class LinkedInJobCreate(BaseModel):
    title: str = Field(..., min_length=1)  # Requerido
    company: str  # Requerido
    description: str  # Requerido
    requirements: List[str]  # Requerido (lista)
    location: str  # Requerido
    is_remote: bool = False
    source_url: str  # Requerido
    salary_range: Optional[str] = None
    job_type: str = "FULL_TIME"
    level: str = "NOT_SPECIFIED"


class ScrapingRequest(BaseModel):
    keywords: List[str]
    country: Optional[str] = None


class ScrapingStats(BaseModel):
    total_jobs_scraped: int
    jobs_scraped_today: int
    last_sync: datetime


class IndeedJobData(BaseModel):
    title: str
    company: str
    location: str
    summary: str
    url: str = ""
    posted_at: datetime = datetime.utcnow()


class JobSource(str, Enum):
    LINKEDIN = "linkedin"
    INDEED = "indeed"
    GLASSDOOR = "glassdoor"
    GOOGLE = "google"
    OTHER = "other"


class RawJobData(BaseModel):
    """Modelo para almacenar los datos crudos de trabajos en MongoDB."""
    source: JobSource
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    title: str
    company: str
    description: str
    location: str
    url: Optional[str] = None
    salary_range: Optional[str] = None
    requirements: Optional[List[str]] = []
    job_type: Optional[str] = None
    experience_level: Optional[str] = None
    raw_data: dict  # Datos originales sin procesar
    processed: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


class ProcessedJobData(BaseModel):
    """Modelo para los datos procesados que serán enviados a ms-job."""
    source_job_id: str
    title: str
    company: str
    description: str
    requirements: List[str]
    location: str
    is_remote: bool = False
    source_url: str
    salary_range: Optional[str] = None
    job_type: str = "FULL_TIME"
    level: str = "NOT_SPECIFIED"
    source: JobSource
