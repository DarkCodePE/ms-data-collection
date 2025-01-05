from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class LinkedInJobCreate(BaseModel):
    title: str = Field(..., min_length=1)  # Requerido
    company: str                           # Requerido
    description: str                       # Requerido
    requirements: List[str]                # Requerido (lista)
    location: str                          # Requerido
    is_remote: bool = False
    source_url: str                        # Requerido
    salary_range: Optional[str] = None
    job_type: str = "FULL_TIME"
    level: str = "NOT_SPECIFIED"


class ScrapingRequest(BaseModel):
    keywords: List[str]
    location: Optional[str] = None


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
