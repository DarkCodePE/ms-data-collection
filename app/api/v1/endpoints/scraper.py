from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session

from datetime import datetime
from typing import List
import logging

from app.core.datastore.database import get_db
#from app.config.database import get_db
from app.core.model.job_offer import JobOffer
from app.core.model.schemas import ScrapingRequest, LinkedInJobCreate, ScrapingStats
from app.service.scraper import LinkedInScraper
from app.service.sync import JobSyncService

from dotenv import load_dotenv
import os

load_dotenv()

# Verificar que la variable esté configurada
SGAI_API_KEY = os.getenv("SGAI_API_KEY")
if not SGAI_API_KEY:
    raise ValueError("SGAI_API_KEY no está configurada en el archivo .env")


# Función para verificar la API key
async def verify_api_key(
        sgai_api_key: str = Header(..., alias="X-SGAI-API-KEY")
):
    if sgai_api_key != SGAI_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )
    return sgai_api_key


router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/scrape")
async def scrape_and_sync_jobs(
        request: ScrapingRequest,
        db: Session = Depends(get_db)
):
    """
    Scrapea trabajos de LinkedIn y los sincroniza con la base de datos.
    """
    try:
        scraper = LinkedInScraper()
        sync_service = JobSyncService()

        # Scrapear trabajos
        linkedin_jobs = await scraper.scrape_jobs(
            request.keywords,
            request.location
        )

        logger.debug(f"Raw jobs data: {linkedin_jobs}")

        # Transformar los datos para que coincidan con LinkedInJobCreate
        transformed_jobs = []
        for job in linkedin_jobs:
            transformed_jobs.append({
                "title": job.get("job_title"),
                "company": job.get("company_name"),
                "description": job.get("job_description", ""),
                "requirements": job.get("requirements", []),
                "location": job.get("location", ""),
                "is_remote": False,  # Puedes ajustar según los datos
                "source_url": job.get("source_url", ""),
                "salary_range": job.get("salary_range", None),
                "job_type": job.get("job_type", "FULL_TIME"),
                "level": job.get("experience_level", "NOT_SPECIFIED")
            })

        # Sincronizar trabajos
        synced_jobs = []
        for linkedin_job in transformed_jobs:
            try:
                job_data = LinkedInJobCreate(**linkedin_job)
                synced_job = await sync_service.sync_job(db, job_data)
                synced_jobs.append(synced_job)
            except Exception as e:
                logger.error(f"Validation failed for job: {linkedin_job}, error: {str(e)}")

        return synced_jobs

    except Exception as e:
        logger.error(f"Error in scrape_and_sync_jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/indeed")
async def scrape_and_sync_indeed_jobs(
        request: ScrapingRequest,
        db: Session = Depends(get_db)
):
    """
    Scrapea trabajos de Indeed y los sincroniza con la base de datos.
    """
    try:
        sync_service = JobSyncService()
        jobs = await sync_service.sync_indeed_jobs(db, request.keywords, request.location)
        return {"jobs_synced": len(jobs), "jobs": jobs}
    except Exception as e:
        logger.error(f"Error al scrapear Indeed: {str(e)}")
        raise HTTPException(status_code=500, detail="Error scraping Indeed")
