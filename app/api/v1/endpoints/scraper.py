from fastapi import APIRouter, Depends, HTTPException, Header, BackgroundTasks, Request
from jobspy import scrape_jobs

from sqlalchemy.orm import Session

from datetime import datetime
from typing import List
import logging

from starlette.responses import JSONResponse

from app.config.settings import settings
from app.core.datastore.database import get_db
from app.core.datastore.repository.mongodb import MongoDBRepository
from app.core.event.kafka.producer import KafkaProducer
from app.core.exceptions import ScraperException
#from app.config.database import get_db
from app.core.model.job_offer import JobOffer
from app.core.model.schemas import ScrapingRequest, LinkedInJobCreate, ScrapingStats, JobSource
from app.service.etl import JobETLService
from app.service.job_spy_scraper import JobSpyScraper
from app.service.scheduler import ScrapingScheduler
from app.service.scraper import LinkedInScraper
from app.service.sync import JobSyncService

from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

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


@router.post("/scrape")
async def scrape_and_sync_jobs(request: ScrapingRequest, app_request: Request):
    logging.info("Starting scrape_and_sync_jobs endpoint")

    mongo_repo = MongoDBRepository(settings.MONGO_URI, settings.MONGO_DB_NAME)

    try:
        # Validar y configurar el país
        country = request.country.lower() if request.country else "peru"

        kafka_producer = app_request.app.state.kafka_producer

        # Realizar scraping con JobSpy
        logging.info("Starting JobSpy scraping...")
        # jobs = scrape_jobs(
        #     site_name=["linkedin"],
        #     search_term="software engineer",
        #     location="San Francisco, CA",
        #     results_wanted=20,
        #     hours_old=72,
        #     country_indeed='USA',
        # )
        jobs_df = scrape_jobs(
            site_name=["linkedin", "indeed", "google"],
            search_term=",".join(request.keywords),
            location=country,  # Configurar el país aquí
            results_wanted=10,
            hours_old=72,
            country_indeed='peru',
        )
        #print(f"jobs_dfxxxx: {jobs_df}")
        # Verificar si jobs es un DataFrame y tiene datos
        if jobs_df is None:
            logging.error("JobSpy returned None instead of a DataFrame")
            raise ValueError("No jobs data returned from scraping")

        logging.info(f"JobSpy scraping completed. DataFrame shape: {jobs_df.shape}")
        logging.info(f"DataFrame columns: {jobs_df.columns.tolist()}")

        # Mostrar una muestra de los datos
        if not jobs_df.empty:
            logging.info("Sample of first job data:")
            sample_job = jobs_df.iloc[0].to_dict()
            logging.info(f"First job: {sample_job}")

        # Inicializar servicios
        job_spy_scraper = JobSpyScraper(
            mongo_repository=mongo_repo,
            etl_service=JobETLService(mongo_repo, kafka_producer),
            proxies=None,
            results_wanted=50
        )

        # Procesar trabajos
        logging.info("Starting job processing...")
        await job_spy_scraper.process_scraped_jobs(jobs_df)
        logging.info("Job processing completed")

        return JSONResponse(
            content={"message": f"Scraping and synchronization completed successfully. Found {len(jobs_df)} jobs."},
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error in scrape_and_sync_jobs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
