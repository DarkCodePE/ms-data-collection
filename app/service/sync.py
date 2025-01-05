from sqlalchemy.orm import Session

from datetime import datetime
import logging

from app.config.settings import settings
from app.core.model.job_offer import JobOffer
from app.core.model.schemas import LinkedInJobCreate
from app.service.indeed_scraper import IndeedScraperSG
from app.service.job_crud import JobCRUD

logger = logging.getLogger(__name__)


class JobSyncService:
    def __init__(self):
        self.job_crud = JobCRUD()
        self.indeed_scraper = IndeedScraperSG()

    async def sync_job(
            self,
            db: Session,
            job_data: LinkedInJobCreate,
            prefer_kafka: bool = True
    ) -> JobOffer:
        """
        Sincroniza una oferta de LinkedIn con ms-job
        """
        try:
            # Verificar si ya existe
            existing_job = db.query(JobOffer).filter(
                JobOffer.source_url == job_data.source_url
            ).first()

            if existing_job:
                # Actualizar
                return await self.job_crud.update_job(
                    db,
                    existing_job.id,
                    job_data.dict(),
                    prefer_kafka=prefer_kafka
                )
            else:
                # Crear nuevo
                return await self.job_crud.create_job(
                    db,
                    job_data,
                    prefer_kafka=prefer_kafka
                )

        except Exception as e:
            logger.error(f"Error syncing job: {str(e)}")
            raise

    async def sync_indeed_jobs(self, db: Session, keywords: str, location: str):
        """
        Sincroniza trabajos obtenidos desde Indeed.
        """
        try:
            jobs = await self.indeed_scraper.scrape_jobs(keywords, location)
            synced_jobs = []

            for job_data in jobs:
                job_model = LinkedInJobCreate(**job_data)  # Reutiliza tu esquema existente
                synced_job = await self.job_crud.create_job(db, job_model)
                synced_jobs.append(synced_job)

            return synced_jobs

        except Exception as e:
            logger.error(f"Error sincronizando trabajos desde Indeed: {str(e)}")
            return []