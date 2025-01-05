from typing import List

from sqlalchemy.orm import Session

from datetime import datetime
import logging

from app.config.settings import settings
from app.core.datastore.repository.mongodb import MongoDBRepository
from app.core.event.kafka.producer import KafkaProducer
from app.core.model.job_offer import JobOffer
from app.core.model.schemas import LinkedInJobCreate, RawJobData, JobSource
from app.service.etl import JobETLService
from app.service.indeed_scraper import IndeedScraperSG
from app.service.job_crud import JobCRUD
from app.service.job_spy_scraper import JobSpyScraper

logger = logging.getLogger(__name__)


class JobSyncService:
    def __init__(
            self,
            mongo_repository: MongoDBRepository,
            kafka_producer: KafkaProducer,
            proxies: List[str] = None,
            scraping_interval: int = 3600,  # 1 hora por defecto
            results_wanted: int = 50
    ):
        self.mongo_repository = mongo_repository
        self.etl_service = JobETLService(mongo_repository, kafka_producer)
        # Inicializar el scraper
        self.scraper = JobSpyScraper(
            mongo_repository=mongo_repository,
            etl_service=self.etl_service,
            scraping_interval=scraping_interval,
            proxies=proxies,
            results_wanted=results_wanted
        )

    async def sync_job(
            self,
            job_data: dict,
            source: JobSource,
    ) -> bool:
        """
        Sincroniza un trabajo individual guardándolo en MongoDB
        y procesándolo con el ETL.
        """
        try:
            # Crear RawJobData
            raw_job = RawJobData(
                source=source,
                title=job_data.get("title"),
                company=job_data.get("company"),
                description=job_data.get("description"),
                location=job_data.get("location"),
                url=job_data.get("url"),
                salary_range=job_data.get("salary_range"),
                requirements=job_data.get("requirements", []),
                job_type=job_data.get("job_type"),
                experience_level=job_data.get("experience_level"),
                raw_data=job_data,
                processed=False
            )

            # Guardar en MongoDB
            await self.mongo_repository.save_raw_job(raw_job)

            # Procesar inmediatamente con ETL
            processed_job = self.etl_service.transform_job_data(raw_job)

            # Crear evento Kafka
            event = {
                "type": "JOB_CREATED",
                "data": processed_job.dict(),
                "metadata": {
                    "source": source,
                    "processed_at": datetime.utcnow().isoformat(),
                    "raw_job_id": raw_job.job_id
                }
            }

            # Enviar a Kafka
            await self.etl_service.kafka_producer.send_event("job-events", event)

            # Marcar como procesado
            await self.mongo_repository.mark_job_as_processed(raw_job.job_id)

            return True

        except Exception as e:
            logger.error(f"Error syncing job: {str(e)}")
            raise

    async def sync_jobs_batch(
            self,
            jobs_data: List[dict],
            source: JobSource
    ) -> List[str]:
        """
        Sincroniza un lote de trabajos.
        Retorna la lista de IDs de los trabajos guardados.
        """
        synced_job_ids = []

        for job_data in jobs_data:
            try:
                await self.sync_job(job_data, source)
                synced_job_ids.append(job_data.get("url"))
            except Exception as e:
                logger.error(f"Error syncing job {job_data.get('url')}: {str(e)}")
                continue

        return synced_job_ids

    # async def sync_indeed_jobs(self, db: Session, keywords: str, location: str):
    #     """
    #     Sincroniza trabajos obtenidos desde Indeed.
    #     """
    #     try:
    #         jobs = await self.indeed_scraper.scrape_jobs(keywords, location)
    #         synced_jobs = []
    #
    #         for job_data in jobs:
    #             job_model = LinkedInJobCreate(**job_data)  # Reutiliza tu esquema existente
    #             synced_job = await self.job_crud.create_job(db, job_model)
    #             synced_jobs.append(synced_job)
    #
    #         return synced_jobs
    #
    #     except Exception as e:
    #         logger.error(f"Error sincronizando trabajos desde Indeed: {str(e)}")
    #         return []
