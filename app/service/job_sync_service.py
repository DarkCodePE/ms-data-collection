from typing import List
from datetime import datetime
import logging

from jobspy import scrape_jobs

from app.core.datastore.repository.mongodb import MongoDBRepository
from app.core.event.kafka.producer import KafkaProducer
from app.core.model.schemas import RawJobData, JobSource
from app.service.etl import JobETLService
from app.service.job_spy_scraper import JobSpyScraper

logger = logging.getLogger(__name__)


class JobSyncService:
    def __init__(
            self,
            mongo_repository: MongoDBRepository,
            kafka_producer: KafkaProducer,
            proxies: List[str] = None
    ):
        self.mongo_repository = mongo_repository
        #print("Initializing ETL service...")
        self.etl_service = JobETLService(mongo_repository, kafka_producer)
        #print("ETL service initialized.")
        self.scraper = JobSpyScraper(
            mongo_repository=mongo_repository,
            etl_service=self.etl_service,
            proxies=proxies
        )

    async def start_sync(self):
        """Inicia el proceso de sincronización continua."""
        try:
            #print("Starting sync process...")
            await self.scraper.start_scraping()
        except Exception as e:
            logger.error(f"Error in sync process: {str(e)}")
            raise

    async def sync_jobs_manual(self, search_term: str, location: str = "Remote") -> List[str]:
        """
        Realiza una sincronización manual con términos de búsqueda específicos.
        """
        try:
            # Usamos la función scrape_jobs directamente de jobspy
            jobs_df = scrape_jobs(
                site_name=["indeed"],
                search_term=search_term,
                location=location,
                results_wanted=10
            )

            # Usamos el método a través de la instancia de scraper
            await self.scraper.process_scraped_jobs(jobs_df)
            return [job.get('JOB_URL', '') for _, job in jobs_df.iterrows()]

        except Exception as e:
            logger.error(f"Error in manual sync: {str(e)}")
            return []