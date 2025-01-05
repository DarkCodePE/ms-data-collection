from datetime import datetime, timedelta
import asyncio
import logging
from typing import List

from app.core.datastore.repository.mongodb import MongoDBRepository
from app.core.model.schemas import JobSource, RawJobData

from app.service.etl import JobETLService
from app.service.indeed_scraper import IndeedScraperSG
from app.service.scraper import LinkedInScraper

logger = logging.getLogger(__name__)


class ScrapingScheduler:
    """Servicio para programar y coordinar el scraping de múltiples fuentes."""

    def __init__(
            self,
            mongo_repository: MongoDBRepository,
            etl_service: JobETLService,
            scraping_interval: int = 3600  # 1 hora por defecto
    ):
        self.mongo_repository = mongo_repository
        self.etl_service = etl_service
        self.scraping_interval = scraping_interval

        # Inicializar scrapers
        self.scrapers = {
            JobSource.LINKEDIN: LinkedInScraper(),
            JobSource.INDEED: IndeedScraperSG(),
            #JobSource.GLASSDOOR: GlassdoorScraper(),
        }

        self.default_keywords = [
            "python developer",
            "software engineer",
            "data scientist",
            "frontend developer",
            "backend developer",
            "devops engineer"
        ]

    async def start_scheduling(self):
        """Inicia el proceso de scheduling."""
        while True:
            try:
                # Ejecutar scraping para cada fuente
                for source in JobSource:
                    await self.run_scraping_for_source(source)

                # Procesar trabajos pendientes con el ETL
                await self.etl_service.process_pending_jobs()

                # Esperar hasta el próximo ciclo
                await asyncio.sleep(self.scraping_interval)

            except Exception as e:
                logger.error(f"Error in scraping cycle: {str(e)}")
                await asyncio.sleep(60)  # Esperar un minuto antes de reintentar

    async def run_scraping_for_source(self, source: JobSource):
        """Ejecuta el scraping para una fuente específica."""
        try:
            scraper = self.scrapers.get(source)
            if not scraper:
                logger.warning(f"No scraper found for source {source}")
                return

            logger.info(f"Starting scraping for {source}")

            for keyword in self.default_keywords:
                try:
                    jobs = await scraper.scrape_jobs(
                        keywords=[keyword],
                        location="Remote"
                    )

                    # Guardar cada trabajo en MongoDB
                    for job in jobs:
                        await self.mongo_repository.save_raw_job(
                            RawJobData(
                                source=source,
                                title=job.get("title"),
                                company=job.get("company"),
                                description=job.get("description"),
                                location=job.get("location"),
                                url=job.get("url"),
                                salary_range=job.get("salary_range"),
                                requirements=job.get("requirements", []),
                                job_type=job.get("job_type"),
                                experience_level=job.get("experience_level"),
                                raw_data=job,  # Guardamos el objeto completo como datos crudos
                                processed=False
                            )
                        )

                    logger.info(
                        f"Successfully scraped and saved {len(jobs)} jobs for keyword '{keyword}' from {source}")

                except Exception as e:
                    logger.error(f"Error scraping jobs for keyword '{keyword}' from {source}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in run_scraping_for_source for {source}: {str(e)}")