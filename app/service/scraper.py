from langchain_scrapegraph.tools import SmartScraperTool
from dotenv import load_dotenv
import os
import logging
from typing import List, Optional

from app.config.settings import settings

logger = logging.getLogger(__name__)

# Cargar variables de entorno al inicio de la aplicación
load_dotenv()

# Verificar configuración de LangSmith
required_env_vars = [
    "LANGCHAIN_API_KEY",
    "LANGCHAIN_PROJECT",
    "SGAI_API_KEY"
]

for var in required_env_vars:
    if not os.getenv(var):
        raise ValueError(f"{var} no está configurada en el archivo .env")


class LinkedInScraper:
    def __init__(self):
        self.scraper = SmartScraperTool()
        self.base_url = "https://www.linkedin.com/jobs/search"

    async def scrape_jobs(
            self,
            keywords: List[str],
            location: Optional[str] = None
    ) -> List[dict]:
        """Scrapea ofertas de trabajo de LinkedIn"""
        jobs = []
        location = location or settings.DEFAULT_LOCATION

        for keyword in keywords:
            try:
                search_url = f"{self.base_url}?keywords={keyword}&location={location}"

                # Prompt para extracción de datos
                prompt = """
                Extract from each job listing:
                - Job title
                - Company name
                - Job description
                - Requirements (as list)
                - Location
                - Job type (FULL_TIME, PART_TIME, CONTRACT)
                - Experience level
                - Salary range (if available)
                Format as JSON array with these fields.
                """

                result = await self.scraper.ainvoke({
                    "user_prompt": prompt,
                    "website_url": search_url
                })
                logger.info(f"Scraping result: {result}")
                # Supongamos que 'result' es un dict con la clave 'job_listings'
                if isinstance(result, dict) and "job_listings" in result:
                    # result["job_listings"] es una lista de dicts
                    # Toma hasta MAX_JOBS_PER_QUERY
                    job_listings = result["job_listings"][:settings.MAX_JOBS_PER_QUERY]
                    jobs.extend(job_listings)

            except Exception as e:
                logger.error(f"Error scraping jobs for {keyword}: {str(e)}")
                continue

        return jobs
