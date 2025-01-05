import httpx
from datetime import datetime
from typing import List, Dict
import logging

from app.core.model.schemas import IndeedJobData  # Importa el modelo
from langchain_scrapegraph.tools import SmartScraperTool  # Asegúrate de tener SmartScraperTool instalado

logger = logging.getLogger(__name__)


class IndeedScraperSG:
    """
    Scraper para Indeed que utiliza ScrapeGraph (SmartScraperTool) para extraer
    datos estructurados del HTML usando prompts.
    """

    def __init__(self):
        self.smart_scraper = SmartScraperTool()

    async def scrape_jobs(self, keywords: str, location: str) -> List[Dict]:
        """
        Scrapea ofertas de trabajo desde Indeed.
        """
        try:
            base_url = "https://pe.indeed.com/jobs"
            params = {
                "q": keywords,
                "l": location,
                "sort": "date",
            }

            async with httpx.AsyncClient() as client:
                response = await client.get(base_url, params=params)
                response.raise_for_status()
                page_html = response.text

            user_prompt = """
            Extract a list of job postings from the HTML. 
            For each job posting, return the following keys in JSON:
              1) title
              2) company
              3) location
              4) summary
              5) posted_at
              6) url
            """
            tool_input = {
                "user_prompt": user_prompt,
                "website_html": page_html,
            }

            result = self.smart_scraper.invoke(tool_input)

            job_list = []
            if isinstance(result, dict) and "items" in result:
                for item in result["items"]:
                    job_model = IndeedJobData(
                        title=item.get("title", ""),
                        company=item.get("company", ""),
                        location=item.get("location", ""),
                        summary=item.get("summary", ""),
                        url=item.get("url", ""),
                        posted_at=datetime.utcnow()
                    )
                    job_list.append(job_model.dict())
            else:
                logger.warning("La respuesta de SmartScraperTool no tiene el formato esperado")

            return job_list

        except Exception as e:
            logger.error(f"Error en IndeedScraperSG: {str(e)}")
            return []
