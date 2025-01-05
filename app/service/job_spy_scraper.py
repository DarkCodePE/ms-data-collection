from datetime import datetime, date, timezone, time
import asyncio
import logging
from typing import List, Optional
from jobspy import scrape_jobs
import pandas as pd

from app.core.datastore.repository.mongodb import MongoDBRepository
from app.core.model.schemas import RawJobData, JobSource

from app.service.etl import JobETLService
from app.core.exceptions import ScraperException
import logging


class JobSpyScraper:
    """Servicio unificado de scraping usando JobSpy."""

    def __init__(
            self,
            mongo_repository: MongoDBRepository,
            etl_service: JobETLService,
            scraping_interval: int = 3600,  # 1 hora por defecto
            proxies: List[str] = None,
            results_wanted: int = 50
    ):
        self.mongo_repository = mongo_repository
        self.etl_service = etl_service
        self.scraping_interval = scraping_interval
        self.proxies = proxies
        self.results_wanted = results_wanted

        self.default_search_terms = [
            "python developer",
            "software engineer",
            "data scientist",
            "frontend developer",
            "backend developer",
            "devops engineer"
        ]

    async def start_scraping(self):
        """Inicia el proceso de scraping continuo."""
        while True:
            try:
                await self.run_scraping_cycle()
                # Procesar trabajos pendientes con el ETL
                print("Processing ETL jobs...")
                await self.etl_service.process_pending_jobs()
                print("ETL jobs processed.")
                await asyncio.sleep(self.scraping_interval)
            except Exception as e:
                logging.error(f"Error in scraping cycle: {str(e)}")
                await asyncio.sleep(60)

    async def run_scraping_cycle(self):
        """Ejecuta un ciclo completo de scraping para todos los términos de búsqueda."""
        for search_term in self.default_search_terms:
            try:
                logging.info(f"Starting scraping for term: {search_term}")
                jobs_df = scrape_jobs(
                    site_name=["linkedin", "indeed", "glassdoor"],
                    search_term=search_term,
                    location="Remote",
                    results_wanted=self.results_wanted,
                    hours_old=72,  # Últimas 72 horas
                    proxies=self.proxies,
                    description_format="markdown",
                    enforce_annual_salary=True,
                    verbose=1
                )
                logging.info(f"Scraping completed. Found {len(jobs_df)} jobs.")
                logging.debug(f"Scraping results: {jobs_df}")
                await self.process_scraped_jobs(jobs_df)

            except Exception as e:
                logging.error(f"Error scraping term '{search_term}': {str(e)}")
                continue

    def _prepare_raw_data(self, job_series: pd.Series) -> dict:
        """Prepara los datos crudos para MongoDB convirtiendo fechas a formato ISO."""
        raw_data = {}
        for key, value in job_series.items():
            if isinstance(value, pd.Timestamp):
                raw_data[key] = value.isoformat()
            elif isinstance(value, date):  # Usar date en lugar de datetime.date
                raw_data[key] = datetime.combine(value, time.min).isoformat()
            else:
                raw_data[key] = value
        return raw_data

    async def process_scraped_jobs(self, jobs_df: pd.DataFrame):
        """Procesa los trabajos scrapeados y los guarda en MongoDB."""
        if jobs_df.empty:
            logging.warning("No jobs found in this scraping cycle")
            return

        # Verificar conexión a MongoDB
        if not await self.mongo_repository.verify_connection():
            #print("MongoDB connection is not available")
            logging.error("MongoDB connection is not available")
            return

        #print(f"Processing {len(jobs_df)} jobs.")
        for _, job in jobs_df.iterrows():
            try:
                # Helper function para manejar valores nan/None
                def clean_field(value):
                    if pd.isna(value) or value is None:
                        return None
                    return str(value)

                # Extraer y limpiar location
                location = job.get('location', '')
                if pd.isna(location):
                    location = None

                # Formatear el nivel del trabajo
                job_level = None if pd.isna(job.get('job_level')) else str(job.get('job_level'))

                # Mapear la fuente
                source = self._map_source(job['site'])
                #print(f"source: {source}")
                # Preparar los datos crudos
                raw_data = self._prepare_raw_data(job)
                #print(f"raw_data: {raw_data}")
                # Crear objeto RawJobData con el campo raw_data procesado
                raw_job = RawJobData(
                    source=source,
                    job_id=str(job.get('job_url', '')),
                    title=job.get('TITLE', ''),
                    company=job.get('COMPANY', ''),
                    description=job.get('DESCRIPTION', ''),
                    location=f"{job.get('CITY', '')} {job.get('STATE', '')}".strip(),
                    url=job.get('JOB_URL', ''),
                    salary_range=self._format_salary(job),
                    requirements=self._extract_requirements(job),
                    job_type=job.get('JOB_TYPE', ''),
                    experience_level=job_level,
                    raw_data=raw_data,  # Usar los datos procesados
                    processed=False,
                    created_at=datetime.now(timezone.utc)
                )
                #print(f"raw_job: {raw_job}")
                await self.mongo_repository.save_raw_job(raw_job)

            except Exception as e:
                print(f"Error processing job: {str(e)}")
                logging.error(f"Error processing job: {str(e)}", exc_info=True)
                continue

    def _map_source(self, site_name: str) -> JobSource:
        """Mapea el nombre del sitio a nuestro enum JobSource."""
        mapping = {
            'linkedin': JobSource.LINKEDIN,
            'indeed': JobSource.INDEED,
            'glassdoor': JobSource.GLASSDOOR,
            'google': JobSource.GOOGLE
        }
        site_name_lower = site_name.lower() if site_name else 'other'
        if site_name_lower not in mapping:
            logging.warning(f"Unrecognized site: {site_name}. Defaulting to OTHER.")
        return mapping.get(site_name_lower, JobSource.OTHER)

    def _format_salary(self, job: pd.Series) -> Optional[str]:
        """Formatea la información de salario para trabajos de Indeed y otras fuentes."""
        try:
            min_amount = job.get('min_amount')
            max_amount = job.get('max_amount')
            interval = job.get('interval')
            currency = job.get('currency', 'USD')

            # Si todos los valores son nan, retornar None
            if all(pd.isna(x) for x in [min_amount, max_amount]):
                return None

            # Convertir valores nan a None
            min_amount = None if pd.isna(min_amount) else float(min_amount)
            max_amount = None if pd.isna(max_amount) else float(max_amount)
            interval = None if pd.isna(interval) else str(interval)

            # Si tenemos rango completo
            if min_amount is not None and max_amount is not None:
                return f"{currency} {min_amount:,.2f} - {max_amount:,.2f} {interval or 'per year'}"
            # Si solo tenemos mínimo
            elif min_amount is not None:
                return f"{currency} {min_amount:,.2f}+ {interval or 'per year'}"
            # Si solo tenemos máximo
            elif max_amount is not None:
                return f"Up to {currency} {max_amount:,.2f} {interval or 'per year'}"

            return None

        except Exception as e:
            logging.error(f"Error formatting salary: {str(e)}")
            return None

    def _extract_requirements(self, job: pd.Series) -> List[str]:
        """Extrae requisitos del trabajo basados en la descripción."""
        # Aquí podrías implementar lógica más sofisticada de NLP
        # Por ahora retornamos una lista vacía
        return []