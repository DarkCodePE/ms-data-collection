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
            results_wanted: int = 1000
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
                #print("Processing ETL jobs...")
                await self.etl_service.process_pending_jobs()
                #print("ETL jobs processed.")
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
                    site_name=["indeed"],
                    search_term=search_term,
                    location="Remote",
                    results_wanted=self.results_wanted,
                    hours_old=120,  # Últimas 72 horas
                    proxies=self.proxies,
                    description_format="markdown",
                    enforce_annual_salary=False,
                    verbose=2,
                )
                print(f"Scraping completed. Found {len(jobs_df)} jobs.")
                print(f"Scraping results: {jobs_df}")
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
            ##print("MongoDB connection is not available")
            logging.error("MongoDB connection is not available")
            return

        ##print(f"Processing {len(jobs_df)} jobs.")
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
                ##print(f"source: {source}")
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
                ##print(f"raw_job: {raw_job}")
                await self.mongo_repository.save_raw_job(raw_job)

            except Exception as e:
                #print(f"Error processing job: {str(e)}")
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

    def extract_country_from_location(self, location: str) -> str:
        """
        Extrae el país de una cadena de ubicación, devolviendo únicamente el código del país o una cadena vacía si no se encuentra.
        """
        try:
            if not location or not isinstance(location, str):
                return ""

            # Normalizar la ubicación (convertir a minúsculas y eliminar espacios extras)
            location = location.strip().lower()

            # Lista de identificadores del país Perú
            peru_identifiers = ["pe", "peru", "perú"]

            # Verificar si algún identificador de Perú está en la ubicación
            for identifier in peru_identifiers:
                if identifier in location:
                    return "PE"  # Código estándar para Perú

            # Si no se encuentra un identificador, devolver cadena vacía
            return ""
        except Exception as e:
            logging.error(f"Error al extraer el país de la ubicación: {str(e)}")
            return ""

    def _format_salary(self, job: pd.Series) -> Optional[str]:
        """
        Formatea la información de salario para trabajos, ajustando valores predeterminados si hay datos faltantes.
        """
        try:
            print(f"job: {job}")
            # Extraer datos directamente del trabajo
            min_amount = job.get("min_amount")
            max_amount = job.get("max_amount")
            interval = job.get("interval")
            currency = job.get("currency")
            location = job.get("location", "").replace(",", "").strip().lower()
            # Extraer el país usando el nuevo algoritmo
            country = self.extract_country_from_location(location).lower()
            print(f"country: {country}")
            # Reemplazar NaN o None con valores predeterminados
            min_amount = float(min_amount) if pd.notna(min_amount) else None
            max_amount = float(max_amount) if pd.notna(max_amount) else None
            interval = str(interval).lower() if pd.notna(interval) else None
            currency = currency if pd.notna(currency) else None
            print(f"location: {country}")
            # Detectar ubicación y ajustar moneda e intervalo
            if "peru" in country or "pe" in country:  # Detectar trabajos en PerúPerú
                print("Trabajo en Perú detectado.")
                if currency is None or currency == "USD":  # Cambiar a Soles si no está definido o es USD
                    print("Cambiando moneda a PEN (Soles) para trabajos en Perú.")
                    currency = "PEN"  # Cambiar moneda a Soles
                if interval == "yearly":  # Convertir salario anual a mensual
                    print("Convirtiendo salario anual a mensual (PEN).")
                    if min_amount:
                        min_amount /= 12
                    if max_amount:
                        max_amount /= 12
                    interval = "monthly"

            # Asignar valores genéricos si falta información de salario
            if not min_amount and not max_amount:
                min_amount, max_amount = 1000.0, 3000.0  # Valores predeterminados

            # Formatear el intervalo
            interval_str = {
                "monthly": "mensual",
                "yearly": "anual",
                "weekly": "semanal",
                "daily": "diario",
                "hourly": "por hora",
            }.get(interval, "")

            # Construir la cadena de salario
            if min_amount and max_amount:
                return f"{currency} {min_amount:,.2f} - {max_amount:,.2f} {interval_str}"
            elif min_amount:
                return f"{currency} {min_amount:,.2f}+ {interval_str}"
            elif max_amount:
                return f"Hasta {currency} {max_amount:,.2f} {interval_str}"
            else:
                return "Salario no especificado."

        except Exception as e:
            logging.error(f"Error al formatear salario: {str(e)}")
            return "Error al calcular salario."

    def _extract_requirements(self, job: pd.Series) -> List[str]:
        """Extrae requisitos del trabajo basados en la descripción."""
        # Aquí podrías implementar lógica más sofisticada de NLP
        # Por ahora retornamos una lista vacía
        return []