import logging
from typing import List, Optional
from datetime import datetime

from app.core.datastore.repository.mongodb import MongoDBRepository
from app.core.model.schemas import RawJobData, ProcessedJobData

from app.core.event.kafka.producer import KafkaProducer

logger = logging.getLogger(__name__)


class JobETLService:
    """Servicio ETL para transformar datos crudos en formato para ms-job."""

    def __init__(
            self,
            mongo_repository: MongoDBRepository,
            kafka_producer: KafkaProducer,
            batch_size: int = 100
    ):
        self.mongo_repository = mongo_repository
        self.kafka_producer = kafka_producer
        self.batch_size = batch_size

    def transform_job_data(self, raw_job: RawJobData) -> ProcessedJobData:
        """Transforma datos crudos al formato esperado por ms-job."""
        try:
            # Extraer requisitos del texto de la descripción usando NLP
            requirements = self._extract_requirements(raw_job.description)

            # Determinar si es remoto basado en la descripción y ubicación
            is_remote = self._detect_remote_job(raw_job.description, raw_job.location)

            # Normalizar el tipo de trabajo
            job_type = self._normalize_job_type(raw_job.job_type)

            return ProcessedJobData(
                source_job_id=raw_job.job_id,
                title=raw_job.title,
                company=raw_job.company,
                description=raw_job.description,
                requirements=requirements,
                location=raw_job.location,
                is_remote=is_remote,
                source_url=raw_job.url,
                salary_range=raw_job.salary_range,
                job_type=job_type,
                level=raw_job.experience_level or "NOT_SPECIFIED",
                source=raw_job.source
            )
        except Exception as e:
            logger.error(f"Error transforming job data: {str(e)}")
            raise

    def _extract_requirements(self, description: str) -> List[str]:
        """Extrae requisitos del texto de la descripción usando NLP."""
        # Aquí implementarías la lógica de extracción de requisitos
        # Por ahora retornamos una lista vacía
        return []

    def _detect_remote_job(self, description: str, location: str) -> bool:
        """Detecta si un trabajo es remoto basado en la descripción y ubicación."""
        description_lower = description.lower()
        location_lower = location.lower()

        remote_keywords = {"remote", "remoto", "trabajo a distancia", "home office", "teletrabajo"}
        return any(keyword in description_lower or keyword in location_lower
                   for keyword in remote_keywords)

    def _normalize_job_type(self, job_type: Optional[str]) -> str:
        """Normaliza el tipo de trabajo a los valores aceptados por ms-job."""
        if not job_type:
            return "FULL_TIME"

        job_type_lower = job_type.lower()

        if "full" in job_type_lower or "tiempo completo" in job_type_lower:
            return "FULL_TIME"
        elif "part" in job_type_lower or "medio tiempo" in job_type_lower:
            return "PART_TIME"
        elif "contract" in job_type_lower or "contrato" in job_type_lower:
            return "CONTRACT"
        else:
            return "FULL_TIME"

    async def process_pending_jobs(self):
        """Procesa trabajos pendientes y los envía a Kafka."""
        try:
            # Obtener trabajos sin procesar
            raw_jobs = await self.mongo_repository.get_unprocessed_jobs(self.batch_size)
            #print(f"Found {len(raw_jobs)} unprocessed jobs.")
            for raw_job in raw_jobs:
                try:
                    # Transformar datos
                    processed_job = self.transform_job_data(raw_job)

                    # Crear evento Kafka
                    event = {
                        "type": "JOB_CREATED",
                        "data": processed_job.dict(),
                        "metadata": {
                            "source": raw_job.source,
                            "processed_at": datetime.utcnow().isoformat(),
                            "raw_job_id": raw_job.job_id
                        }
                    }

                    # Enviar a Kafka
                    await self.kafka_producer.send_event("job-events", event)

                    # Marcar como procesado
                    await self.mongo_repository.mark_job_as_processed(raw_job.job_id)

                    logger.info(f"Successfully processed and sent job {raw_job.job_id}")

                except Exception as e:
                    logger.error(f"Error processing job {raw_job.job_id}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in process_pending_jobs: {str(e)}")
            raise
