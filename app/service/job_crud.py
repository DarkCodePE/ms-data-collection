# app/services/job_crud.py
from datetime import datetime

from sqlalchemy.orm import Session

import httpx
import json
import logging
from app.config.settings import settings
from app.core.event.kafka.producer import KafkaProducer
from app.core.model.job_offer import JobOffer
from app.core.model.schemas import LinkedInJobCreate

logger = logging.getLogger(__name__)


class JobCRUD:
    def __init__(self):
        self.kafka_producer = KafkaProducer()
        self.ms_job_api_url = settings.MS_JOB_API_URL

    async def create_job(
            self,
            db: Session,
            job_data: LinkedInJobCreate,
            prefer_kafka: bool = True
    ) -> JobOffer:
        """
        Crea una oferta de trabajo usando Kafka como método principal
        y la API REST como fallback.
        """
        try:
            if prefer_kafka:
                # Intentar primero con Kafka
                await self.publish_job_event(job_data, "JOB_CREATED")
                # Guardar localmente también para tracking
                return self._create_local_job(db, job_data)
            else:
                # Usar API REST directamente
                return await self._create_job_api(job_data)
        except Exception as e:
            logger.error(f"Error creating job: {str(e)}")
            # Si falla Kafka, intentar con API REST
            if prefer_kafka:
                logger.info("Retrying with REST API...")
                return await self._create_job_api(job_data)
            raise

    async def publish_job_event(self, job_data: LinkedInJobCreate, event_type: str):
        """Publica un evento de trabajo en Kafka"""
        event = {
            "type": event_type,
            "data": job_data.dict(),
            "source": "linkedin_scraper",
            "timestamp": datetime.utcnow().isoformat()
        }

        await self.kafka_producer.send_event("job-events", event)

    async def _create_job_api(self, job_data: LinkedInJobCreate) -> JobOffer:
        """Crea una oferta usando la API REST de ms-job"""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.ms_job_api_url}/jobs",
                json=job_data.dict()
            )
            if response.status_code == 201:
                return JobOffer(**response.json())
            raise Exception(f"Error creating job via API: {response.text}")

    def _create_local_job(self, db: Session, job_data: LinkedInJobCreate) -> JobOffer:
        """
        Crea una versión local de la oferta para tracking
        No es la fuente principal de verdad, solo para seguimiento
        """
        job = JobOffer(
            **job_data.dict(),
            source="LinkedIn",
            synced=True
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        return job

    async def update_job(
            self,
            db: Session,
            job_id: str,
            job_data: dict,
            prefer_kafka: bool = True
    ) -> JobOffer:
        """Actualiza una oferta de trabajo"""
        try:
            if prefer_kafka:
                await self.publish_job_event(
                    {"id": job_id, **job_data},
                    "JOB_UPDATED"
                )
                return self._update_local_job(db, job_id, job_data)
            else:
                return await self._update_job_api(job_id, job_data)
        except Exception as e:
            logger.error(f"Error updating job: {str(e)}")
            if prefer_kafka:
                return await self._update_job_api(job_id, job_data)
            raise