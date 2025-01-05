from datetime import datetime
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from app.core.model.schemas import RawJobData, ProcessedJobData, JobSource
import logging


class MongoDBRepository:
    """Repositorio para gestionar las operaciones con MongoDB."""

    def __init__(self, mongodb_url: str, database: str):
        try:
            self.client = AsyncIOMotorClient(mongodb_url)
            # Verificar la conexión
            self.client.admin.command('ping')
            logging.info(f"Successfully connected to MongoDB at {mongodb_url}")

            self.db = self.client[database]
            self.raw_jobs_collection: AsyncIOMotorCollection = self.db.raw_jobs
            logging.info(f"Using database: {database}, collection: raw_jobs")

        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {str(e)}", exc_info=True)
            raise

    async def verify_connection(self) -> bool:
        """Verifica que la conexión a MongoDB esté funcionando."""
        try:
            logging.debug("Pinging MongoDB...")
            await self.client.admin.command('ping')
            logging.debug("MongoDB ping successful.")
            return True
        except Exception as e:
            logging.error(f"MongoDB connection test failed: {str(e)}", exc_info=True)
            return False

    async def initialize(self):
        """Método para inicializar índices y otras configuraciones asincrónicas."""
        await self._setup_indexes()

    async def _setup_indexes(self):
        """Configura los índices necesarios en MongoDB."""
        await self.raw_jobs_collection.create_index([
            ("source", 1),
            ("url", 1)
        ], unique=True)

        await self.raw_jobs_collection.create_index([
            ("processed", 1),
            ("created_at", 1)
        ])

    async def save_raw_job(self, job_data: RawJobData) -> str:
        try:
            logging.info(f"Preparing to save job: {job_data.url}")

            # Map the raw data fields correctly
            job_dict = {
                "source": job_data.source,
                "job_id": job_data.raw_data.get('job_url'),
                "title": job_data.raw_data.get('title'),
                "company": job_data.raw_data.get('company'),
                "description": job_data.raw_data.get('description'),
                "location": job_data.raw_data.get('location'),
                "url": job_data.raw_data.get('job_url'),
                "salary_range": job_data.salary_range,
                "requirements": job_data.requirements,
                "job_type": job_data.raw_data.get('job_type'),
                "experience_level": job_data.raw_data.get('job_level'),
                "raw_data": job_data.raw_data,
                "processed": False
            }

            update_operation = {
                "$set": {
                    **job_dict,
                    "updated_at": datetime.utcnow()
                },
                "$setOnInsert": {
                    "created_at": datetime.utcnow()
                }
            }

            result = await self.raw_jobs_collection.update_one(
                {
                    "source": job_data.source,
                    "url": job_data.raw_data.get('job_url')
                },
                update_operation,
                upsert=True
            )

            return str(result.upserted_id) if result.upserted_id else str(result.matched_count)

        except Exception as e:
            logging.error(f"Error saving raw job: {str(e)}", exc_info=True)
            raise

    async def get_unprocessed_jobs(self, limit: int = 100) -> List[RawJobData]:
        """Obtiene trabajos que no han sido procesados."""
        cursor = self.raw_jobs_collection.find(
            {"processed": False}
        ).sort("created_at", 1).limit(limit)

        jobs = []
        async for doc in cursor:
            jobs.append(RawJobData(**doc))
        return jobs

    async def mark_job_as_processed(self, job_id: str) -> bool:
        """Marca un trabajo como procesado."""
        result = await self.raw_jobs_collection.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "processed": True,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        return result.modified_count > 0

    async def get_jobs_by_source(
            self,
            source: JobSource,
            processed: Optional[bool] = None,
            limit: int = 100
    ) -> List[RawJobData]:
        """Obtiene trabajos por fuente y estado de procesamiento."""
        filter_query = {"source": source}
        if processed is not None:
            filter_query["processed"] = processed

        cursor = self.raw_jobs_collection.find(filter_query).limit(limit)
        return [RawJobData(**doc) async for doc in cursor]
