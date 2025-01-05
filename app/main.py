from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.api import api_router
from app.config.database import initialize_database
from app.config.settings import settings
from dotenv import load_dotenv
import logging
import os

from app.core.event.kafka.producer import KafkaProducer

# Configurar logging
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include your v1 router
app.include_router(api_router, prefix=settings.API_V1_STR)

# Declare a global or `app.state` variable to store the Kafka producer instance
kafka_producer: KafkaProducer | None = None


@app.on_event("startup")
async def startup_event():
    print(">>> DEBUG: Entrando a startup_event() <<<")
    await initialize_database()

    try:
        print("DEBUG: Antes de instanciar KafkaProducer.")
        app.state.kafka_producer = KafkaProducer()
        print("DEBUG: Después de instanciar KafkaProducer, antes de start().")

        await app.state.kafka_producer.start()
        print("DEBUG: Después de await kafka_producer.start().")

        logger.info("Kafka producer started.")
    except Exception as e:
        print(f"ERROR en startup_event: {str(e)}")
        logger.exception("Excepción inicializando el KafkaProducer")


@app.on_event("shutdown")
async def shutdown_event():
    kafka_producer = app.state.kafka_producer
    if kafka_producer:
        await kafka_producer.stop()
        logger.info("Kafka producer stopped.")


# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": settings.VERSION,
        "langsmith_enabled": True
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8092, workers=1)
