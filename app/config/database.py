from motor.motor_asyncio import AsyncIOMotorClient
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://admin:password@localhost:27017/?authSource=admin"
    MONGO_DB_NAME: str = "scraper_db"
    MONGO_COLLECTION_NAME: str = "job_offers"


settings = Settings()

client = AsyncIOMotorClient(settings.MONGO_URI)
db = client[settings.MONGO_DB_NAME]


async def initialize_database():
    """
    Inicializa la base de datos verificando que las colecciones existan y tengan índices configurados.
    """
    # Verifica si la colección existe, si no, la crea
    existing_collections = await db.list_collection_names()
    if settings.MONGO_COLLECTION_NAME not in existing_collections:
        print(f"Creating collection: {settings.MONGO_COLLECTION_NAME}")
        await db.create_collection(settings.MONGO_COLLECTION_NAME)

    # Configura índices en la colección
    collection = db[settings.MONGO_COLLECTION_NAME]
    await collection.create_index([("source", 1), ("url", 1)], unique=True)
    await collection.create_index([("processed", 1), ("created_at", 1)])


def get_collection():
    return db[settings.MONGO_COLLECTION_NAME]
