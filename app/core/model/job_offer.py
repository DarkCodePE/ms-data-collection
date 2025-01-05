from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID

from app.config.base import Base


# Solo definimos los campos que necesitamos para hacer el scraping
class JobOffer(Base):
    __tablename__ = "job_offers"
    __table_args__ = {"schema": "jobs"}

    id = Column(UUID, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=False)
    company = Column(String, nullable=False)
    source = Column(String, nullable=True)  # Para identificar que viene de LinkedIn
    source_url = Column(String, nullable=True, unique=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)