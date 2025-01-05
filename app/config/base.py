# base.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData

# Esquema predeterminado para las tablas
schema = "jobs"

# Metadata con el esquema predeterminado
metadata = MetaData(schema=schema)

# Base declarativa para los modelos
Base = declarative_base(metadata=metadata)