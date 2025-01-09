# ms-scraper

## Descripción del Proyecto

**ms-scraper** es un servicio diseñado para recopilar ofertas de empleo desde diversas plataformas como LinkedIn, Indeed y Glassdoor. Este servicio utiliza herramientas avanzadas de scraping e integra funcionalidades como sincronización con bases de datos y manejo de eventos a través de Kafka. Está optimizado para capturar, procesar y almacenar datos relevantes de ofertas de trabajo.

## Funcionalidades Principales

- **Scraping de Empleos**:
  - Uso de `SmartScraperTool` y `JobSpy` para extraer ofertas de empleo.
  - Capacidad de filtrar por palabras clave, ubicación y antigüedad de las ofertas.

- **Sincronización de Datos**:
  - Almacenamiento de datos crudos en MongoDB.
  - Transformación y normalización mediante un ETL.
  - Publicación de eventos procesados en Kafka para otros servicios.

- **Manejo de Errores y Retrys**:
  - Implementación de estrategias de fallback entre Kafka y APIs REST.

- **Configuración Extensible**:
  - Parámetros ajustables como el número máximo de empleos por consulta, ubicación predeterminada y configuraciones de scraping.

## Arquitectura

- **Scraping**:
  - `LinkedInScraper` para LinkedIn.
  - `JobSpyScraper` para scraping en múltiples plataformas.
- **Almacenamiento**:
  - Uso de MongoDB para datos crudos.
  - Sincronización con bases de datos SQL utilizando SQLAlchemy.
- **Mensajería**:
  - Kafka para publicar eventos de sincronización y actualizaciones.

## Configuración

### Variables de Entorno

El proyecto utiliza un archivo `.env` para configurar las variables de entorno. Asegúrate de definir las siguientes variables:

- `LANGCHAIN_API_KEY`
- `SGAI_API_KEY`
- `KAFKA_BOOTSTRAP_SERVERS`
- `MONGO_URI`
- `MS_JOB_API_URL`
- Entre otras especificadas en `settings.py`.

### Instalación

1. Clona este repositorio:
   ```bash
   git clone <repositorio>
   cd ms-scraper
