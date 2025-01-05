# app/core/exceptions.py
class ScraperException(Exception):
    """Base exception for scraper errors."""
    pass


class MongoDBError(ScraperException):
    """Raised when there's an error with MongoDB operations."""
    pass


class ScrapingError(ScraperException):
    """Raised when there's an error during scraping."""
    pass


class ETLError(ScraperException):
    """Raised when there's an error during ETL process."""
    pass


class KafkaError(ScraperException):
    """Raised when there's an error with Kafka operations."""
    pass
