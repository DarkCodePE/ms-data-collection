import json
import logging
from aiokafka import AIOKafkaProducer
from app.config.settings import settings

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self._started = False

    async def start(self):
        if not self._started:
            await self.producer.start()
            self._started = True

    async def stop(self):
        if self._started:
            await self.producer.stop()
            self._started = False

    async def send_event(self, topic: str, event: dict):
        if not self._started:
            await self.start()
        try:
            await self.producer.send_and_wait(topic, event)
            logger.info(f"Event sent to Kafka: {event['type']}")
        except Exception as e:
            logger.error(f"Error sending event to Kafka: {str(e)}")
            raise
