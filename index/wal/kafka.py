import logging
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from confluent_kafka import Consumer as SyncConsumer
from confluent_kafka import KafkaException, Message
from confluent_kafka import Producer as SyncProducer

from core.model import default_deserializer, default_serializer

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaMessageData:
    key: Optional[Any]
    value: Optional[Any]
    partition: int
    offset: int


class KafkaReader:

    def __init__(
        self,
        conf: dict,
        topic: str,
        key_deserializer: Callable[
            [Optional[bytes]], Any
        ] = default_deserializer,
        value_deserializer: Callable[
            [Optional[bytes]], Any
        ] = default_deserializer,
    ):
        conf.setdefault("enable.auto.commit", False)
        conf.setdefault("auto.offset.reset", "earliest")

        self._conf = conf
        self._topic = topic
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._consumer: Optional[SyncConsumer] = None

    def __enter__(self):
        log.info(f"KafkaReader: Initializing consumer topic={self._topic}")
        self._consumer = SyncConsumer(self._conf)
        self._consumer.subscribe([self._topic])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._consumer:
            log.info("KafkaReader: Closing consumer")
            self._consumer.close()

    def consume_one_batch(
        self,
        callback: Callable[[List[KafkaMessageData]], Any],
        batch_size: int = 10,
        timeout: float = 1.0,
    ) -> int:
        if not self._consumer:
            raise RuntimeError(
                "Consumer not initialized. Use 'with KafkaReader(...)'."
            )

        msgs: List[Message] = self._consumer.consume(
            num_messages=batch_size, timeout=timeout
        )

        if not msgs:
            return 0

        # 1. Process Deserialization
        batch: List[KafkaMessageData] = []
        for msg in msgs:
            if msg is None:
                continue

            if msg.error():
                if msg.error().fatal():
                    log.error(f"KafkaReader: Fatal Kafka error", exc_info=True)
                    raise KafkaException(msg.error())
                log.warning(
                    f"KafkaReader: Non-fatal Kafka issue: {msg.error()}"
                )
                continue

            try:
                key = self._key_deserializer(msg.key())
                value = self._value_deserializer(msg.value())
                batch.append(
                    KafkaMessageData(key, value, msg.partition(), msg.offset())
                )
            except Exception as e:
                log.error(
                    f"KafkaReader: Failed to deserialize message", exc_info=True
                )
                continue

        if not batch:
            return 0

        # 2. Execute Callback and Commit
        try:
            callback(batch)
            self._consumer.commit(asynchronous=False)

            return len(batch)
        except Exception as e:
            # Crucial: Log callback failure and DO NOT COMMIT
            log.error(
                f"KafkaReader: Callback failed for batch of {len(batch)} messages. Offsets NOT committed",
                exc_info=True,
            )
            return 0


class KafkaWriter:

    def __init__(
        self,
        conf: dict,
        key_serializer: Callable[
            [Optional[Any]], Optional[bytes]
        ] = default_serializer,
        value_serializer: Callable[
            [Optional[Any]], Optional[bytes]
        ] = default_serializer,
    ):
        self._conf = conf
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._producer: Optional[SyncProducer] = None

    def __enter__(self):
        log.info(f"KafkaWriter: Initializing producer")
        self._producer = SyncProducer(self._conf)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._producer:
            log.info("KafkaWriter: Flushing producer queue...")
            remaining = self._producer.flush(timeout=10)
            if remaining > 0:
                log.error(
                    f"KafkaWriter: Failed to flush {remaining} messages after 10s."
                )
            log.info(f"KafkaWriter: Producer closed.")

    def publish(
        self, topic: str, value: Any, key: Optional[Any] = None
    ) -> None:
        if not self._producer:
            raise RuntimeError(
                "Producer not initialized. Use 'with KafkaWriter(...)'."
            )

        serialized_key = self._key_serializer(key)
        serialized_value = self._value_serializer(value)

        def delivery_report(err, msg):
            if err is not None:
                log.error(
                    f"KafkaWriter: Message failed to deliver", exc_info=True
                )
            else:
                # Use print for debug output, typically
                # print(f"KafkaWriter: Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                pass

        try:
            self._producer.produce(
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                callback=delivery_report,
            )
            self._producer.poll(0)

        except KafkaException as e:
            log.error(f"KafkaWriter: Message failed to deliver", exc_info=True)
            raise
