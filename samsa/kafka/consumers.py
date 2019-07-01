import logging
import time
import random

from typing import List, Callable, Any

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    A Kafka consumer implemented as a context so it can be used like:
            `with KafkaConsumer(...) as consumer:`
    This saves the developer needing to remember to close the consumer at the end.
    A callback can be registered when call consume to process the message.
    """

    def __init__(
        self, bootstrap_servers: List[str], group_id: str, topics: list
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics

        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.partition.eof": False,
            "log.connection.close": False,
        }

        self.consumer = Consumer(consumer_config, logger=logger)
        logger.debug("Kafka consumer config: {}".format(consumer_config))
        logger.info(
            "Initialized kafka consumer (group id: {}) with topics {}".format(
                self.group_id, self.topics
            )
        )

    def __enter__(self):
        """
        Context method called when entering.
        """
        self.consumer.subscribe(self.topics)
        logger.debug("Kafka consumer subscribed to topics {}".format(self.topics))

        return self

    def __exit__(self, *args):
        """
        Context method called when exiting and used to close the consumer.
        """
        self.consumer.close()

        logger.info("Closed Kafka consumer with topics {}".format(self.topics))

    def consume(self, callback: Callable, *args: Any, **kwargs: Any):
        """
        Consumes a message from Kafka and deals with errors. Each time a message is consumed the
        `callback` will be called with the message as its first argument followed by any *args and
        **kwargs specified.
        """
        msg = self.consumer.poll(timeout=3.4)
        if msg is not None:
            if not msg.error():
                resp = callback(msg, *args, **kwargs)
            elif (
                msg.error().code() == KafkaError._PARTITION_EOF
            ):
                logger.debug("Partition EOF for {}".format((msg.topic(), msg.key())))
                time.sleep(random.random() * 3)
            else:
                logger.error(
                    "Error for {}: {}".format((msg.topic(), msg.key()), msg.error())
                )
            return resp
        else:
            # timeout, nothing to read, let the caller handle it.
            logger.debug("Timed out. Nothing to read. Raising KafkaTimeoutError.")
            raise KafkaTimeoutError()

