"""
Kafka producer.
"""
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    A Kafka producer implemented as a context so it can be used like:
            `with KafkaProducer(...) as producer:`
    This saves having to remember to flush the producer at the end.
    """

    def __init__(self, bootstrap_servers, topic=None):
        """
        We allow topic to be none to allow for ad-hoc producing
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "enable.partition.eof": False,
            "log.connection.close": False,
        }

        self.producer = Producer(producer_config, logger=logger)

        logger.debug("Kafka producer config: {}".format(producer_config))
        logger.info("Initialized kafka producer for topic {}".format(self.topic))

    def __enter__(self):
        """
        Context method called when entering.
        """
        return self

    def __exit__(self, *args):
        """
        Context method called when exiting the context.
        """
        self.producer.flush()

    def produce(self, key, value, topic=None):
        """
        Produces a message to Kafka
        """
        produce_topic = topic if topic else self.topic
        logger.debug("Producing to {}".format(produce_topic))
        self.producer.produce(produce_topic, key, value)
        self.producer.poll(0)
