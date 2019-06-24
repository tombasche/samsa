import logging

from random import random
from time import sleep
from typing import Optional, List, Callable, Any

from confluent_kafka import Consumer, Producer, KafkaError

from samsa.db import ConnectionType, SQLiteClient
from samsa.kafka import KafkaTimeoutError

logger = logging.getLogger(__name__)  # noqa


class StatefulConsumer:
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: str,
        table_name: str,
        store: str = ConnectionType.sqlite.value,
        block_time: float = 25.0,
    ):
        """
        Class for a stateful consumer.
        Args:
            topics: a list of topics
            group_id: a group id for the main consumer
            bootstrap_servers: string of servers separated by a semicolon
            table_name: The name of the table to hold the state required
            store: the store provider, either rocksdb or sqlite
            block_time:
                A configurable block time. The default should be satisfactory
                but this can be tweaked if the replica recovery is timing out.

        """
        self.bootstrap_servers = bootstrap_servers
        if store == ConnectionType.sqlite.value:
            self.db = SQLiteClient(
                db_name="{}.db".format(group_id), table_name=table_name
            )
        elif store == ConnectionType.rocksdb.value:
            try:
                from statestore.db import RocksDBClient
            except ImportError:
                logger.error("RocksDB bindings are not installed...")
                raise
            self.db = RocksDBClient()
        else:
            raise self.UnknownDatabaseSpecified(
                "Storage method '{}' is not supported.".format(store)
            )
        self.table_name = table_name
        self._replica_topics = []
        self._partition_ids = []  # set a sensible default
        self.logger = logging.getLogger("kafka")
        self.consumer = self._init_consumer(topics, group_id)
        self._group_id = group_id
        self._block_time = block_time

    def __enter__(self):
        """ Entered context manager"""
        logger.debug('Created StatefulConsumer with table {}'.format(
            self.table_name
        ))
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """Method called when exiting context manager"""
        self.consumer.close()
        self.db.close()

    def _set_replica_topics(self):
        """ Set the topics names to replicate back to the broker"""
        self._replica_topics = [
            "{}-{}-changelog".format(self.table_name, partition)
            for partition in self._partition_ids
        ]

    def _init_consumer(self, topics: List[str], group_id: str) -> Consumer:
        """
        Initialise a consumer which will be used to consumer whatever
        topics we specify.
        Args:
            topics: The topics for the main consumer
            group_id: Consumer group id specified on start.
        Returns: Consumer

        """
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "enable.partition.eof": False,
            "log.connection.close": False,
        }

        def _on_assign(_, partitions: List):
            """
            If the main consumer gets reassigned different partitions, then
            we reset the producer to the new partition ids specified, and
            recover any message for those partition ids.
            Args:
                _: ?
                partitions: list of partitions we've been reassigned.

            Returns:

            """
            if not partitions:
                logger.debug("No partitions assigned")
                return
            new_ids = set([partition.partition for partition in partitions])
            if new_ids == self._partition_ids:
                logger.debug("Partition ids haven't changed")
                return

            self._partition_ids = new_ids
            logger.debug("Assigning to partitions {}".format(self._partition_ids))
            self._recover_from_topic()

        consumer = Consumer(consumer_config, logger=self.logger)
        consumer.subscribe(topics, on_assign=_on_assign)
        logger.debug('Started consumer for topic(s) {}'.format(topics))
        return consumer

    def _replicate_to_topic(self, topic: str, key: str, value: str):
        """
        For a given topic, key and value,
        produce the message back into Kafka so we can restore from this topic.
        Args:
            topic: topic message is associated with
            key: message key
            value: message contents
        Returns:
        """
        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "enable.partition.eof": False,
            "log.connection.close": False,
        }

        producer = Producer(producer_config)
        producer.produce(topic=topic, key=key, value=str(value))
        producer.poll(0)
        producer.flush()
        logger.debug('Produced to {}'.format(topic))

    def _recover_from_topic(self):
        """
        Consume from the replica topic(s) until there's
        nothing left and repopulate the local store.
        """
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": '{}-recovery1'.format(self._group_id),
            "enable.partition.eof": False,
            "log.connection.close": False,
        }
        self._set_replica_topics()
        consumer = Consumer(consumer_config)
        consumer.subscribe(self._replica_topics)

        received_all = False
        while not received_all:
            logger.debug("Consuming from replica and waiting {}s".format(self._block_time))
            messages = consumer.consume(timeout=self._block_time)
            received_all = messages is not None
            for msg in messages:
                if msg.error():
                    logger.exception("Error fetching message on recovery")
                    continue
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                partition = msg.partition()
                logger.debug("Recovered {}={} at {}".format(key, value, partition))
                self.db.put(key, value)
        consumer.poll(0)

    def changelog_topic_name(self, partition: int) -> str:
        """ Generate the name of the topic for a given partition id and table """
        return "{}-{}-changelog".format(self.table_name, partition)

    def save(self, key: str, value: str):
        """
        Save the last message to the db

        Args:
            key: Key to associate the value with
            value: The value to store in the database
        Returns:

        """
        partitions = set([partition.partition for partition in self.consumer.assignment()])
        ok = self.db.put(key, value)
        for partition_id in partitions:
            if ok:
                self._replicate_to_topic(
                    self.changelog_topic_name(partition_id), key, value
                )

    def query(self, key: str) -> Optional[str]:
        """
        Query the data store for a specific key
        Args:
            key: string to identify the key/value pair
        Returns:
        """
        data = self.db.get(key)
        if data:
            return data
        logger.debug("No data found for key {}".format(key))
        return None

    class UnknownDatabaseSpecified(Exception):
        """
        Error raised if user specifies db other than sqlite or rocksdb
        """

        pass

    def consume(self, callback: Callable, *args: Any, **kwargs: Any) -> Optional[Any]:
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
                msg.error().code() == KafkaError._PARTITION_EOF  # pylint: disable=protected-access
            ):
                logger.debug("Partition EOF for {}".format((msg.topic(), msg.key())))
                sleep(random.random() * 3)
                resp = None
            else:
                logger.error(
                    "Error for {}: {}".format((msg.topic(), msg.key()), msg.error())
                )
                resp = None
            return resp
        else:
            # timeout, nothing to read, let the caller handle it.
            logger.debug("Timed out. Nothing to read. Raising KafkaTimeoutError.")
            raise KafkaTimeoutError
