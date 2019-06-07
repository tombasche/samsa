import logging

import socket
from typing import Optional, List

from confluent_kafka import Consumer, Producer

from statestore.db.sqldb import SQLiteClient

logger = logging.getLogger(__name__)  # noqa


class PersistentConsumer:
    def __init__(
        self,
        topics,
        group_id,
        bootstrap_servers,
        table_name,
        store="sqlite",
        block_time=3.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        if store == "sqlite":
            self.db = SQLiteClient(
                db_name="{}.db".format(group_id), table_name=table_name
            )
        elif store == "rocksdb":
            try:
                from statestore.db.rocksdb import RocksDBClient
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
        self._partition_ids = []
        self._producer_has_started = False
        self._replica_consumer_has_started = False
        self.logger = logging.getLogger("kafka")
        self.consumer = self._init_consumer(topics, group_id)
        self._group_id = group_id
        self._replica_producer = None
        self._block_time = block_time
        if not self._replica_consumer_has_started:
            self._init_replica_consumer([0])
            self._replica_consumer_has_started = True
            self._recover_from_topic()

    def __enter__(self):
        """ Entered statestore context manager"""
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """Method called when exiting context manager"""
        logger.debug("Exiting persistent consumer...")
        self.consumer.close()
        logger.debug("Flushing any remaining messages to Kafka...")
        self._replica_producer.flush()
        self.db.close()
        if self._replica_consumer_has_started:
            self._replica_consumer.close()

    def _init_producer(self, partition_ids: List[int]) -> Producer:
        """
        Initialise a producer which will replicate received messages
        Returns: Producer

        """
        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "enable.partition.eof": False,
            "log.connection.close": False,
        }
        self._replica_topics = [
            "{}-{}-changelog".format(self.table_name, partition_id)
            for partition_id in partition_ids
        ]
        self._partition_ids = partition_ids
        producer = Producer(producer_config, logger=self.logger)
        logger.debug("Producer config: {}".format(producer_config))
        logger.info("Initialized producer for topic {}".format(self._replica_topics))
        self._producer_has_started = True
        self._replica_producer = producer
        return producer

    def _set_replica_topics(self, partition_ids: List = None):
        """ Set the topics names to replicate back to the broker"""
        if partition_ids:
            self._partition_ids = partition_ids
        self._replica_topics = [
            "{}-{}-changelog".format(self.table_name, partition_id)
            for partition_id in self._partition_ids
        ]

    def _init_replica_consumer(self, partition_ids: List = None) -> Consumer:
        """ Initialise the replica consumer and optionally set the partition_ids """
        group_id = self._group_id
        self._set_replica_topics(partition_ids)
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "client.id": socket.gethostname(),
            "enable.partition.eof": False,
            "log.connection.close": False,
            "partition.assignment.strategy": "range",
        }

        consumer = Consumer(consumer_config, logger=self.logger)
        consumer.subscribe(self._replica_topics)
        self._replica_consumer = consumer
        self._replica_consumer_has_started = True

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
            "client.id": socket.gethostname(),
            "enable.partition.eof": False,
            "log.connection.close": False,
            "partition.assignment.strategy": "range",
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
            new_ids = [partition.partition for partition in partitions]
            if new_ids == self._partition_ids:
                return

            self._partition_ids = new_ids
            logger.debug("Rebalancing to partitions {}".format(self._partition_ids))
            self._init_producer(self._partition_ids)
            self._recover_from_topic()

        consumer = Consumer(consumer_config, logger=self.logger)
        consumer.subscribe(topics, on_assign=_on_assign)
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
        if not self._producer_has_started:
            self._init_producer(self._partition_ids)
        logger.debug("Replicating to topic...")
        self._replica_producer.produce(topic=topic, key=key, value=value)
        self._replica_producer.poll(0)

    def _recover_from_topic(self):
        """
        Consume from the replica topic(s) until there's
        nothing left and repopulate the local store.
        """
        if not self._replica_consumer_has_started:
            self._init_replica_consumer()

        received_all = False
        first_run = True
        while not received_all:
            if first_run:
                block_time = 2.0
                first_run = False
            else:
                block_time = self._block_time
            logger.debug("Consuming from replica and waiting {}".format(block_time))
            msg = self._replica_consumer.poll(timeout=block_time)
            received_all = msg is not None
            if msg:
                if msg.error():
                    logger.exception("Error fetching message on recovery")
                    continue
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                partition = msg.partition()
                logger.debug("Recovered {}={} at {}".format(key, value, partition))
                self.db.put(key, value, partition)

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
        partitions = [partition.partition for partition in self.consumer.assignment()]
        for partition_id in partitions:

            ok = self.db.put(key, value, partition_id)
            if ok:
                self._replicate_to_topic(
                    self.changelog_topic_name(partition_id), key, value
                )

    def query(self, key: str) -> Optional[str]:
        """
        Query the data store for a specific key and partition
        Args:
            key: string to identify the key/value pair
        Returns:
        """
        for partition_id in self._partition_ids:
            data = self.db.get(key, partition_id)
            if data:
                return data
        logger.debug("No data found for key {}".format(key))
        return None

    class UnknownDatabaseSpecified(Exception):
        """
        Error raised if user specifies db other than sqlite or rocksdb
        """

        pass
