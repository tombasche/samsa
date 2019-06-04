import logging

import socket

from confluent_kafka import Consumer, Producer

from statestore.db.sqldb import SQLiteClient

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class PersistentConsumer:
    def __init__(
        self, topic, group_id, bootstrap_servers, store="sqlite", block_time=15
    ):
        self.bootstrap_servers = bootstrap_servers
        if store == "sqlite":
            self.db = SQLiteClient(db_name="{}.db".format(group_id))
        elif store == "rocksdb":
            try:
                from statestore.db.rocksdb import RocksDBClient
            except ImportError:
                logger.error("RocksDB bindings are not installed...")
                raise
            self.db = RocksDBClient()
        else:
            raise self.UnknownDatabaseSpecified("{} is not supported.".format(store))

        self._replica_topics = []
        self.topic = topic
        self._partition_ids = []
        self._producer_has_started = False
        self._replica_consumer_has_started = False
        self.logger = logging.getLogger("kafka")
        self.consumer = self._init_consumer([topic], group_id)
        self._group_id = group_id
        self._replica_producer = None

        self._block_time = block_time

    def __enter__(self):
        """ Entered statestore context manager"""
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """Method called when exiting context manager"""
        logger.debug("Exiting persistent consumer...")
        self.consumer.close()
        self.db.close()
        if self._replica_consumer_has_started:
            self._replica_consumer.close()

    def _init_producer(self, partition_ids) -> Producer:
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
            "{}-{}-changelog".format(self.topic, partition_id)
            for partition_id in partition_ids
        ]
        self._partition_ids = partition_ids
        producer = Producer(producer_config, logger=self.logger)
        logger.debug("Producer config: {}".format(producer_config))
        logger.info("Initialized producer for topic {}".format(self._replica_topics))
        self._producer_has_started = True
        self._replica_producer = producer
        if not self._replica_consumer_has_started:
            self._replica_consumer = self._init_consumer(
                self._replica_topics, group_id="{}-statestore".format(self._group_id)
            )
            self._replica_consumer_has_started = True
            self._recover_from_topic()
        return producer

    def _init_replica_consumer(self) -> Consumer:
        group_id = self._group_id
        topics = self._replica_topics
        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "client.id": socket.gethostname(),
            "enable.partition.eof": False,
            "log.connection.close": False,
            "partition.assignment.strategy": "range",
        }

        consumer = Consumer(consumer_config, logger=self.logger)
        consumer.subscribe(topics)
        self._replica_consumer = consumer
        self._replica_consumer_has_started = True

    def _init_consumer(self, topics, group_id) -> Consumer:
        """
        Initialise a consumer which will consume replicated messages
        if the consumer crashes.
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

        def _on_assign(_, partitions):
            """Rebalancing..."""
            self._partition_ids = [partition.partition for partition in partitions]
            logger.debug("Rebalancing to partitions {}".format(self._partition_ids))
            if not self._producer_has_started:
                self._init_producer(self._partition_ids)
            self._recover_from_topic()

        consumer = Consumer(consumer_config, logger=self.logger)
        consumer.subscribe(topics, on_assign=_on_assign)
        return consumer

    def _replicate_to_topic(self, topic, key, value):
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
                block_time = 5
                first_run = False
            else:
                block_time = self._block_time
            msgs = self._replica_consumer.consume(timeout=block_time)
            received_all = msgs is not None
            for msg in msgs:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                partition = msg.partition()
                logger.debug("Recovered {}={} at {}".format(key, value, partition))
                self.db.put(key, value, partition)

    def changelog_topic_name(self, topic, partition) -> str:
        return "{}-{}-changelog".format(topic, partition)

    def save(self, key, value):
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
                    self.changelog_topic_name(self.topic, partition_id), key, value
                )

    def query(self, key: str, partition_id: int) -> str:
        """
        Query the data store for a specific key and partition
        Args:
            key:
            partition_id:
        Returns:
        """
        return self.db.get(key, partition_id)

    class UnknownDatabaseSpecified(Exception):
        """
        Error raised if user specifies db other than sqlite or rocksdb
        """

        pass
