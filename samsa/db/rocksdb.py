"""
Class to help with storing Kafka state.
"""
import os
import logging
from typing import Any, Optional

import pyrocksdb

logger = logging.getLogger(__name__)


class RocksDBClient:
    """
    A wrapper client around the python-rocksdb binding library.
    Examples:

    >>>    db = RocksDBClient()
    >>>    db.put("something", "test")
    """

    # defines where the data is stored
    data_path = os.environ.get("ROCKS_DB_PATH", "/tmp/rocksdb")
    last_entry_key = "last_entry"

    def __init__(self, create_if_missing=True, flush_size=10):
        """
        Initialise a new RocksDBClient
        Args:
            create_if_missing: Create the key and value pair if they
            aren't already in the database.
            flush_size: the size of the internal cache to keep data for before
            writing to disk.
        """
        self.flush_size = flush_size
        self.path = RocksDBClient.data_path
        self.db = pyrocksdb.DB()
        self.opts = pyrocksdb.Options()
        self.create_if_missing = create_if_missing
        self.opts.IncreaseParallelism()
        self.opts.OptimizeLevelStyleCompaction()
        self.opts.create_if_missing = self.create_if_missing
        self.connection = self.db.open(self.opts, self.path)
        if self.connection.ok():
            logger.debug("DB connection established")
        else:
            raise self.ConnectionError("DB connection failed")

    def put(self, key: str, value: Any) -> bool:
        """
        Put the given key value pair in the database.
        Args:
            key: key to identify the 'row' in the database with.
            value: A stringable object to save in the database.
        Returns: True/False if the put was successful.

        """
        result = self.db.put(pyrocksdb.WriteOptions(), key, value)
        logger.debug("Put {}:{} to {}".format(key, value, self.path))
        return result.ok()

    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the database by the given key.
        Returns the value if found, or nothing.
        Args:
            key: the key to query with.
        Returns: The value specified, or nothing.

        """
        result = self.db.get(pyrocksdb.ReadOptions(), key)
        logger.debug("Retrieved {} from {}".format(key, self.path))
        return result.data

    def delete(self, key: str) -> bool:
        """
        Delete a supplied key/value pair from the database.
        Args:
            key: the key to delete
        Returns: True/False if the delete was successful.

        """
        result = self.db.delete(pyrocksdb.WriteOptions(), key)
        logger.debug("Deleted {} from {}".format(key, self.path))
        return result.ok()

    def __delete__(self, instance):
        """
        We close the connection when GC cleans up the database object.
        Write any values in the cache to disk.
        Args:
            instance: the RocksDBClient instance
        Returns:
        """
        logger.debug("DB Connection closed.")
        instance.db.close()

    class ConnectionError(Exception):
        """
        Exception raised if database fails to establish a connection.
        """

        pass

    class InvalidPutArgs(Exception):
        """
        Raise this if the args to put are invalid.
        """

        pass
