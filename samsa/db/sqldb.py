import logging
import os
import sqlite3
from typing import Any, Optional

logger = logging.getLogger(__name__)

DB_LOCATION = os.environ.get("DB_LOCATION", "/tmp")


class SQLiteClient:

    create_initial_sql = "CREATE TABLE IF NOT EXISTS {table_name} (key TEXT, value TEXT)"

    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.connection = sqlite3.connect(self._database_path)
        self.table_name = table_name
        self._initial_create()

    @property
    def _database_path(self):
        return os.path.join(DB_LOCATION, self.db_name)

    def _initial_create(self):
        try:
            self.connection.execute(
                SQLiteClient.create_initial_sql.format(table_name=self.table_name)
            )
        except Exception as ex:
            raise Exception(
                "Failed to create table with name {} because of {}".format(
                    self.table_name, ex
                )
            )

    def __delete__(self, instance):
        instance.connection.close()

    def put(self, key: str, value: Any) -> bool:
        """
        'Put' the given key value pair in the database. This requires
        2 sql queries, one to check if the value already exists and another
        to update or insert it. Seems SQLite doesn't have any concept of a proper
        UPSERT.
        Args:
            key: key to identify the 'row' in the database with.
            value: A stringable object to save in the database.
        Returns: True/False if the put was successful.

        """
        existing_val = self.get(key)
        if existing_val:
            logger.debug('Updating existing value')
            sql = "UPDATE {} SET value = ? WHERE key = ?".format(self.table_name)
            result = self.connection.execute(sql, (value, key))
        else:
            logger.debug('Inserting new value')
            sql = "INSERT INTO {} (key, value) VALUES (?, ?)".format(self.table_name)
            result = self.connection.execute(sql, (key, value))

        logger.debug("Put {}:{} to {}".format(key, value, self.db_name))
        self.connection.commit()
        return result.rowcount == 1

    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the database by the given key.
        Returns the value if found, or nothing.
        Args:
            key: the key to query with.
        Returns: The value specified, or nothing.

        """
        get_sql = (
            "SELECT value FROM {} WHERE key = ?".format(self.table_name)
        )
        cursor = self.connection.cursor()
        result = cursor.execute(get_sql, (key, )).fetchone()
        if result:
            logger.debug("Retrieved {} from {}".format(key, self.db_name))
            return result[0]  # will be a tuple of a single value
        return None

    def close(self):
        """
        Close the connection
        """
        self.connection.close()
