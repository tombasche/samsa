import logging
import sqlite3
import string
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SQLiteClient:

    create_initial_sql = "CREATE TABLE IF NOT EXISTS {table_name} (key TEXT, value TEXT, partition_id INTEGER)"

    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.connection = sqlite3.connect(self.db_name)
        self.table_name = table_name
        self._initial_create()

    def _initial_create(self):
        try:
            self.connection.execute(SQLiteClient.create_initial_sql.format(table_name=self.table_name))
        except Exception as ex:
            raise Exception(f'Failed to create table with name {self.table_name} because of {ex}')

    def __delete__(self, instance):
        instance.connection.close()

    def put(self, key: str, value: Any, partition_id: int) -> bool:
        """
        Put the given key value pair in the database.
        Args:
            key: key to identify the 'row' in the database with.
            value: A stringable object to save in the database.
            partition_id: The partition id this data belongs to.
        Returns: True/False if the put was successful.

        """
        put_sql = f"INSERT INTO {self.table_name} (key, value, partition_id) VALUES (?, ?, ?)"
        result = self.connection.execute(
            put_sql, (key, value, partition_id)
        )
        logger.debug("Put {}:{} to {}".format(key, value, self.db_name))
        self.connection.commit()
        return result.rowcount == 1

    def get(self, key: str, partition_id: int) -> Optional[Any]:
        """
        Get a value from the database by the given key.
        Returns the value if found, or nothing.
        Args:
            key: the key to query with.
            partition_id: The partition id this data belongs to
        Returns: The value specified, or nothing.

        """
        get_sql = f"SELECT value FROM {self.table_name} WHERE key = ? AND partition_id = ?"
        cursor = self.connection.cursor()
        result = cursor.execute(get_sql, (key, partition_id)).fetchone()
        if result:
            logger.debug("Retrieved {} from {}".format(key, self.db_name))
            return result
        return None

    def close(self):
        """
        Close the connection
        """
        self.connection.close()
