from enum import Enum

from samsa.db.rocksdb import RocksDBClient
from samsa.db.sqldb import SQLiteClient


class ConnectionType(Enum):
    """
    Defines the available connection types when defining the stateful consumer
    """
    sqlite = 'sqlite'
    rocksdb = 'rocksdb'
