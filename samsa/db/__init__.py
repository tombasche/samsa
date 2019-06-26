from enum import Enum

try:
    from samsa.db.rocksdb import RocksDBClient
except ImportError:
    pass
from samsa.db.sqldb import SQLiteClient


class ConnectionType(Enum):
    """
    Defines the available connection types when defining the stateful consumer
    """
    sqlite = 'sqlite'
    rocksdb = 'rocksdb'
