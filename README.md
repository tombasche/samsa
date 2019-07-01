# samsa 🐞

A Python implementation of the state stores concept within the Kafka Streams API

## Whats in a name?

Gregor Samsa was the unfortunate soul in the Kafka classic 'The Metamorphosis (1915)' who woke up one day as a giant insect.

## Storage options

This uses SQLite by default but also supports RocksDB by specifying `store='rocksdb'` in the StatefulConsumer init.

## Installing the RocksDB client

`brew install cmake` if you haven't got it already

`git clone https://github.com/twmht/python-rocksdb.git --recursive -b pybind11`
`python setup.py install` inside the python-rocksdb directory.

You can run rocksdb locally on OSX with `brew install rocksdb`. It's not a DB in the traditional sense, but really just
a super fast way to store stuff on disk (as fast as that really is!).

## Example 
The following is a pretty simple way of pulling data from kafka, and saving it to a local store. The default is sqlite but rocksdb is also possible.
The example below:

1. Initialises a consumer with a table called 'node_status'
2. Queries the local store for the received key.
3. Compares the received value with a saved value in the local store.
4. It then saves the message to the store for future computation.

```
from samsa import StatefulConsumer

def loop():
    topic = "nodes.status"
    with StatefulConsumer(
            [topic], table_name="node_status", group_id=KAFKA_GROUP_ID,
            bootstrap_servers="localhost:9092"
    ) as store:
        while True:
            msg = store.consume(process_callback)
            if msg:
                key = msg.key().decode("utf-8")
                value = json.loads(msg.value().decode("utf-8"))["data"]["status"]
                
                status = store.query(key)
                if status != value:
                    print(f"Status has changed from {status} to {value} for {key}!")
                else:
                    print("No status change, nothing to see here!")
                store.save(key, value)
                
if __name__ == "__main__":
    loop()                
```
