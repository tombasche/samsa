# samsa

A Python implementation of the state stores concept within the Kafka Streams API

## Whats in a name?

Gregor Samsa was the unfortunate soul in the Kafka classic 'The Metamorphosis (1915)' who woke up one day as a giant insect.

## Installing the RocksDB client

`brew install cmake` if you haven't got it already

`git clone https://github.com/twmht/python-rocksdb.git --recursive -b pybind11`
`python setup.py install` inside the python-rocksdb directory.

You can run rocksdb locally on OSX with `brew install rocksdb`. It's not a DB in the traditional sense, but really just
a super fast way to store stuff on disk (as fast as that really is!).

## Example 
The following is a pretty simple way of pulling data from kafka, and saving it to a local store. The default is sqlite but rocksdb is also possible.
The example below:

1. Initialises a consumer with a table called 'deployment_status' 
2. Fetches the deployment object for the received message from topic 'events.connection.status'
2. Compares it to the local store and handles it accordingly
3. It then saves the message to the store for future computation.

```
from samsa import PersistentConsumer

def loop():
    topic = "events.connection.status"
    with PersistentConsumer(
            topic, table_name="deployment_status", group_id=KAFKA_GROUP_ID,
            bootstrap_servers="localhost:9092"
    ) as store:
        while True:
            msg = store.consumer.poll(timeout=3.4)
            if msg:
                key = msg.key().decode("utf-8")
                depl = Deployment.get(key)
                if depl.site is not None and depl.state == "COMMISSIONED":
                    value = json.loads(msg.value().decode("utf-8"))["data"]["status"]
                    status = store.query(key)
                    if status != value:
                        print(f"Status has changed from {status} to {value} for depl {key}!")
                    else:
                        print("No status change, nothing to see here!")
                    store.save(key, value)
                
if __name__ == "__main__":
    loop()                
```
