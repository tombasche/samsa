# kafka-state-stores

A python implementation of the state stores concept within the Kafka Streams API

## Installing the RocksDB client

`brew install cmake` if you haven't got it already

`git clone https://github.com/twmht/python-rocksdb.git --recursive -b pybind11`
`python setup.py install` inside the python-rocksdb directory.

You can run rocksdb locally on OSX with `brew install rocksdb`. It's not a DB in the traditional sense, but really just
a super fast way to store stuff on disk (as fast as that really is!).

## The following is a pretty simple way of pulling data from kafka, and saving it to a local store. The default is sqlite but rocksdb is also possible.

```
def loop():
    topic = CONNECTION_STATUS_TOPIC
    with PersistentConsumer(
        topic, group_id=KAFKA_GROUP_ID, bootstrap_servers="someKafka.repositpower.net:9092"
    ) as store:
        while True:
            msg = store.consumer.poll(timeout=3.4)
            if msg:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                store.save(key, value)
            else:
                data = store.query("testec001", 0)
                print(data)
```
