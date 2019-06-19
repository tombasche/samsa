class KafkaTimeoutError(Exception):
    """
    Error raised when the poll from Kafka returns nothing and therefore there is nothing to read.
    """

    pass  # pylint:disable=unnecessary-pass
