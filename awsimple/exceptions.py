class AWSimpleException(Exception):
    """Base exception for all AWSimple errors."""

    pass


class AWSimpleExceptionBase(AWSimpleException):
    """Deprecated intermediate base exception, retained for backwards compatibility. Use AWSimpleException."""

    pass


class DynamoDBItemAlreadyExists(AWSimpleExceptionBase):
    """Raised when an item already exists in DynamoDB."""

    def __init__(self, table_name: str, primary_partition_value: str, primary_sort_value: str):
        if len(primary_sort_value) < 1:
            sort_message = ""
        else:
            sort_message = f" and {primary_sort_value=}"
        message = f"Item with {primary_partition_value=}{sort_message} already exists in table {table_name}"
        super().__init__(message)


class S3BucketAlreadyExistsNotOwnedByYou(AWSimpleExceptionBase):
    """Raised when an S3 bucket already exists and is not owned by you."""

    def __init__(self, bucket_name: str):
        message = f'S3 Bucket "{bucket_name}" already exists and is not owned by you'
        super().__init__(message)


class PubSubFailed(AWSimpleException):
    """Raised when a pubsub (Pub or Sub) thread has ended on an error, so it no longer publishes or receives messages."""

    def __init__(self, channel: str, reason: str):
        message = f'pubsub thread for channel "{channel}" failed : {reason}'
        super().__init__(message)
