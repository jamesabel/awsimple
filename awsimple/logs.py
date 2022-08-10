import time
import getpass
import platform
from functools import lru_cache
from typing import Union

from awsimple import AWSAccess


@lru_cache()
def get_user_name() -> str:
    return getpass.getuser()


@lru_cache()
def get_computer_name() -> str:
    return platform.node()


class LogsAccess(AWSAccess):
    """
    Perform logging to AWS using CloudWatch Logs
    """

    def __init__(self, log_group: str, **kwargs):
        """
        Log to AWS CloudWatch.
        :param log_group: AWS CloudWatch log group
        :param kwargs: other kwargs (e.g. for authentication)
        """
        super().__init__(**kwargs)
        self.log_group = log_group
        self.client = self.session.client("logs")  # AWSAccess doesn't provide the client
        self._upload_sequence_token = None  # type: Union[str, None]

    def put(self, message: str):
        """
        Log a message.
        :param message: message as a string
        """
        try:
            self._put(message)
            success = True
        except self.client.exceptions.ResourceNotFoundException:
            success = False
        if not success:
            # log group and stream does not appear to exist, so make them
            try:
                self.client.create_log_group(logGroupName=self.log_group)
                self.client.put_retention_policy(logGroupName=self.log_group, retentionInDays=self.get_retention_in_days())
            except self.client.exceptions.ResourceAlreadyExistsException:
                pass
            self.client.create_log_stream(logGroupName=self.log_group, logStreamName=self.get_stream_name())
            self._put(message)

    def _put(self, message: str):
        """
        Perform the put log event. Internal method to enable try/except in the regular .put() method.
        :param message: message as a string
        """

        if self._upload_sequence_token is None:
            # we don't yet have the sequence token, so try to get it from AWS
            log_streams_description = self.client.describe_log_streams(logGroupName=self.log_group)
            if (log_streams := log_streams_description.get("logStreams")) is not None and len(log_streams) > 0:
                self._upload_sequence_token = log_streams[0].get("uploadSequenceToken")

        # timestamp defined by AWS to be mS since epoch
        log_events = [{"timestamp": int(round(time.time() * 1000)), "message": message}]
        try:
            if self._upload_sequence_token is None:
                put_response = self.client.put_log_events(logGroupName=self.log_group, logStreamName=self.get_stream_name(), logEvents=log_events)
            else:
                put_response = self.client.put_log_events(logGroupName=self.log_group, logStreamName=self.get_stream_name(), logEvents=log_events, sequenceToken=self._upload_sequence_token)
        except self.client.exceptions.InvalidSequenceTokenException:
            put_response = self.client.put_log_events(logGroupName=self.log_group, logStreamName=self.get_stream_name(), logEvents=log_events)
        self._upload_sequence_token = put_response.get("nextSequenceToken")

    def get_stream_name(self) -> str:
        """
        Get the stream name. User of this class can override this method to use a different stream name.
        :return: stream name string
        """
        return f"{get_computer_name()}-{get_user_name()}"

    def get_retention_in_days(self) -> int:
        """
        Define the log retention in days.  User of this class can override this method to use a different retention period (only used when log group is created).
        :return: retention time in days as an integer
        """
        return 365
