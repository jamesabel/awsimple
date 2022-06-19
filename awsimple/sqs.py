"""
SQS Access
"""

from dataclasses import dataclass
from typing import List, Any, Dict, Union
import time
import statistics
from datetime import timedelta
from pathlib import Path
import json

from botocore.exceptions import ClientError
from typeguard import typechecked
import appdirs
from balsa import get_logger

from awsimple import AWSAccess, __application_name__, __author__

log = get_logger(__application_name__)


@dataclass
class SQSMessage:
    """
    SQS Message
    """

    message: str  # payload
    _m: Any  # AWS message itself (from boto3)
    _q: Any  # SQSAccess instance

    def delete(self):
        self._m.delete()  # boto3
        self._q._update_response_history(self.get_id())

    def get_id(self):
        return self._m.message_id

    def get_aws_message(self):
        # get the native AWS message
        return self._m


# AWS defaults
aws_sqs_long_poll_max_wait_time = 20  # seconds
aws_sqs_max_messages = 10


class SQSAccess(AWSAccess):
    @typechecked()
    def __init__(self, queue_name: str, immediate_delete: bool = True, visibility_timeout: int = None, minimum_visibility_timeout: int = 0, **kwargs):
        """
        SQS access

        :param queue_name: queue name
        :param immediate_delete: True to immediately delete read message(s) upon receipt, False to require the user to call delete_message()
        :param visibility_timeout: visibility timeout (if explicitly given) - set to None to automatically attempt to determine the timeout
        :param minimum_visibility_timeout: visibility timeout will be at least this long (do not set if visibility_timeout set)
        :param kwargs: kwargs to send to base class
        """
        super().__init__(resource_name="sqs", **kwargs)
        self.queue_name = queue_name

        # visibility timeout
        self.immediate_delete = immediate_delete  # True to immediately delete messages
        self.user_provided_timeout = visibility_timeout  # the queue will re-try a message (make it re-visible) if not deleted within this time
        self.user_provided_minimum_timeout = minimum_visibility_timeout  # the timeout will be at least this long
        self.auto_timeout_multiplier = 10.0  # for automatic timeout calculations, multiply this times the median run time to get the timeout

        self.sqs_call_wait_time = 0  # short (0) or long poll (> 0, usually 20)
        self.queue = None  # since this requires a call to AWS, this will be set only when needed

        self.immediate_delete_timeout: int = 30  # seconds
        self.minimum_nominal_work_time = 1.0  # minimum work time in seconds so we don't timeout too quickly, e.g. in case the user doesn't actually do any work

        # receive/delete times for messages (auto_delete set to False)
        self.response_history = {}  # type: Dict[Any, Any]

        # We write the history out as a file so don't make this too big. We take the median (for the nominal run time) so make this big enough to tolerate a fair number of outliers.
        self.max_history = 20

    def _get_queue(self):
        if self.queue is None:
            self.queue = self.resource.get_queue_by_name(QueueName=self.queue_name)
        return self.queue

    @typechecked()
    def _get_response_history_file_path(self) -> Path:
        """
        get response history file path

        :return:
        """
        p = Path(appdirs.user_data_dir(__application_name__, __author__), "response", f"{self.queue_name}.json")
        log.debug(f'response history file path : "{p}"')
        return p

    @typechecked()
    def create_queue(self) -> str:
        """
        create SQS queue

        :return: queue URL
        """
        return self.client.create_queue(QueueName=self.queue_name)["QueueUrl"]

    def delete_queue(self):
        """
        delete queue
        """
        self.resource.get_queue_by_name(QueueName=self.queue_name).delete()

    @typechecked()
    def exists(self) -> bool:
        """
        test if SQS queue exists

        :return: True if exists
        """
        try:
            self.resource.get_queue_by_name(QueueName=self.queue_name)
            queue_exists = True
        except self.client.exceptions.QueueDoesNotExist:
            queue_exists = False
        return queue_exists

    def calculate_nominal_work_time(self) -> int:
        response_times = []
        for begin, end in self.response_history.values():
            if end is not None:
                response_times.append(end - begin)
        nominal_work_time = max(statistics.median(response_times), self.minimum_nominal_work_time)  # tolerate in case the measured work is very short
        log.debug(f"{nominal_work_time=}")
        return nominal_work_time

    def calculate_visibility_timeout(self) -> int:

        if self.user_provided_timeout is None:
            if self.immediate_delete:
                visibility_timeout = self.immediate_delete_timeout  # we immediately delete the message so this doesn't need to be very long
            else:
                visibility_timeout = max(self.user_provided_minimum_timeout, round(self.auto_timeout_multiplier * self.calculate_nominal_work_time()))
        else:
            if self.immediate_delete:
                # if we immediately delete the message it doesn't make sense for the user to try to specify the timeout
                raise ValueError(f"nonsensical values: {self.user_provided_timeout=} and {self.immediate_delete=}")
            elif self.user_provided_minimum_timeout > 0:
                raise ValueError(f"do not specify both timeout ({self.user_provided_timeout}) and minimum_timeout {self.user_provided_minimum_timeout}")
            else:
                visibility_timeout = self.user_provided_timeout  # timeout explicitly given by the user

        return visibility_timeout

    @typechecked()
    def _receive(self, max_number_of_messages_parameter: int = None) -> List[SQSMessage]:

        if self.user_provided_timeout is None and not self.immediate_delete:
            # read in response history (and initialize it if it doesn't exist)
            try:
                with open(self._get_response_history_file_path()) as f:
                    self.response_history = json.load(f)
            except FileNotFoundError:
                pass
            except IOError as e:
                log.warning(f'IOError : "{self._get_response_history_file_path()}" : {e}')
            except json.JSONDecodeError as e:
                log.warning(f'JSONDecodeError : "{self._get_response_history_file_path()}" : {e}')
            if len(self.response_history) == 0:
                now = time.time()
                self.response_history[None] = (now, now + timedelta(hours=1).total_seconds())  # we have no history, so the initial nominal run time is a long time

        # receive the message(s)
        messages = []  # type: List[Any]
        continue_to_receive = True
        call_wait_time = self.sqs_call_wait_time  # first time through may be long poll, but after that it's a short poll

        while continue_to_receive:

            aws_messages = None

            if max_number_of_messages_parameter is None:
                max_number_of_messages = aws_sqs_max_messages
            else:
                max_number_of_messages = max_number_of_messages_parameter - len(messages)  # how many left to do

            try:

                aws_messages = self._get_queue().receive_messages(
                    MaxNumberOfMessages=min(max_number_of_messages, aws_sqs_max_messages), VisibilityTimeout=self.calculate_visibility_timeout(), WaitTimeSeconds=call_wait_time
                )

                for m in aws_messages:
                    if self.immediate_delete:
                        m.delete()
                    elif self.user_provided_timeout is None:

                        #  keep history of message processing times for user deletes, by AWS's message id
                        self.response_history[m.message_id] = [time.time(), None]  # start (finish will be filled in upon delete)

                        # if history is too large, delete the oldest
                        while len(self.response_history) > self.max_history:
                            oldest = None
                            for handle, start_finish in self.response_history.items():
                                if oldest is None or start_finish[0] < self.response_history[oldest][0]:
                                    oldest = handle
                            del self.response_history[oldest]

                    messages.append(SQSMessage(m.body, m, self))

            except ClientError as e:
                # should happen very infrequently
                log.warning(f"{self.queue_name=} {e}")

            call_wait_time = 0  # now, short polls

            if aws_messages is None or len(aws_messages) == 0 or (max_number_of_messages_parameter is not None and len(messages) >= max_number_of_messages_parameter):
                continue_to_receive = False

        return messages

    @typechecked()
    def receive_message(self) -> Union[SQSMessage, None]:
        """
        receive SQS message from this queue
        :return: one SQSMessage if one available, else None
        """

        messages = self._receive(1)
        message_count = len(messages)
        if message_count == 0:
            message = None
        elif message_count == 1:
            message = messages[0]
        else:
            raise RuntimeError(f"{message_count=}")
        return message

    @typechecked()
    def receive_messages(self, max_messages: int = None) -> List[SQSMessage]:
        """
        receive a (possibly empty) list of SQS messages from this queue

        :param max_messages: maximum number of messages to receive (None for all available messages)
        :return: list of messages
        """
        return self._receive(max_messages)

    def _update_response_history(self, message_id: str):
        """
        update response history

        :param message_id: message ID
        """
        # update response history
        if not self.immediate_delete and self.user_provided_timeout is None and message_id in self.response_history:
            self.response_history[message_id][1] = time.time()  # set finish time

            # save to file
            file_path = self._get_response_history_file_path()
            file_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(self._get_response_history_file_path(), "w") as f:
                    json.dump(self.response_history, f, indent=4)
            except IOError as e:
                log.info(f'"{file_path}" : {e}')

    @typechecked()
    def send(self, message: str):
        """
        send SQS message

        :param message: message string
        """
        self._get_queue().send_message(MessageBody=message)

    @typechecked()
    def get_arn(self) -> str:
        """
        get SQS ARN

        :return: ARN string
        """
        return self._get_queue().attributes["QueueArn"]

    @typechecked()
    def add_permission(self, source_arn: str):
        """
        allow source (e.g. SNS topic) to send to this SQS queue

        :param source_arn: source arn (e.g. SNS queue arn)

        """

        # a little brute-force, but this is the only way I could assign SQS policy to accept messages from SNS
        policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "SQS:SendMessage", "Resource": self.get_arn(), "Condition": {"StringEquals": {"aws:SourceArn": source_arn}}}],
        }

        policy_string = json.dumps(policy)
        log.info(f"{policy_string=}")
        self.client.set_queue_attributes(QueueUrl=self._get_queue().url, Attributes={"Policy": policy_string})

    def purge(self):
        """
        purge all messages in the queue
        """
        self.client.purge_queue(QueueUrl=self._get_queue().url)

    def messages_available(self) -> int:
        """
        return number of messages available
        :return: number of messages available
        """
        key = "ApproximateNumberOfMessages"
        response = self.client.get_queue_attributes(QueueUrl=self._get_queue().url, AttributeNames=[key])
        number_of_messages_available = int(response["Attributes"][key])
        return number_of_messages_available


class SQSPollAccess(SQSAccess):
    def __init__(self, queue_name: str, **kwargs):
        super().__init__(queue_name, **kwargs)
        self.sqs_call_wait_time = aws_sqs_long_poll_max_wait_time
