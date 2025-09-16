"""
SNS Access
"""

from typing import Union, Dict, Any

from typeguard import typechecked

from awsimple import AWSAccess


class SNSAccess(AWSAccess):
    @typechecked()
    def __init__(self, topic_name: str, auto_create: bool = False, **kwargs):
        """
        SNS Access

        :param topic_name: SNS topic
        :param kwargs: kwargs
        """
        super().__init__(resource_name="sns", **kwargs)
        self.topic_name = topic_name
        self.auto_create = auto_create

    def get_topic(self) -> Any:
        """
        gets the associated SNS Topic instance

        :return: sns.Topic instance
        """
        topic = None
        for t in self.resource.topics.all():
            if t.arn.split(":")[-1] == self.topic_name:
                topic = t
        if self.auto_create and topic is None:
            self.create_topic()
            self.auto_create = False  # only do this once
            topic = self.get_topic()
        return topic

    @typechecked()
    def get_arn(self) -> str:
        """
        get topic ARN from topic name

        :return: topic ARN
        """
        return self.get_topic().arn

    @typechecked()
    def create_topic(self) -> str:
        """
        create an SNS topic

        :return: the SNS topic's arn
        """
        response = self.client.create_topic(Name=self.topic_name, Attributes={"DisplayName": self.topic_name})
        # todo: see if there are any waiters for SNS topic creation
        # https://stackoverflow.com/questions/50818327/aws-sns-and-waiter-functions-for-boto3
        return response["TopicArn"]

    def delete_topic(self):
        """
        delete SNS topic

        """
        self.client.delete_topic(TopicArn=self.get_arn())

    @typechecked()
    def subscribe(self, subscriber: str) -> str:
        """
        Subscribe to an SNS topic

        :param subscriber: email
        :return: subscription ARN
        """
        if "@" in subscriber:
            # email
            endpoint = subscriber
            protocol = "email"
        else:
            raise ValueError(f"{subscriber=}")
        response = self.client.subscribe(TopicArn=self.get_arn(), Protocol=protocol, Endpoint=endpoint, ReturnSubscriptionArn=True)
        return response["SubscriptionArn"]

    @typechecked()
    def publish(self, message: str, subject: Union[str, None] = None, attributes: Union[dict, None] = None) -> str:
        """
        publish to an existing SNS topic

        :param message: message string
        :param subject: subject string
        :param attributes: message attributes (see AWS SNS documentation on SNS MessageAttributes)
        :return: message ID
        """
        topic = self.get_topic()
        kwargs = {"Message": message}  # type: Dict[str, Union[str, dict]]
        if subject is not None:
            kwargs["Subject"] = subject
        if attributes is not None:
            kwargs["MessageAttributes"] = attributes
        response = topic.publish(**kwargs)
        return response["MessageId"]
