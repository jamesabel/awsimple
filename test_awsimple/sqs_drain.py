import time
from pprint import pprint

from awsimple import SQSAccess

from test_awsimple import test_awsimple_str


def drain():

    # drain existing messages
    q = SQSAccess(test_awsimple_str, profile_name=test_awsimple_str)
    q.create_queue()  # just in case it doesn't exist
    while len(messages := q.receive_messages()) > 0:
        print("existing:")
        pprint(messages)
        time.sleep(0.1)
    print()
