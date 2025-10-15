import time
from multiprocessing import Queue
from queue import Empty

from awsimple import Pub, Sub


def test_pubsub_callback():

    test_channel = "test_channel"
    sent_message = {"number": 1}

    message_queue = Queue()

    pub = Pub(test_channel)
    pub.start()

    sub = Sub(test_channel, sub_callback=message_queue.put)  # the callback puts messages in the queue
    sub.start()

    pub.publish(sent_message)

    # test the callback
    count = 100
    message_from_queue = None
    while message_from_queue is None and count > 0:
        try:
            message_from_queue = message_queue.get(timeout=10)
        except Empty:
            time.sleep(1)
        count -= 1
    assert message_from_queue == sent_message

    pub.request_exit()
    sub.request_exit()

    pub.join(60)
    assert not pub.is_alive()
    sub.join(60)
    assert not sub.is_alive()
