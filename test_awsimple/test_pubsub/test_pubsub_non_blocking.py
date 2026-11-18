"""
Creating and using Pub and Sub objects must not block the caller: all AWS work is done by the object's own thread.

All tests in this file are AI-generated (Claude Code); the private helpers, fixtures and constants below are covered by this note.

AWS is made slow for these tests: creating any AWS access object (which every use of AWS starts with) takes _AWS_DELAY
seconds. A call that used AWS in the caller's thread would therefore take at least that long, well over the _CALL_LIMIT it
is checked against. join() is not checked: it waits for the thread, by design.
"""

import time
from typing import Any, Callable

import pytest

from awsimple import AWSAccess, Pub, Sub, SNSAccess
from awsimple.pubsub import make_name_aws_safe

_AWS_DELAY = 1.0  # seconds added to each creation of an AWS access object
_CALL_LIMIT = _AWS_DELAY / 2.0  # seconds a non-blocking call may take (generous, to tolerate a busy test machine)
_TIMEOUT = 120.0  # seconds to wait for a thread to finish (its AWS setup is slow here)


def _timed(description: str, function: Callable, *args, **kwargs) -> Any:
    """
    Call the function and assert that it returned quickly, i.e. it did not wait for AWS.
    """
    start = time.perf_counter()
    result = function(*args, **kwargs)
    duration = time.perf_counter() - start
    assert duration < _CALL_LIMIT, f"{description} blocked for {duration:.3f} seconds (limit {_CALL_LIMIT} seconds)"
    return result


def _exit(*nodes: Pub | Sub) -> None:
    for node in nodes:
        _timed(f"{type(node).__name__}.request_exit()", node.request_exit)
    for node in nodes:
        node.join(_TIMEOUT)
        assert not node.is_alive()
        _timed(f"{type(node).__name__}.raise_if_failed() after join()", node.raise_if_failed)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.fixture
def slow_aws(monkeypatch):
    original_init = AWSAccess.__init__

    def slow_init(self, *args, **kwargs):
        time.sleep(_AWS_DELAY)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(AWSAccess, "__init__", slow_init)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_timing_check_detects_blocking(slow_aws):
    # the check itself: something that does use AWS in the caller's thread is reported as blocking
    with pytest.raises(AssertionError, match="blocked"):
        _timed("SNSAccess()", SNSAccess, "test_non_blocking_check")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_pub_does_not_block(slow_aws):
    pub = _timed("Pub()", Pub, "test_non_blocking_pub", "non_blocking_node")
    _timed("Pub.raise_if_failed() before start()", pub.raise_if_failed)
    _timed("Pub.start()", pub.start)
    _timed("Pub.publish()", pub.publish, {"number": 1})
    _timed("Pub.raise_if_failed()", pub.raise_if_failed)
    for number in range(100):
        _timed("Pub.publish() in a burst", pub.publish, {"number": number})
    _exit(pub)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_sub_get_messages_does_not_block(slow_aws):
    sub = _timed("Sub()", Sub, "test_non_blocking_sub", "non_blocking_node")
    _timed("Sub.raise_if_failed() before start()", sub.raise_if_failed)
    _timed("Sub.start()", sub.start)
    assert _timed("Sub.get_messages()", sub.get_messages) == []  # returns at once when there is nothing to get
    _timed("Sub.publish()", sub.publish, {"number": 1})  # a Sub can publish too
    _timed("Sub.raise_if_failed()", sub.raise_if_failed)
    _exit(sub)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_sub_callback_does_not_block(slow_aws):
    received = []  # type: list[dict]
    sub = _timed("Sub() with a callback", Sub, "test_non_blocking_sub_callback", "non_blocking_node", sub_callback=received.append)
    _timed("Sub.start() with a callback", sub.start)
    _timed("Sub.raise_if_failed() with a callback", sub.raise_if_failed)
    _exit(sub)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_pub_and_sub_together_do_not_block(slow_aws):
    channel = "test_non_blocking_pub_and_sub"
    pub = _timed("Pub()", Pub, channel, "non_blocking_pub_node")
    sub = _timed("Sub()", Sub, channel, "non_blocking_sub_node")
    _timed("Pub.start()", pub.start)
    _timed("Sub.start()", sub.start)

    # keep calling while both threads are busy with their (slow) AWS setup and then with sending and receiving
    received = []  # type: list[dict]
    deadline = time.time() + _TIMEOUT
    while len(received) == 0 and time.time() < deadline:
        _timed("Pub.publish() during AWS activity", pub.publish, {"number": 1})
        received = _timed("Sub.get_messages() during AWS activity", sub.get_messages)
        time.sleep(0.2)
    assert len(received) > 0

    _exit(pub, sub)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_make_name_aws_safe_does_not_block(slow_aws):
    _timed("make_name_aws_safe()", make_name_aws_safe, "test_non_blocking_name", "non_blocking_node")
