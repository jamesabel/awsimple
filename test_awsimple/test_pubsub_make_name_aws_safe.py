import pytest

from awsimple.pubsub import make_name_aws_safe


def test_pubsub_make_name_aws_safe():

    assert make_name_aws_safe("My Topic Name!") == "mytopicname"
    assert make_name_aws_safe("Topic@123") == "topic123"
    assert make_name_aws_safe("with.a.dot") == "withadot"
    assert make_name_aws_safe("a_6.3") == "a63"
    assert make_name_aws_safe("-5") == "5"
    assert make_name_aws_safe("0") == "0"
    assert make_name_aws_safe("Valid_Name-123") == "validname123"
    assert make_name_aws_safe("Invalid#Name$With%Special&Chars*") == "invalidnamewithspecialchars"


def test_pubsub_make_name_aws_safe_empty():
    with pytest.raises(ValueError):
        assert make_name_aws_safe("!!!") == ""

    with pytest.raises(ValueError):
        assert make_name_aws_safe(".") == ""
