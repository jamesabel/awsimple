import pytest

from ismain import is_main

from awsimple import AWSAccess


def test_get_account_id():

    with pytest.raises(NotImplementedError):
        aws_access = AWSAccess()
        account_id = aws_access.get_account_id()
        assert len(account_id) >= 12  # currently all account IDs are 12 numeric digits, but allow for them to increase in size (but still be only digits)
        assert account_id.isdigit()
        print(account_id)


if is_main():
    test_get_account_id()
