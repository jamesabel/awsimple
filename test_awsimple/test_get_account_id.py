from ismain import is_main

from awsimple import AWSAccess, is_mock


def test_get_account_id():
    if not is_mock():
        # todo: get this to work with mocking
        aws_access = AWSAccess()
        account_id = aws_access.get_account_id()
        assert len(account_id) >= 12  # currently all account IDs are 12 numeric digits, but allow for them to increase in size (but still be only digits)
        assert account_id.isdigit()
        print(account_id)


if is_main():
    test_get_account_id()
