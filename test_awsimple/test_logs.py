from awsimple import LogsAccess, is_mock

from test_awsimple import test_awsimple_str


def test_logs():

    # mock seems to have a problem mocking logs
    logs_access = LogsAccess(test_awsimple_str)
    logs_access.put("my first log test")
    logs_access.put("my second log test")
