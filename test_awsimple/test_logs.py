from awsimple import LogsAccess

from test_awsimple import test_awsimple_str


def test_logs():

    logs_access = LogsAccess(test_awsimple_str)
    logs_access.put("my first log test")
    logs_access.put("my second log test")

    logs_access = LogsAccess(test_awsimple_str)
    logs_access.put("my third log test")
    logs_access.put("my forth log test")
