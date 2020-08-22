from awsimple import get_disk_free


def test_disk_free():
    assert get_disk_free() > 1E9  # assume we have some reasonable amount free
