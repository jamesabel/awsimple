from pathlib import Path

from awsimple import get_disk_free, get_directory_size


def test_disk_free():
    free = get_disk_free()
    print(f"{free=:,}")
    assert free > 1E9  # assume we have some reasonable amount free


def test_get_directory_size():
    size = get_directory_size(Path("venv"))  # just use the venv as something that's relatively large and multiple directory levels
    print(f"{size=:,}")
    assert size >= 50000000  # 94302709 on 8/21/20, so assume it's not going to get a lot smaller
