from pathlib import Path

from awsimple import get_disk_free, get_directory_size, is_mock


def test_disk_free():
    free = get_disk_free()
    print(f"{free=:,}")
    assert free > 1e9  # assume we have some reasonable amount free


def test_get_directory_size():
    venv = Path("venv")
    if venv.exists():
        # doesn't work with Linux CI
        size = get_directory_size(venv)  # just use the venv as something that's relatively large and multiple directory levels
        print(f"{size=:,}")
        assert size >= 50000000  # 94,302,709 on 8/21/20, so assume it's not going to get a lot smaller
