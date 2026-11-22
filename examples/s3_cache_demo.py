"""
Demonstrates awsimple's S3 cache, and measures what it saves.

The download destination and the cache are new temporary directories that are left in place when the program ends, so you can look at how the cache is made:

- every cached S3 object is one file in the cache directory, named with the SHA-512 of its contents (so S3 objects with the same contents are one file)
- the "index" sub-directory is only for S3 objects that have no SHA-512 (objects that were not written by awsimple): it has a file, named with a hash of the object's
  identity, that holds the SHA-512 awsimple calculated when it downloaded the object

Each step downloads the same S3 objects with S3Access.download_cached() and counts the requests made to S3 and the bytes downloaded from it, and times the step.

Run with real AWS (the default profile, or --profile), which is where the time saved shows:

    python s3_cache_demo.py --bucket my-bucket

or with no AWS account at all, using the moto mock (the counts are the same, but there is no network so the times mean little):

    python s3_cache_demo.py --mock

The S3 objects this writes all have keys that start with "s3_cache_demo/", and are deleted at the end (unless --keep).
"""

import argparse
import getpass
import os
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List

key_prefix = "s3_cache_demo/"
index_dir_name = "index"


@dataclass
class Measurement:
    """What one step of the demonstration did."""

    step: str
    seconds: float
    cache_hits: int
    cache_misses: int
    requests: Counter = field(default_factory=Counter)  # S3 requests made, by operation name
    bytes_downloaded: int = 0


class S3RequestCounter:
    """Counts the requests an S3Access makes to S3, and the bytes of object contents it downloads."""

    def __init__(self, s3_access):
        self.requests: Counter = Counter()
        self.bytes_downloaded = 0
        # the client (used for transfers and metadata) and the resource (used for reads and writes of whole objects) each make their own requests
        for client in (s3_access.client, s3_access.resource.meta.client):
            client.meta.events.register("before-call.s3", self._before_call)
            client.meta.events.register("after-call.s3.GetObject", self._after_get_object)

    def _before_call(self, model, **_unused_kwargs):
        self.requests[model.name] += 1

    def _after_get_object(self, parsed, **_unused_kwargs):
        self.bytes_downloaded += parsed.get("ContentLength", 0)

    def reset(self):
        self.requests = Counter()
        self.bytes_downloaded = 0


def measure(step: str, counter: S3RequestCounter, downloads: List[Callable]) -> Measurement:
    """
    Do the downloads of one step, and measure them.

    :param step: what the step is
    :param counter: the request counter of the S3Access the downloads use
    :param downloads: the downloads, each of which returns an S3DownloadStatus
    :return: the measurement
    """
    counter.reset()
    start = time.perf_counter()
    statuses = [download() for download in downloads]
    seconds = time.perf_counter() - start
    hits = sum(1 for status in statuses if status.cache_hit)
    return Measurement(step, seconds, hits, len(statuses) - hits, Counter(counter.requests), counter.bytes_downloaded)


def print_measurements(measurements: List[Measurement]):
    """Print the measurements as a table."""
    print()
    print(f"{'step':<46} {'seconds':>8} {'hits':>5} {'misses':>6} {'requests':>8} {'downloaded':>12}  requests by operation")
    for m in measurements:
        by_operation = ", ".join(f"{name} {count}" for name, count in sorted(m.requests.items()))
        print(f"{m.step:<46} {m.seconds:>8.3f} {m.cache_hits:>5} {m.cache_misses:>6} {sum(m.requests.values()):>8} {m.bytes_downloaded:>12,}  {by_operation}")


def print_directory(title: str, directory: Path, names: dict):
    """
    Print the files in a directory and its sub-directories.

    :param title: what the directory is
    :param directory: the directory
    :param names: what some of the files are, by file name
    """
    print()
    print(f"{title} : {directory}")
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            relative = path.relative_to(directory)
            name = str(relative) if len(str(relative)) <= 40 else f"{str(relative)[:28]}...{str(relative)[-9:]}"
            print(f"  {name:<40} {path.stat().st_size:>12,} bytes  {names.get(path.name, '')}")


def print_summary(measurements: List[Measurement], object_count: int, is_mock: bool):
    """
    Print what the measurements show: a warm cache compared to no cache.

    :param measurements: the measurements (the first three are no cache, an empty cache and a warm cache, for the same objects)
    :param object_count: number of S3 objects in those steps
    :param is_mock: True if the moto mock was used
    """
    no_cache, cold, warm = measurements[0], measurements[1], measurements[2]
    print()
    print(f"With a warm cache (step 3), compared to no cache (step 1) for the same {object_count} objects:")
    print(f"  bytes downloaded : {warm.bytes_downloaded:,} instead of {no_cache.bytes_downloaded:,}")
    print(f"  requests to S3   : {sum(warm.requests.values())} instead of {sum(no_cache.requests.values())} (a cache hit is one HeadObject, which is how a changed object is seen)")
    if warm.seconds > 0:
        print(f"  time             : {warm.seconds:.3f} s instead of {no_cache.seconds:.3f} s ({no_cache.seconds / warm.seconds:.1f} times as fast)")
    print(f"  filling the cache (step 2) took {cold.seconds:.3f} s : a download, plus the SHA-512 and the copy into the cache")
    if is_mock:
        print("  (with the moto mock there is no network, so the times do not show what the cache saves with AWS)")


def parse_args() -> argparse.Namespace:
    """
    :return: the command line arguments
    """
    parser = argparse.ArgumentParser(description="Demonstrate and measure awsimple's S3 cache.")
    parser.add_argument("--bucket", default=f"awsimple-test-bucket-{getpass.getuser().lower()}", help="S3 bucket (made if it does not exist). Bucket names are globally unique.")
    parser.add_argument("--profile", default=None, help="AWS profile name (the default profile if not given)")
    parser.add_argument("--mock", action="store_true", help="use the moto mock: no AWS account is needed")
    parser.add_argument("--count", type=int, default=4, help="number of S3 objects")
    parser.add_argument("--size", type=int, default=1024 * 1024, help="size of each S3 object in bytes")
    parser.add_argument("--keep", action="store_true", help="do not delete the S3 objects at the end")
    return parser.parse_args()


def main():
    """
    Run the demonstration.
    """
    args = parse_args()

    if args.mock:
        os.environ["AWSIMPLE_USE_MOTO_MOCK"] = "1"  # before awsimple is used
    from hashy import get_bytes_sha512

    from awsimple import S3Access

    work_dir = Path(tempfile.mkdtemp(prefix="awsimple_s3_cache_demo_"))
    cache_dir = Path(work_dir, "cache")
    dest_dir = Path(work_dir, "dest")
    source_dir = Path(work_dir, "source")
    for directory in (cache_dir, dest_dir, source_dir):
        directory.mkdir()

    s3_access = S3Access(args.bucket, profile_name=args.profile, cache_dir=cache_dir)
    print(f"{'moto mock' if args.mock else 'AWS'} : bucket {args.bucket} : {args.count} objects of {args.size:,} bytes")
    s3_access.create_bucket()
    counter = S3RequestCounter(s3_access)

    # ---- make the S3 objects
    contents = {f"{key_prefix}object_{number}.bin": os.urandom(args.size) for number in range(args.count)}
    file_names = {}  # what each cache file is, by the SHA-512 of the contents
    for s3_key, content in contents.items():
        source_path = Path(source_dir, Path(s3_key).name)
        source_path.write_bytes(content)
        s3_access.upload(source_path, s3_key, force=True)  # awsimple writes the SHA-512 of the contents with every object
        file_names[get_bytes_sha512(content)] = f"contents of {s3_key}"
    first_key = next(iter(contents))

    # the same contents as the first object, under another key
    same_contents_key = f"{key_prefix}same_contents_as_object_0.bin"
    s3_access.upload(Path(source_dir, Path(first_key).name), same_contents_key, force=True)
    file_names[get_bytes_sha512(contents[first_key])] += f" and {same_contents_key}"

    # an object written by another tool: it has no SHA-512, so awsimple finds it in the cache through the index
    other_tool_key = f"{key_prefix}written_by_another_tool.bin"
    other_tool_content = os.urandom(args.size)
    s3_access.client.put_object(Bucket=args.bucket, Key=other_tool_key, Body=other_tool_content, ChecksumAlgorithm="CRC64NVME")
    file_names[get_bytes_sha512(other_tool_content)] = f"contents of {other_tool_key}"
    file_names[s3_access.get_s3_object_metadata(other_tool_key).get_identity_sha512()] = f"the SHA-512 of the contents of {other_tool_key}"

    def cached(s3_key: str) -> Callable:
        return lambda: s3_access.download_cached(s3_key, Path(dest_dir, Path(s3_key).name))

    def not_cached(s3_key: str) -> Callable:
        def _download():
            s3_access.download(s3_key, Path(dest_dir, Path(s3_key).name))
            return s3_access.download_status.__class__(success=True, cache_hit=False)

        return _download

    # ---- the steps
    all_keys = list(contents)
    measurements = [
        measure("1. download(), no cache", counter, [not_cached(s3_key) for s3_key in all_keys]),
        measure("2. download_cached(), empty cache", counter, [cached(s3_key) for s3_key in all_keys]),
        measure("3. download_cached() again", counter, [cached(s3_key) for s3_key in all_keys]),
        measure("4. another key with the same contents", counter, [cached(same_contents_key)]),
        measure("5. object from another tool, first time", counter, [cached(other_tool_key)]),
        measure("6. object from another tool, again", counter, [cached(other_tool_key)]),
    ]

    # one object is written again with new contents: only that one is downloaded
    changed_content = os.urandom(args.size)
    Path(source_dir, Path(first_key).name).write_bytes(changed_content)
    s3_access.upload(Path(source_dir, Path(first_key).name), first_key)
    file_names[get_bytes_sha512(changed_content)] = f"new contents of {first_key}"
    measurements.append(measure("7. after one object was written again", counter, [cached(s3_key) for s3_key in all_keys]))
    assert Path(dest_dir, Path(first_key).name).read_bytes() == changed_content  # a changed S3 object is never served from the cache

    # a cache file is damaged: awsimple sees that it does not have the SHA-512 it is named with, and downloads the object
    damaged_key = all_keys[-1]
    damaged_path = Path(cache_dir, get_bytes_sha512(contents[damaged_key]))
    damaged_path.write_bytes(bytes(args.size))
    print("(step 8 damages a cache file on purpose: awsimple logs a warning about it)")
    measurements.append(measure("8. after a cache file was damaged", counter, [cached(damaged_key)]))
    assert Path(dest_dir, Path(damaged_key).name).read_bytes() == contents[damaged_key]

    print_measurements(measurements)

    print_summary(measurements, args.count, args.mock)

    # ---- how the cache is made
    print_directory("cache directory", cache_dir, file_names)
    print_directory("download destination", dest_dir, {})
    print()
    print("In the cache directory: each file is the contents of an S3 object, named with the SHA-512 of the contents. S3 objects with the same contents are one file.")
    print(f'The "{index_dir_name}" sub-directory is only for S3 objects that have no SHA-512: it holds the SHA-512 awsimple calculated, in a file named with a hash of')
    print("the identity of the S3 object (bucket, key, size, modification time, ETag and checksums).")
    print(f"These directories are left in place so you can look at them : {work_dir}")

    if not args.keep:
        for s3_key in s3_access.keys(key_prefix):
            s3_access.delete_object(s3_key)
        print(f'deleted the S3 objects with the prefix "{key_prefix}" from {args.bucket}')


if __name__ == "__main__":
    main()
