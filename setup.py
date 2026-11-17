from setuptools import setup

from awsimple.__version__ import __version__, __title__, __author__, __author_email__, __url__, __download_url__, __description__

readme_file_path = "README.md"  # exact case: the build runs on Linux, where the file system is case sensitive

with open(readme_file_path, encoding="utf-8") as f:
    long_description = "\n" + f.read()

setup(
    name=__title__,
    description=__description__,
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=__version__,
    author=__author__,
    author_email=__author_email__,
    license="MIT License",
    url=__url__,
    download_url=__download_url__,
    keywords=["aws", "cloud", "storage", "database", "dynamodb", "s3"],
    packages=[__title__],
    package_data={__title__: ["py.typed"]},
    install_requires=["boto3[crt]>=1.43.0", "typeguard", "hashy>=0.14.0", "dictim", "appdirs", "tobool", "urllib3", "python-dateutil", "yasf", "strif"],  # boto3[crt]>=1.43.0 and hashy>=0.14.0 for CRC64NVME checksum support
    project_urls={"Documentation": "https://jamesabel.github.io/awsimple/"},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Typing :: Typed",
    ],
    python_requires=">=3.12",
)
