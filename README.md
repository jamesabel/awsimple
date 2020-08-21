# awsimple

Simple API for basic AWS services

## Usage

    pip install awsimple

## Examples

    # print string contents of an existing S3 object
    s = S3Access(profile_name="testawsimple", bucket="testawsimple").read_string("helloworld.txt")
    print(s)

## Introduction

`awsimple` is a simple interface into basic AWS services such as S3 (Simple Storage Service) and
DynamoDB (a simple NoSQL database).  It has a set of higher level default settings and behavior
that should cover many basic usage models.

## Discussion

While AWS has many varied services with extensive flexibility, using it for more straight-forward 
applications is sometimes a daunting task. There are access modes that are probably not requried 
and some default behaviors are not best for common usages.  `awsimple` aimes to create a higher 
level API to S3 and DynamoDB to improve programmer productivity.

## What`awsimple` Is Not

- `awsimple` is not necessarily memory and CPU efficient

- `awsimple` does not provide cost monitoring hooks

- `awsimple` is not particularly flexible
