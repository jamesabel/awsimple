# awsimple

Simple API for basic AWS services

Usage

    pip install awsimple

Examples

    s = S3Access(profile_name="testawsimple", bucket="testawsimple").read_string("helloworld.txt")
    print(s)

Introduction

`awsimple` is a simple interface into basic AWS services such as S3 (Simple Storage Service) and
DynamoDB (a simple NoSQL database).
