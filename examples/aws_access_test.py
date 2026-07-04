from awsimple import AWSAccess

# In this example we're using the default profile
print(AWSAccess().test())  # 'True' if your default-profile AWS credentials are valid, 'False' if not
