from awsimple import Sub

sub = Sub("awsimple_pubsub_example", sub_callback=print)
sub.start()
input("Press Enter to exit...\n")
sub.request_exit()
