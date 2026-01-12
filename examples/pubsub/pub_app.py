from awsimple import Pub

pub = Pub("awsimple_pubsub_example")
pub.start()
while input_message := input(f"Message to publish (or Enter to exit):").strip():
    pub.publish({"message": input_message})
pub.request_exit()
