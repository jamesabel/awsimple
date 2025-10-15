from awsimple.pubsub import make_name_aws_safe


def test_pubsub_make_name_aws_safe():

    assert make_name_aws_safe("My Topic Name!") == "lc6dwk3bn32n6upqpos4ryluxluxsvd"
    assert make_name_aws_safe("Topic@123") == "jwxskzojw9o2717rs24rtwqgy50v4yn"
    assert make_name_aws_safe("with.a.dot") == "boo0z8dyxiirijsg69qg4g2yg3tsd7w"
    assert make_name_aws_safe("a_6.3") == "d4gg6yrpd2pieqhchpz2qlc7a52shxy"
    assert make_name_aws_safe("-5") == "dk1zux1ndufnok5x2v4uj3flwtzxpr6"
    assert make_name_aws_safe("0") == "lasw0dw8dpjcoalne79l4km57y91kwc"
    assert make_name_aws_safe("Valid_Name-123") == "5fpq80the6qly6weat39tnh57x8bjhm"
    assert make_name_aws_safe("Invalid#Name$With%Special&Chars*") == "jbudqy4oq2aqhcxgs40b0nmahou3pgq"

    assert make_name_aws_safe("ab") == "phbce4exwcst2t3d0hqd8k81nc27kd8"
    assert make_name_aws_safe("a", "b") == "phbce4exwcst2t3d0hqd8k81nc27kd8"
