import time

from awsimple import DynamoDBMIVUI, miv_string, get_time_us, miv_us_to_timestamp


def test_dynamodb_miv_ui():

    test_name = "test_dynamodb_miv_ui"
    primary_partition_key = "id"
    id_value = "me"
    input_data = {primary_partition_key: id_value}

    dynamodb_miv_ui = DynamoDBMIVUI(test_name)
    dynamodb_miv_ui.create_table(primary_partition_key)  # use default of str
    dynamodb_miv_ui.put_item(input_data)
    dynamodb_miv_ui.put_item(input_data)
    output_data = dynamodb_miv_ui.get_most_senior_item(primary_partition_key, id_value)
    print(output_data)
    assert output_data[primary_partition_key] == id_value
    miv_value = output_data[miv_string]
    assert miv_value <= get_time_us()  # basic check for miv value
    difference = time.time() - miv_us_to_timestamp(miv_value)
    print(f"{difference=} seconds")
    assert 0 < difference < 100  # check that we can convert the MIV back to time in seconds since epoch
