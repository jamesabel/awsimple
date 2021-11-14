def test_s3_transfer_lines(s3_access):
    s3_key = "a"
    lines = ["1", "2"]
    s3_access.write_lines(lines, s3_key)
    read_lines = s3_access.read_lines(s3_key)
    assert lines == read_lines
