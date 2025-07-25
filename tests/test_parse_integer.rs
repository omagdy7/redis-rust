use codecrafters_redis::resp_parser::*;

#[test]
fn test_valid_integers() {
    // Basic valid cases
    assert_eq!(parse_integers(b":0\r\n").unwrap().0, 0u64);
    assert_eq!(parse_integers(b":1\r\n").unwrap().0, 1u64);
    assert_eq!(parse_integers(b":42\r\n").unwrap().0, 42u64);
    assert_eq!(parse_integers(b":1000\r\n").unwrap().0, 1000u64);

    assert_eq!(parse_integers(b":+42\r\n").unwrap().0, 42u64);

    // Large numbers
    assert_eq!(
        parse_integers(b":9223372036854775807\r\n").unwrap().0,
        9223372036854775807u64
    );
    assert_eq!(
        parse_integers(b":18446744073709551615\r\n").unwrap().0,
        18446744073709551615u64
    ); // u64::MAX

    // Edge cases
    assert_eq!(parse_integers(b":123456789\r\n").unwrap().0, 123456789u64);

    // Numbers with leading zeros (should still parse correctly)
    assert_eq!(parse_integers(b":0000042\r\n").unwrap().0, 42u64);
    assert_eq!(parse_integers(b":00000\r\n").unwrap().0, 0u64);
}

#[test]
fn test_invalid_integers() {
    // Wrong data type marker
    assert_eq!(
        parse_integers(b"+42\r\n").err().unwrap().message(),
        "ERR Invalid data type"
    );

    // Negative numbers (not valid for u64)
    assert_eq!(
        parse_integers(b":-42\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Non-numeric content
    assert_eq!(
        parse_integers(b":abc\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Mixed numeric and non-numeric
    assert_eq!(
        parse_integers(b":42abc\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Empty integer
    assert_eq!(
        parse_integers(b":\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Contains \r in content
    assert_eq!(
        parse_integers(b":42\r23\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Contains \n in content
    assert_eq!(
        parse_integers(b":42\n23\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Missing \r\n terminator
    assert_eq!(
        parse_integers(b":42").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Only \r without \n
    assert_eq!(
        parse_integers(b":42\r").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Only \n without \r
    assert_eq!(
        parse_integers(b":42\n").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Empty input
    assert_eq!(
        parse_integers(b"").err().unwrap().message(),
        "ERR Empty data"
    );

    // Just the marker without content
    assert_eq!(
        parse_integers(b":").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Number too large for u64
    assert_eq!(
        parse_integers(b":18446744073709551616\r\n") // u64::MAX + 1
            .err()
            .unwrap()
            .message(),
        "ERR invalid value"
    );

    // Floating point numbers
    assert_eq!(
        parse_integers(b":42.5\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Scientific notation
    assert_eq!(
        parse_integers(b":1e5\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Hexadecimal numbers
    assert_eq!(
        parse_integers(b":0x42\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Whitespace
    assert_eq!(
        parse_integers(b": 42\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    assert_eq!(
        parse_integers(b":42 \r\n").err().unwrap().message(),
        "ERR invalid value"
    );
}

#[test]
fn test_integer_remaining_bytes() {
    // Test that remaining bytes are correctly returned
    let (integer, remaining) = parse_integers(b":42\r\nnext data").unwrap();
    assert_eq!(integer, 42u64);
    assert_eq!(remaining, b"next data");

    // Test with multiple commands
    let (integer, remaining) = parse_integers(b":1337\r\n+OK\r\n").unwrap();
    assert_eq!(integer, 1337u64);
    assert_eq!(remaining, b"+OK\r\n");

    // Test with no remaining data
    let (integer, remaining) = parse_integers(b":999\r\n").unwrap();
    assert_eq!(integer, 999u64);
    assert_eq!(remaining, b"");

    // Test with zero and remaining data
    let (integer, remaining) = parse_integers(b":0\r\n-ERR test\r\n").unwrap();
    assert_eq!(integer, 0u64);
    assert_eq!(remaining, b"-ERR test\r\n");
}
