use codecrafters_redis::resp_parser::*;
use codecrafters_redis::frame::Frame;

#[test]
fn test_valid_booleans() {
    // Basic true value
    assert_eq!(parse_boolean(b"#t\r\n").unwrap().0, Frame::Boolean(true));

    // Basic false value
    assert_eq!(
        parse_boolean(b"#f\r\n").unwrap().0,
        Frame::Boolean(false)
    );
}

#[test]
fn test_invalid_booleans() {
    // Wrong data type marker
    assert_eq!(
        parse_boolean(b":t\r\n").err().unwrap().message(),
        "ERR Invalid data type"
    );

    // Invalid boolean value
    assert_eq!(
        parse_boolean(b"#x\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Missing \r\n terminator
    assert_eq!(
        parse_boolean(b"#t").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Only \r without \n
    assert_eq!(
        parse_boolean(b"#t\r").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Empty input
    assert_eq!(
        parse_boolean(b"").err().unwrap().message(),
        "ERR Empty data"
    );

    // Just the marker
    assert_eq!(
        parse_boolean(b"#").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Case sensitivity
    assert_eq!(
        parse_boolean(b"#T\r\n").err().unwrap().message(),
        "ERR invalid value"
    );

    // Extra content
    assert_eq!(
        parse_boolean(b"#ttrue\r\n").err().unwrap().message(),
        "ERR Unexpected end of input"
    );
}

#[test]
fn test_boolean_remaining_bytes() {
    // Test with remaining data
    let (value, remaining) = parse_boolean(b"#t\r\n+OK\r\n").unwrap();
    assert_eq!(value, Frame::Boolean(true));
    assert_eq!(remaining, b"+OK\r\n");

    // Test with no remaining data
    let (value, remaining) = parse_boolean(b"#f\r\n").unwrap();
    assert_eq!(value, Frame::Boolean(false));
    assert_eq!(remaining, b"");

    // Test with multiple commands
    let (value, remaining) = parse_boolean(b"#t\r\n:42\r\n").unwrap();
    assert_eq!(value, Frame::Boolean(true));
    assert_eq!(remaining, b":42\r\n");

    // Test with false and remaining data
    let (value, remaining) = parse_boolean(b"#f\r\n-ERR test\r\n").unwrap();
    assert_eq!(value, Frame::Boolean(false));
    assert_eq!(remaining, b"-ERR test\r\n");
}
