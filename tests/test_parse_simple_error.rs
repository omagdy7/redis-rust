use codecrafters_redis::frame::Frame;
use codecrafters_redis::parser::*;

#[test]
fn test_valid_simple_errors() {
    // Basic valid cases
    assert_eq!(
        parse_simple_errors(b"-ERR unknown command\r\n").unwrap().0,
        Frame::SimpleError("ERR unknown command".to_string())
    );
    assert_eq!(
        parse_simple_errors(b"-WRONGTYPE\r\n").unwrap().0,
        Frame::SimpleError("WRONGTYPE".to_string())
    );
    assert_eq!(
        parse_simple_errors(b"-ERR syntax error\r\n").unwrap().0,
        Frame::SimpleError("ERR syntax error".to_string())
    );

    // Empty error string
    assert_eq!(
        parse_simple_errors(b"-\r\n").unwrap().0,
        Frame::SimpleError("".to_string())
    );

    // Error with spaces and special characters (but no \r or \n)
    assert_eq!(
        parse_simple_errors(b"-ERR invalid key: 'test123'\r\n")
            .unwrap()
            .0,
        Frame::SimpleError("ERR invalid key: 'test123'".to_string())
    );

    // Error with various ASCII characters
    assert_eq!(
        parse_simple_errors(b"-ERR !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ \r\n")
            .unwrap()
            .0,
        Frame::SimpleError("ERR !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ ".to_string())
    );

    // Unicode characters in error message
    assert_eq!(
        parse_simple_errors(b"-ERR \xc3\xa9\xc3\xa1\xc3\xb1\r\n")
            .unwrap()
            .0,
        Frame::SimpleError("ERR éáñ".to_string())
    );

    // Common Redis error patterns
    assert_eq!(
        parse_simple_errors(b"-NOAUTH Authentication required\r\n")
            .unwrap()
            .0,
        Frame::SimpleError("NOAUTH Authentication required".to_string())
    );
    assert_eq!(
        parse_simple_errors(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
        .unwrap()
        .0,
        Frame::SimpleError(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
        )
    );
}

#[test]
fn test_invalid_simple_errors() {
    // Wrong data type marker
    assert_eq!(
        parse_simple_errors(b"+OK\r\n").err().unwrap().message(),
        "ERR Invalid data type"
    );

    // Contains \r in content
    assert_eq!(
        parse_simple_errors(b"-ERR invalid\r character\r\n")
            .err()
            .unwrap()
            .message(),
        "ERR invalid value"
    );

    // Contains \n in content
    assert_eq!(
        parse_simple_errors(b"-ERR invalid\n character\r\n")
            .err()
            .unwrap()
            .message(),
        "ERR invalid value"
    );

    // Missing \r\n terminator
    assert_eq!(
        parse_simple_errors(b"-ERR no terminator")
            .err()
            .unwrap()
            .message(),
        "ERR Unexpected end of input"
    );

    // Only \r without \n
    assert_eq!(
        parse_simple_errors(b"-ERR only carriage return\r")
            .err()
            .unwrap()
            .message(),
        "ERR Unexpected end of input"
    );

    // Only \n without \r
    assert_eq!(
        parse_simple_errors(b"-ERR only newline\n")
            .err()
            .unwrap()
            .message(),
        "ERR Unexpected end of input"
    );

    // Empty input
    assert_eq!(
        parse_simple_errors(b"").err().unwrap().message(),
        "ERR Empty data"
    );

    // Just the marker without content
    assert_eq!(
        parse_simple_errors(b"-").err().unwrap().message(),
        "ERR Unexpected end of input"
    );
}

#[test]
fn test_simple_error_remaining_bytes() {
    // Test that remaining bytes are correctly returned
    let (error, remaining) = parse_simple_errors(b"-ERR test\r\nnext data").unwrap();
    assert_eq!(error, Frame::SimpleError("ERR test".to_string()));
    assert_eq!(remaining, b"next data");

    // Test with multiple commands
    let (error, remaining) = parse_simple_errors(b"-WRONGTYPE\r\n+OK\r\n").unwrap();
    assert_eq!(error, Frame::SimpleError("WRONGTYPE".to_string()));
    assert_eq!(remaining, b"+OK\r\n");

    // Test with no remaining data
    let (error, remaining) = parse_simple_errors(b"-ERR final\r\n").unwrap();
    assert_eq!(error, Frame::SimpleError("ERR final".to_string()));
    assert_eq!(remaining, b"");
}
