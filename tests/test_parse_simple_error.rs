use codecrafters_redis::resp_parser::*;

#[test]
fn test_valid_simple_errors() {
    // Basic valid cases
    assert_eq!(
        parse_simple_errors(b"-ERR unknown command\r\n").unwrap().0,
        "ERR unknown command"
    );
    assert_eq!(
        parse_simple_errors(b"-WRONGTYPE\r\n").unwrap().0,
        "WRONGTYPE"
    );
    assert_eq!(
        parse_simple_errors(b"-ERR syntax error\r\n").unwrap().0,
        "ERR syntax error"
    );

    // Empty error string
    assert_eq!(parse_simple_errors(b"-\r\n").unwrap().0, "");

    // Error with spaces and special characters (but no \r or \n)
    assert_eq!(
        parse_simple_errors(b"-ERR invalid key: 'test123'\r\n")
            .unwrap()
            .0,
        "ERR invalid key: 'test123'"
    );

    // Error with various ASCII characters
    assert_eq!(
        parse_simple_errors(b"-ERR !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ \r\n")
            .unwrap()
            .0,
        "ERR !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ "
    );

    // Unicode characters in error message
    assert_eq!(
        parse_simple_errors(b"-ERR \xc3\xa9\xc3\xa1\xc3\xb1\r\n")
            .unwrap()
            .0,
        "ERR éáñ"
    );

    // Common Redis error patterns
    assert_eq!(
        parse_simple_errors(b"-NOAUTH Authentication required\r\n")
            .unwrap()
            .0,
        "NOAUTH Authentication required"
    );
    assert_eq!(
        parse_simple_errors(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        )
        .unwrap()
        .0,
        "WRONGTYPE Operation against a key holding the wrong kind of value"
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
    assert_eq!(error, "ERR test");
    assert_eq!(remaining, b"next data");

    // Test with multiple commands
    let (error, remaining) = parse_simple_errors(b"-WRONGTYPE\r\n+OK\r\n").unwrap();
    assert_eq!(error, "WRONGTYPE");
    assert_eq!(remaining, b"+OK\r\n");

    // Test with no remaining data
    let (error, remaining) = parse_simple_errors(b"-ERR final\r\n").unwrap();
    assert_eq!(error, "ERR final");
    assert_eq!(remaining, b"");
}
