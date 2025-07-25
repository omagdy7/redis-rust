use codecrafters_redis::resp_parser::*;

#[test]
fn test_valid_simple_strings() {
    // Basic valid cases
    assert_eq!(parse_simple_strings(b"+OK\r\n").unwrap().0, "OK");
    assert_eq!(parse_simple_strings(b"+PONG\r\n").unwrap().0, "PONG");
    assert_eq!(
        parse_simple_strings(b"+Hello World\r\n").unwrap().0,
        "Hello World"
    );

    // Empty string
    assert_eq!(parse_simple_strings(b"+\r\n").unwrap().0, "");

    // String with spaces and special characters (but no \r or \n)
    assert_eq!(
        parse_simple_strings(b"+Hello, World! 123\r\n").unwrap().0,
        "Hello, World! 123"
    );

    // String with various ASCII characters
    assert_eq!(
        parse_simple_strings(b"+!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ \r\n")
            .unwrap()
            .0,
        "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ "
    );

    // Unicode characters (should work with UTF-8)
    assert_eq!(
        parse_simple_strings(b"+\xc3\xa9\xc3\xa1\xc3\xb1\r\n")
            .unwrap()
            .0,
        "éáñ"
    );
}

#[test]
fn test_invalid_prefix() {
    // Missing '+' prefix
    assert_eq!(
        parse_simple_strings(b"OK\r\n").err().unwrap().message(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );

    // Wrong prefix
    assert_eq!(
        parse_simple_strings(b"-Error\r\n").err().unwrap().message(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    assert_eq!(
        parse_simple_strings(b":123\r\n").err().unwrap().message(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    assert_eq!(
        parse_simple_strings(b"$5\r\nhello\r\n")
            .err()
            .unwrap()
            .message(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
}

#[test]
fn test_missing_crlf_terminator() {
    // No CRLF at all
    assert_eq!(
        parse_simple_strings(b"+OK").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Only \r
    assert_eq!(
        parse_simple_strings(b"+OK\r").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Only \n
    assert_eq!(
        parse_simple_strings(b"+OK\n").err().unwrap().message(),
        "ERR Unexpected end of input"
    );

    // Wrong order (\n\r instead of \r\n)
    assert_eq!(
        parse_simple_strings(b"+OK\n\r").err().unwrap().message(),
        "ERR Unexpected end of input"
    );
}

#[test]
fn test_invalid_characters_in_content() {
    // Contains \r in content
    assert_eq!(
        parse_simple_strings(b"+Hello\rWorld\r\n")
            .err()
            .unwrap()
            .message(),
        "ERR invalid value"
    );

    // Contains \n in content
    assert_eq!(
        parse_simple_strings(b"+Hello\nWorld\r\n")
            .err()
            .unwrap()
            .message(),
        "ERR invalid value"
    );
}

#[test]
fn test_empty_input() {
    assert_eq!(
        parse_simple_strings(b"").err().unwrap().message(),
        "ERR Empty data"
    );
}

#[test]
fn test_with_trailing_data() {
    // RESP simple string with extra data after CRLF (should be ignored)
    assert_eq!(parse_simple_strings(b"+OK\r\nextra_data").unwrap().0, "OK");
    assert_eq!(
        parse_simple_strings(b"+PONG\r\n+another_string\r\n")
            .unwrap()
            .0,
        "PONG"
    );
}

#[test]
fn test_real_world_redis_responses() {
    // Common Redis simple string responses
    assert_eq!(parse_simple_strings(b"+OK\r\n").unwrap().0, "OK");
    assert_eq!(parse_simple_strings(b"+PONG\r\n").unwrap().0, "PONG");
    assert_eq!(parse_simple_strings(b"+QUEUED\r\n").unwrap().0, "QUEUED");

    // Redis status responses
    assert_eq!(
        parse_simple_strings(b"+Background saving started\r\n")
            .unwrap()
            .0,
        "Background saving started"
    );
    assert_eq!(
        parse_simple_strings(b"+Background saving successfully finished\r\n")
            .unwrap()
            .0,
        "Background saving successfully finished"
    );
}

#[test]
fn test_edge_cases() {
    // Just the prefix and CRLF
    assert_eq!(parse_simple_strings(b"+\r\n").unwrap().0, "");

    // Long string
    let long_string = "a".repeat(1000);
    let mut input = b"+".to_vec();
    input.extend_from_slice(long_string.as_bytes());
    input.extend_from_slice(b"\r\n");
    assert_eq!(parse_simple_strings(&input).unwrap().0, long_string);

    // String with only spaces
    assert_eq!(parse_simple_strings(b"+   \r\n").unwrap().0, "   ");

    // String with tabs and other whitespace
    assert_eq!(parse_simple_strings(b"+\t  \t\r\n").unwrap().0, "\t  \t");
}

#[test]
fn test_binary_safety_within_limits() {
    // Non-UTF8 bytes (but no \r or \n)
    let mut input = b"+".to_vec();
    input.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // Invalid UTF-8
    input.extend_from_slice(b"\r\n");

    // Should handle invalid UTF-8 gracefully with replacement characters
    if let RespType::SimpleString(data) = parse_simple_strings(&input).unwrap().0 {
        assert!(!data.is_empty()); // Should contain replacement characters
    }
}
