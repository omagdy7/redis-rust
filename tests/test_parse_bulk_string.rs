use bytes::Bytes;
use codecrafters_redis::frame::Frame;
use codecrafters_redis::parser::*;

#[test]
fn test_valid_bulk_strings() {
    // basic valid cases
    assert_eq!(
        parse_bulk_strings(b"$2\r\nok\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("ok"))
    );
    assert_eq!(
        parse_bulk_strings(b"$4\r\npong\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("pong"))
    );
    assert_eq!(
        parse_bulk_strings(b"$11\r\nhello world\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("hello world"))
    );

    // empty string
    assert_eq!(
        parse_bulk_strings(b"$0\r\n\r\n").unwrap().0,
        Frame::BulkString(Bytes::from(""))
    );

    // string with special characters (including \r and \n - allowed in bulk strings)
    assert_eq!(
        parse_bulk_strings(b"$13\r\nhello\r\nworld!\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("hello\r\nworld!"))
    );

    // string with various ascii characters
    assert_eq!(
        parse_bulk_strings(b"$30\r\n!@#$%^&*()_+-={}[]|\\:;\"'<>?,./\r\n")
            .unwrap()
            .0,
        Frame::BulkString(Bytes::from("!@#$%^&*()_+-={}[]|\\:;\"'<>?,./"))
    );

    // large string
    let large_content = "x".repeat(1000);
    let large_bulk = format!("$1000\r\n{}\r\n", large_content);
    assert_eq!(
        parse_bulk_strings(large_bulk.as_bytes()).unwrap().0,
        Frame::BulkString(Bytes::from(large_content))
    );

    // string with only whitespace
    assert_eq!(
        parse_bulk_strings(b"$3\r\n   \r\n").unwrap().0,
        Frame::BulkString(Bytes::from("   "))
    );

    // string with tabs and newlines
    assert_eq!(
        parse_bulk_strings(b"$7\r\nhe\tllo\n\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("he\tllo\n"))
    );
}

#[test]
fn test_null_bulk_string() {
    // Null bulk string
    let (result, remaining) = parse_bulk_strings(b"$-1\r\n").unwrap();
    assert_eq!(result, Frame::Null);
    assert_eq!(remaining, b"");

    // Null bulk string with remaining data
    let (result, remaining) = parse_bulk_strings(b"$-1\r\n+OK\r\n").unwrap();
    assert_eq!(result, Frame::Null);
    assert_eq!(remaining, b"+OK\r\n");
}

#[test]
fn test_invalid_bulk_strings() {
    // Wrong data type marker
    assert_eq!(
        parse_bulk_strings(b"+OK\r\n").err().unwrap().to_string(),
        "ERR Invalid data type"
    );

    // Invalid length format
    assert_eq!(
        parse_bulk_strings(b"$abc\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Negative length (other than -1)
    assert_eq!(
        parse_bulk_strings(b"$-5\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Missing length
    assert_eq!(
        parse_bulk_strings(b"$\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Missing first \r\n after length
    assert_eq!(
        parse_bulk_strings(b"$5hello\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Content shorter than declared length
    assert_eq!(
        parse_bulk_strings(b"$5\r\nhi\r\n").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Content longer than declared length (missing final \r\n)
    assert_eq!(
        parse_bulk_strings(b"$2\r\nhello\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );

    // Missing final \r\n
    assert_eq!(
        parse_bulk_strings(b"$5\r\nhello").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Only \r without \n at the end
    assert_eq!(
        parse_bulk_strings(b"$5\r\nhello\r")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );

    // Only \n without \r at the end
    assert_eq!(
        parse_bulk_strings(b"$5\r\nhello\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );

    // Empty input
    assert_eq!(
        parse_bulk_strings(b"").err().unwrap().to_string(),
        "ERR Empty data"
    );

    // Just the marker
    assert_eq!(
        parse_bulk_strings(b"$").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Length too large for available data
    assert_eq!(
        parse_bulk_strings(b"$100\r\nshort\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );

    // Zero length but with content
    assert_eq!(
        parse_bulk_strings(b"$0\r\nhello\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );
}

#[test]
fn test_bulk_string_remaining_bytes() {
    // Test that remaining bytes are correctly returned
    let (string, remaining) = parse_bulk_strings(b"$5\r\nhello\r\nnext data").unwrap();
    assert_eq!(string, Frame::BulkString(Bytes::from("hello")));
    assert_eq!(remaining, b"next data");

    // Test with multiple commands
    let (string, remaining) = parse_bulk_strings(b"$4\r\ntest\r\n:42\r\n").unwrap();
    assert_eq!(string, Frame::BulkString(Bytes::from("test")));
    assert_eq!(remaining, b":42\r\n");

    // Test with no remaining data
    let (string, remaining) = parse_bulk_strings(b"$3\r\nend\r\n").unwrap();
    assert_eq!(string, Frame::BulkString(Bytes::from("end")));
    assert_eq!(remaining, b"");

    // Test null string with remaining data
    let (result, remaining) = parse_bulk_strings(b"$-1\r\n+PONG\r\n").unwrap();
    assert_eq!(result, Frame::Null);
    assert_eq!(remaining, b"+PONG\r\n");
}

#[test]
fn test_bulk_string_edge_cases() {
    // String that contains the exact sequence that would end it
    assert_eq!(
        parse_bulk_strings(b"$8\r\ntest\r\n\r\n\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("test\r\n\r\n"))
    );

    // String with only \r\n
    assert_eq!(
        parse_bulk_strings(b"$2\r\n\r\n\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("\r\n"))
    );

    // String that starts with numbers
    assert_eq!(
        parse_bulk_strings(b"$5\r\n12345\r\n").unwrap().0,
        Frame::BulkString(Bytes::from("12345"))
    );

    // String with control characters
    assert_eq!(
        parse_bulk_strings(b"$5\r\n\x01\x02\x03\x04\x05\r\n")
            .unwrap()
            .0,
        Frame::BulkString(Bytes::from("\x01\x02\x03\x04\x05"))
    );

    // Maximum length value (within reason)
    let content = "a".repeat(65535);
    let bulk = format!("$65535\r\n{}\r\n", content);
    assert_eq!(
        parse_bulk_strings(bulk.as_bytes()).unwrap().0,
        Frame::BulkString(Bytes::from(content))
    );
}
