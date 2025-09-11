use codecrafters_redis::frame::Frame;
use codecrafters_redis::parser::*;

#[test]
fn test_valid_arrays() {
    // Simple array with strings
    let arr = vec![
        Frame::BulkString("hello".into()),
        Frame::BulkString("world".into()),
    ];
    assert_eq!(
        parse_array(b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
            .unwrap()
            .0,
        Frame::Array(arr)
    );

    // Array with mixed types
    let arr = vec![
        Frame::Integer(42),
        Frame::SimpleString("OK".to_string()),
        Frame::Null,
    ];
    assert_eq!(
        parse_array(b"*3\r\n:42\r\n+OK\r\n_\r\n").unwrap().0,
        Frame::Array(arr)
    );

    // Nested array
    let arr = vec![
        Frame::Array(vec![
            Frame::BulkString("nested".into()),
            Frame::Integer(123),
        ]),
        Frame::SimpleError("ERR test".to_string()),
    ];
    assert_eq!(
        parse_array(b"*2\r\n*2\r\n$6\r\nnested\r\n:123\r\n-ERR test\r\n")
            .unwrap()
            .0,
        Frame::Array(arr)
    );

    // Empty array
    let arr = vec![];
    assert_eq!(parse_array(b"*0\r\n").unwrap().0, Frame::Array(arr));
}

#[test]
fn test_invalid_arrays() {
    // Wrong data type marker
    assert_eq!(
        parse_array(b"+2\r\n$5\r\nhello\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Invalid data type"
    );

    // Missing \r\n terminator
    assert_eq!(
        parse_array(b"*2\r\n$5\r\nhello\r\n$5\r\nworld")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );

    // Invalid length
    assert_eq!(
        parse_array(b"*-1\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Non-numeric length
    assert_eq!(
        parse_array(b"*abc\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Incomplete array elements
    assert_eq!(
        parse_array(b"*2\r\n$5\r\nhello\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Unexpected end of input"
    );

    // Empty input
    assert_eq!(parse_array(b"").err().unwrap().to_string(), "ERR Empty data");

    // Just the marker
    assert_eq!(
        parse_array(b"*").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Invalid element type
    assert_eq!(
        parse_array(b"*1\r\n@invalid\r\n").err().unwrap().to_string(),
        "ERR Invalid data type"
    );
}

#[test]
fn test_array_remaining_bytes() {
    // Test with remaining data
    let arr = vec![Frame::BulkString("test".into()), Frame::Integer(99)];
    let (value, remaining) = parse_array(b"*2\r\n$4\r\ntest\r\n:99\r\n+OK\r\n").unwrap();
    assert_eq!(value, Frame::Array(arr));
    assert_eq!(remaining, b"+OK\r\n");

    // Test with no remaining data
    let arr = vec![Frame::SimpleString("PONG".to_string())];
    let (value, remaining) = parse_array(b"*1\r\n+PONG\r\n").unwrap();
    assert_eq!(value, Frame::Array(arr));
    assert_eq!(remaining, b"");

    // Test with multiple commands
    let arr = vec![Frame::Null];
    let (value, remaining) = parse_array(b"*1\r\n_\r\n*0\r\n").unwrap();
    assert_eq!(value, Frame::Array(arr));
    assert_eq!(remaining, b"*0\r\n");

    // Test with empty array and remaining data
    let arr = vec![];
    let (value, remaining) = parse_array(b"*0\r\n-ERR test\r\n").unwrap();
    assert_eq!(value, Frame::Array(arr));
    assert_eq!(remaining, b"-ERR test\r\n");
}
