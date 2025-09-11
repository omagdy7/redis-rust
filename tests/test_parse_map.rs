use bytes::Bytes;
use codecrafters_redis::frame::Frame;
use codecrafters_redis::parser::*;
use std::collections::HashMap;

#[test]
fn test_valid_empty_map() {
    // Empty map: %0\r\n
    let expected_map = HashMap::new();
    assert_eq!(parse_maps(b"%0\r\n").unwrap().0, Frame::Map(expected_map));
}

#[test]
fn test_valid_simple_string_map() {
    // Simple map with simple string key-value pairs
    // %2\r\n+key1\r\n+value1\r\n+key2\r\n+value2\r\n
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "key1".to_string(),
        Frame::SimpleString("value1".to_string()),
    );
    expected_map.insert(
        "key2".to_string(),
        Frame::SimpleString("value2".to_string()),
    );

    assert_eq!(
        parse_maps(b"%2\r\n+key1\r\n+value1\r\n+key2\r\n+value2\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_valid_mixed_types_map() {
    // Map with different value types
    // %4\r\n+string_key\r\n+string_value\r\n+int_key\r\n:42\r\n+bool_key\r\n#t\r\n+null_key\r\n_\r\n
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "string_key".to_string(),
        Frame::SimpleString("string_value".to_string()),
    );
    expected_map.insert("int_key".to_string(), Frame::Integer(42));
    expected_map.insert("bool_key".to_string(), Frame::Boolean(true));
    expected_map.insert("null_key".to_string(), Frame::Null);

    assert_eq!(
             parse_maps(b"%4\r\n+string_key\r\n+string_value\r\n+int_key\r\n:42\r\n+bool_key\r\n#t\r\n+null_key\r\n_\r\n").unwrap().0,
             Frame::Map(expected_map)
         );
}

#[test]
fn test_valid_bulk_string_keys_and_values() {
    // Map with bulk string keys and values
    // %2\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n
    let mut expected_map = HashMap::new();
    expected_map.insert("key1".to_string(), Frame::BulkString("value1".into()));
    expected_map.insert("key2".to_string(), Frame::BulkString("value2".into()));

    assert_eq!(
        parse_maps(b"%2\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_valid_array_values() {
    // Map with array values
    // %1\r\n+array_key\r\n*3\r\n+item1\r\n:123\r\n#f\r\n
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "array_key".to_string(),
        Frame::Array(vec![
            Frame::SimpleString("item1".to_string()),
            Frame::Integer(123),
            Frame::Boolean(false),
        ]),
    );

    assert_eq!(
        parse_maps(b"%1\r\n+array_key\r\n*3\r\n+item1\r\n:123\r\n#f\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_valid_nested_map() {
    // Map with nested map value
    // %1\r\n+nested_key\r\n%1\r\n+inner_key\r\n+inner_value\r\n
    let mut inner_map = HashMap::new();
    inner_map.insert(
        "inner_key".to_string(),
        Frame::SimpleString("inner_value".to_string()),
    );

    let mut expected_map = HashMap::new();
    expected_map.insert("nested_key".to_string(), Frame::Map(inner_map));

    assert_eq!(
        parse_maps(b"%1\r\n+nested_key\r\n%1\r\n+inner_key\r\n+inner_value\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_valid_double_values() {
    // Map with double values
    // %2\r\n+pi\r\n,3.14159\r\n+e\r\n,2.71828\r\n
    let mut expected_map = HashMap::new();
    expected_map.insert("pi".to_string(), Frame::Double(3.14159));
    expected_map.insert("e".to_string(), Frame::Double(2.71828));

    assert_eq!(
        parse_maps(b"%2\r\n+pi\r\n,3.14159\r\n+e\r\n,2.71828\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_valid_simple_error_values() {
    // Map with simple error values
    // %2\r\n+error_key\r\n-ERR Something went wrong\r\n+another_key\r\n+value\r\n
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "error_key".to_string(),
        Frame::SimpleError("ERR Something went wrong".to_string()),
    );
    expected_map.insert(
        "another_key".to_string(),
        Frame::SimpleString("value".to_string()),
    );

    assert_eq!(
        parse_maps(b"%2\r\n+error_key\r\n-ERR Something went wrong\r\n+another_key\r\n+value\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_valid_complex_mixed_map() {
    // Complex map with various types mixed together
    // %5\r\n+string\r\n+hello\r\n+number\r\n:42\r\n+list\r\n*2\r\n+a\r\n+b\r\n+map\r\n%1\r\n+nested\r\n+value\r\n+double\r\n,3.14\r\n
    let mut nested_map = HashMap::new();
    nested_map.insert(
        "nested".to_string(),
        Frame::SimpleString("value".to_string()),
    );

    let mut expected_map = HashMap::new();
    expected_map.insert(
        "string".to_string(),
        Frame::SimpleString("hello".to_string()),
    );
    expected_map.insert("number".to_string(), Frame::Integer(42));
    expected_map.insert(
        "list".to_string(),
        Frame::Array(vec![
            Frame::SimpleString("a".to_string()),
            Frame::SimpleString("b".to_string()),
        ]),
    );
    expected_map.insert("map".to_string(), Frame::Map(nested_map));
    expected_map.insert("double".to_string(), Frame::Double(3.14));

    assert_eq!(
             parse_maps(b"%5\r\n+string\r\n+hello\r\n+number\r\n:42\r\n+list\r\n*2\r\n+a\r\n+b\r\n+map\r\n%1\r\n+nested\r\n+value\r\n+double\r\n,3.14\r\n").unwrap().0,
             Frame::Map(expected_map)
         );
}

#[test]
fn test_invalid_maps() {
    // Wrong data type marker
    assert_eq!(
        parse_maps(b"+2\r\n+key\r\n+value\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Invalid data type"
    );

    // Wrong prefix
    assert_eq!(
        parse_maps(b"*2\r\n+key\r\n+value\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Invalid data type"
    );

    // Missing \r\n terminator after count
    assert_eq!(
        parse_maps(b"%1\r\n+key\r\n+value").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Invalid length - negative
    assert_eq!(
        parse_maps(b"%-1\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Non-numeric length
    assert_eq!(
        parse_maps(b"%abc\r\n").err().unwrap().to_string(),
        "ERR invalid value"
    );

    // Odd number of elements (maps need key-value pairs)
    assert_eq!(
        parse_maps(b"%1\r\n+key\r\n").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Incomplete map elements
    assert_eq!(
        parse_maps(b"%1\r\n+key\r\n").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Empty input
    assert_eq!(parse_maps(b"").err().unwrap().to_string(), "ERR Empty data");

    // Just the marker
    assert_eq!(
        parse_maps(b"%").err().unwrap().to_string(),
        "ERR Unexpected end of input"
    );

    // Invalid element type
    assert_eq!(
        parse_maps(b"%1\r\n@invalid\r\n+value\r\n")
            .err()
            .unwrap()
            .to_string(),
        "ERR Invalid data type"
    );
}

#[test]
fn test_map_remaining_bytes() {
    // Test with remaining data
    let mut expected_map = HashMap::new();
    expected_map.insert("key".to_string(), Frame::SimpleString("value".to_string()));

    let (value, remaining) = parse_maps(b"%1\r\n+key\r\n+value\r\n+OK\r\n").unwrap();
    assert_eq!(value, Frame::Map(expected_map));
    assert_eq!(remaining, b"+OK\r\n");

    // Test with no remaining data
    let mut expected_map = HashMap::new();
    expected_map.insert("test".to_string(), Frame::Integer(42));

    let (value, remaining) = parse_maps(b"%1\r\n+test\r\n:42\r\n").unwrap();
    assert_eq!(value, Frame::Map(expected_map));
    assert_eq!(remaining, b"");

    // Test with multiple commands
    let mut expected_map = HashMap::new();
    expected_map.insert("null_key".to_string(), Frame::Null);

    let (value, remaining) = parse_maps(b"%1\r\n+null_key\r\n_\r\n*0\r\n").unwrap();
    assert_eq!(value, Frame::Map(expected_map));
    assert_eq!(remaining, b"*0\r\n");

    // Test with empty map and remaining data
    let expected_map = HashMap::new();
    let (value, remaining) = parse_maps(b"%0\r\n-ERR test\r\n").unwrap();
    assert_eq!(value, Frame::Map(expected_map));
    assert_eq!(remaining, b"-ERR test\r\n");
}

#[test]
fn test_duplicate_keys() {
    // Duplicate keys should overwrite (Redis behavior)
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "key1".to_string(),
        Frame::SimpleString("value2".to_string()),
    );

    assert_eq!(
        parse_maps(b"%2\r\n+key1\r\n+value1\r\n+key1\r\n+value2\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_large_map() {
    // Test with a larger map
    let mut input = b"%100\r\n".to_vec();
    let mut expected_map = HashMap::new();

    for i in 0..100 {
        input.extend_from_slice(format!("+key{}\r\n", i).as_bytes());
        input.extend_from_slice(format!("+value{}\r\n", i).as_bytes());
        expected_map.insert(
            format!("key{}", i),
            Frame::SimpleString(format!("value{}", i)),
        );
    }

    assert_eq!(parse_maps(&input).unwrap().0, Frame::Map(expected_map));
}

#[test]
fn test_edge_cases() {
    // Empty string keys and values
    let mut expected_map = HashMap::new();
    expected_map.insert("".to_string(), Frame::SimpleString("".to_string()));

    assert_eq!(
        parse_maps(b"%1\r\n+\r\n+\r\n").unwrap().0,
        Frame::Map(expected_map)
    );

    // String with spaces and special characters
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "key with spaces".to_string(),
        Frame::SimpleString("value with spaces".to_string()),
    );

    assert_eq!(
        parse_maps(b"%1\r\n+key with spaces\r\n+value with spaces\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_nested_complex_structures() {
    // Map containing array containing map
    // %1\r\n+complex\r\n*1\r\n%1\r\n+nested\r\n+deep\r\n
    let mut inner_map = HashMap::new();
    inner_map.insert(
        "nested".to_string(),
        Frame::SimpleString("deep".to_string()),
    );

    let mut expected_map = HashMap::new();
    expected_map.insert(
        "complex".to_string(),
        Frame::Array(vec![Frame::Map(inner_map)]),
    );

    assert_eq!(
        parse_maps(b"%1\r\n+complex\r\n*1\r\n%1\r\n+nested\r\n+deep\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_binary_data_in_bulk_strings() {
    // Map with binary data in bulk string values
    let mut input = b"%1\r\n+binary_key\r\n$3\r\n".to_vec();
    input.extend_from_slice(&[0xFF, 0x00, 0xFE]); // Binary data
    input.extend_from_slice(b"\r\n");

    let mut expected_map = HashMap::new();
    expected_map.insert(
        "binary_key".to_string(),
        Frame::BulkString(Bytes::from(vec![0xFF, 0x00, 0xFE])),
    );

    assert_eq!(parse_maps(&input).unwrap().0, Frame::Map(expected_map));
}

#[test]
fn test_unicode_keys() {
    // Map with Unicode keys
    let mut expected_map = HashMap::new();
    expected_map.insert("éáñ".to_string(), Frame::SimpleString("value1".to_string()));
    expected_map.insert(
        "中文".to_string(),
        Frame::SimpleString("value2".to_string()),
    );

    assert_eq!(
            parse_maps(b"%2\r\n+\xc3\xa9\xc3\xa1\xc3\xb1\r\n+value1\r\n+\xe4\xb8\xad\xe6\x96\x87\r\n+value2\r\n").unwrap().0,
            Frame::Map(expected_map)
        );
}

#[test]
fn test_mixed_key_types() {
    // Test with bulk string keys
    let mut expected_map = HashMap::new();
    expected_map.insert(
        "bulk_key".to_string(),
        Frame::SimpleString("value".to_string()),
    );
    expected_map.insert("simple_key".to_string(), Frame::Integer(42));

    assert_eq!(
        parse_maps(b"%2\r\n$8\r\nbulk_key\r\n+value\r\n+simple_key\r\n:42\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}

#[test]
fn test_deeply_nested_structures() {
    // Map -> Array -> Map -> Array
    // %1\r\n+level1\r\n*1\r\n%1\r\n+level2\r\n*2\r\n+item1\r\n+item2\r\n
    let mut level2_map = HashMap::new();
    level2_map.insert(
        "level2".to_string(),
        Frame::Array(vec![
            Frame::SimpleString("item1".to_string()),
            Frame::SimpleString("item2".to_string()),
        ]),
    );

    let mut expected_map = HashMap::new();
    expected_map.insert(
        "level1".to_string(),
        Frame::Array(vec![Frame::Map(level2_map)]),
    );

    assert_eq!(
        parse_maps(b"%1\r\n+level1\r\n*1\r\n%1\r\n+level2\r\n*2\r\n+item1\r\n+item2\r\n")
            .unwrap()
            .0,
        Frame::Map(expected_map)
    );
}
