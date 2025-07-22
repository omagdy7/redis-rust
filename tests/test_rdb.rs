use codecrafters_redis::rdb::*;

#[test]
fn test_rdb_header_valid_parsing() {
    // Valid RDB header: "REDIS" + "0009" (version 9)
    let input = b"REDIS0009";

    let result = RDBHeader::from_bytes(input);
    assert!(result.is_ok());

    let (header, bytes_consumed) = result.unwrap();
    assert_eq!(header.magic_number, *b"REDIS");
    assert_eq!(header.version, *b"0009");
    assert_eq!(bytes_consumed, 9);
}

#[test]
fn test_rdb_header_invalid_magic_number() {
    let input = b"BADIS0009";

    let result = RDBHeader::from_bytes(input);
    assert_eq!(result, Err(ParseError::InvalidMagicNumber));
}

#[test]
fn test_rdb_header_insufficient_bytes() {
    let input = b"RED"; // Too short

    let result = RDBHeader::from_bytes(input);
    assert_eq!(result, Err(ParseError::UnexpectedEof));
}

#[test]
fn test_rdb_header_version_variations() {
    // Test different valid versions
    let test_cases = vec![
        (b"REDIS0006", *b"0006"),
        (b"REDIS0007", *b"0007"),
        (b"REDIS0008", *b"0008"),
        (b"REDIS0009", *b"0009"),
        (b"REDIS0010", *b"0010"),
    ];

    for (input, expected_version) in test_cases {
        let (header, _) = RDBHeader::from_bytes(input).unwrap();
        assert_eq!(header.version, expected_version);
    }
}

#[test]
fn test_rdb_metadata_empty() {
    // Empty metadata (no AUX fields before next opcode)
    let input = &[RDBFile::SELECT_DB]; // Next opcode, no metadata

    let result = RDBMetaData::from_bytes(input);
    assert!(result.is_ok());

    let (metadata, bytes_consumed) = result.unwrap();
    assert!(metadata.metadata.is_empty());
    assert_eq!(bytes_consumed, 0); // No bytes consumed, just detected end
}

#[test]
fn test_rdb_metadata_single_aux_field() {
    // AUX opcode (0xFA) + key + value
    // Format: 0xFA + length-encoded key + length-encoded value
    let mut input = Vec::new();
    input.push(RDBFile::AUX); // 0xFA

    // Length-encoded string "redis-ver" (9 bytes, so length = 9)
    input.push(9); // length of key
    input.extend_from_slice(b"redis-ver");

    // Length-encoded string "6.0.0" (5 bytes, so length = 5)
    input.push(5); // length of value
    input.extend_from_slice(b"6.0.0");

    // End marker (next opcode)
    input.push(RDBFile::SELECT_DB);

    let result = RDBMetaData::from_bytes(&input);
    assert!(result.is_ok());

    let (metadata, bytes_consumed) = result.unwrap();
    assert_eq!(metadata.metadata.len(), 1);
    assert_eq!(
        metadata.metadata.get(&"redis-ver".as_bytes().to_vec()),
        Some(&"6.0.0".as_bytes().to_vec())
    );
    assert_eq!(bytes_consumed, 17); // 1 + 1 + 9 + 1 + 5 = 17 bytes
}

#[test]
fn test_rdb_metadata_multiple_aux_fields() {
    let mut input = Vec::new();

    // First AUX field: redis-ver = 6.0.0
    input.push(RDBFile::AUX);
    input.push(9);
    input.extend_from_slice(b"redis-ver");
    input.push(5);
    input.extend_from_slice(b"6.0.0");

    // Second AUX field: redis-bits = 64
    input.push(RDBFile::AUX);
    input.push(10);
    input.extend_from_slice(b"redis-bits");
    input.push(2);
    input.extend_from_slice(b"64");

    // End marker
    input.push(RDBFile::SELECT_DB);

    let result = RDBMetaData::from_bytes(&input);
    assert!(result.is_ok());

    let (metadata, bytes_consumed) = result.unwrap();
    assert_eq!(metadata.metadata.len(), 2);
    assert_eq!(
        metadata.metadata.get(&"redis-ver".as_bytes().to_vec()),
        Some(&"6.0.0".as_bytes().to_vec())
    );
    assert_eq!(
        metadata.metadata.get(&"redis-bits".as_bytes().to_vec()),
        Some(&"64".as_bytes().to_vec())
    );
    assert_eq!(bytes_consumed, 32); // Calculate: 1+1+9+1+5 + 1+1+10+1+2 = 32
}

#[test]
fn test_rdb_metadata_invalid_aux_field() {
    let mut input = Vec::new();
    input.push(RDBFile::AUX);
    input.push(5); // Claims 5 bytes but only provides 3
    input.extend_from_slice(b"key"); // Only 3 bytes

    let result = RDBMetaData::from_bytes(&input);
    assert_eq!(result.err().unwrap(), (ParseError::UnexpectedEof));
}

#[test]
fn test_rdb_metadata_string_encoding_edge_cases() {
    // Test with binary data in metadata values
    let mut input = Vec::new();
    input.push(RDBFile::AUX);
    input.push(4);
    input.extend_from_slice(b"test");
    input.push(3);
    input.extend_from_slice(&[0xFF, 0x00, 0x7F]); // Binary data
    input.push(RDBFile::SELECT_DB);

    let result = RDBMetaData::from_bytes(&input);
    assert!(result.is_ok());

    let (metadata, _) = result.unwrap();
    // Since we're storing as String, this tests how we handle non-UTF8
    assert_eq!(metadata.metadata.len(), 1);
    assert!(metadata.metadata.contains_key(&"test".as_bytes().to_vec()));
}

#[test]
fn test_integration_header_plus_metadata() {
    // Test parsing header followed by metadata
    let mut input = Vec::new();

    // Header
    input.extend_from_slice(b"REDIS0009");

    // Metadata
    input.push(RDBFile::AUX);
    input.push(9);
    input.extend_from_slice(b"redis-ver");
    input.push(5);
    input.extend_from_slice(b"6.0.0");

    // End of metadata
    input.push(RDBFile::SELECT_DB);

    // Parse header first
    let (_, header_consumed) = RDBHeader::from_bytes(&input).unwrap();
    assert_eq!(header_consumed, 9);

    // Parse metadata from remaining bytes
    let (metadata, metadata_consumed) = RDBMetaData::from_bytes(&input[header_consumed..]).unwrap();
    assert_eq!(metadata.metadata.len(), 1);

    let total_consumed = header_consumed + metadata_consumed;
    assert_eq!(total_consumed, input.len() - 1); // -1 for the SELECT_DB marker
}

// Helper function to create test data for string entries
fn create_test_string_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    // Value type (0 = String)
    data.push(0);
    // Key (length-prefixed)
    data.push(key.len() as u8);
    data.extend_from_slice(key);
    // Value (length-prefixed)
    data.push(value.len() as u8);
    data.extend_from_slice(value);
    data
}

// Helper function to create string entry with integer value encoding
fn create_test_integer_string_entry(key: &[u8], value: i64) -> Vec<u8> {
    let mut data = Vec::new();
    // Value type (0 = String)
    data.push(0);
    // Key
    data.push(key.len() as u8);
    data.extend_from_slice(key);
    // Integer value encoded as special format
    if value >= i8::MIN as i64 && value <= i8::MAX as i64 {
        data.extend_from_slice(&[0xC0, value as u8]);
    } else if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
        data.push(0xC1);
        data.extend_from_slice(&(value as i16).to_be_bytes());
    } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
        data.push(0xC2);
        data.extend_from_slice(&(value as i32).to_be_bytes());
    }
    data
}

#[test]
fn test_empty_database_parsing() {
    // Test parsing a database with no entries
    let mut input = Vec::new();

    // Hash table size info (both sizes are 0)
    input.push(RDBFile::RESIZE_DB);
    input.push(0); // hash_table_size = 0
    input.push(0); // expired_hash_table_size = 0
    input.push(RDBFile::EOF); // EOF

    // No entries follow

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, bytes_consumed) = result.unwrap();
    assert_eq!(database.size_hints.hash_table_size, 0);
    assert_eq!(database.size_hints.expired_hash_table_size, 0);
    assert!(database.hash_table.is_empty());
    assert_eq!(bytes_consumed, 3); // RESIZE_DB + 2 size bytes
}

#[test]
fn test_single_string_entry_database() {
    let mut input = Vec::new();

    // Hash table size info
    input.push(RDBFile::RESIZE_DB);
    input.push(1); // hash_table_size = 1
    input.push(0); // expired_hash_table_size = 0

    // Single string entry
    input.extend_from_slice(&create_test_string_entry(b"mykey", b"myvalue"));
    input.push(RDBFile::EOF); // EOF

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, bytes_consumed) = result.unwrap();
    assert_eq!(database.size_hints.hash_table_size, 1);
    assert_eq!(database.size_hints.expired_hash_table_size, 0);
    assert_eq!(database.hash_table.len(), 1);

    let entry = database.hash_table.get(b"mykey".as_slice()).unwrap();
    assert!(entry.expiry.is_none());
    assert_eq!(entry.value_type as u8, 0); // String type
    if let RedisValue::String(value) = &entry.value {
        assert_eq!(value, b"myvalue");
    } else {
        panic!("Expected string value");
    }

    // Verify bytes consumed: RESIZE_DB(1) + sizes(2) + entry(1+1+5+1+7) = 17
    assert_eq!(bytes_consumed, 18);
}

#[test]
fn test_multiple_string_entries() {
    let mut input = Vec::new();

    // Hash table size info
    input.push(RDBFile::RESIZE_DB);
    input.push(3); // hash_table_size = 3
    input.push(0); // expired_hash_table_size = 0

    // Multiple string entries
    input.extend_from_slice(&create_test_string_entry(b"key1", b"value1"));
    input.extend_from_slice(&create_test_string_entry(b"key2", b"value2"));
    input.extend_from_slice(&create_test_string_entry(b"key3", b"value3"));
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.size_hints.hash_table_size, 3);
    assert_eq!(database.hash_table.len(), 3);

    // Verify all entries
    let entry1 = database.hash_table.get(b"key1".as_slice()).unwrap();
    if let RedisValue::String(value) = &entry1.value {
        assert_eq!(value, b"value1");
    } else {
        panic!("Expected string value");
    }

    let entry2 = database.hash_table.get(b"key2".as_slice()).unwrap();
    if let RedisValue::String(value) = &entry2.value {
        assert_eq!(value, b"value2");
    } else {
        panic!("Expected string value");
    }

    let entry3 = database.hash_table.get(b"key3".as_slice()).unwrap();
    if let RedisValue::String(value) = &entry3.value {
        assert_eq!(value, b"value3");
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_string_entry_with_expiry_seconds() {
    let mut input = Vec::new();

    // Hash table size info
    input.push(RDBFile::RESIZE_DB);
    input.push(1); // hash_table_size = 1
    input.push(1); // expired_hash_table_size = 1 (this entry will have expiry)

    // Entry with expiry in seconds
    input.push(RDBFile::EXPIRE_TIME); // 0xFD
    input.extend_from_slice(&1672531200u32.to_le_bytes()); // Unix timestamp (4 bytes)
    input.extend_from_slice(&create_test_string_entry(
        b"expiring_key",
        b"expiring_value",
    ));
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.size_hints.expired_hash_table_size, 1);
    assert_eq!(database.hash_table.len(), 1);

    let entry = database.hash_table.get(b"expiring_key".as_slice()).unwrap();
    assert!(entry.expiry.is_some());

    let expiry = entry.expiry.as_ref().unwrap();
    assert_eq!(expiry.timestamp, 1672531200);
    assert!(matches!(expiry.unit, ExpiryUnit::Seconds));

    if let RedisValue::String(value) = &entry.value {
        assert_eq!(value, b"expiring_value");
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_string_entry_with_expiry_milliseconds() {
    let mut input = Vec::new();

    // Hash table size info
    input.push(RDBFile::RESIZE_DB);
    input.push(1);
    input.push(1); // 1 entry will have expiry

    // Entry with expiry in milliseconds
    input.push(RDBFile::EXPIRE_TIME_MS); // 0xFC
    input.extend_from_slice(&1672531200000u64.to_le_bytes()); // Unix timestamp in ms (8 bytes)
    input.extend_from_slice(&create_test_string_entry(
        b"expiring_ms_key",
        b"expiring_ms_value",
    ));
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();

    let entry = database
        .hash_table
        .get(b"expiring_ms_key".as_slice())
        .unwrap();
    assert!(entry.expiry.is_some());

    let expiry = entry.expiry.as_ref().unwrap();
    assert_eq!(expiry.timestamp, 1672531200000);
    assert!(matches!(expiry.unit, ExpiryUnit::Milliseconds));

    if let RedisValue::String(value) = &entry.value {
        assert_eq!(value, b"expiring_ms_value");
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_mixed_expiry_and_non_expiry_entries() {
    let mut input = Vec::new();

    input.push(RDBFile::RESIZE_DB);
    input.push(3); // 3 total entries
    input.push(2); // 2 entries will have expiry

    // Non-expiring entry
    input.extend_from_slice(&create_test_string_entry(
        b"permanent_key",
        b"permanent_value",
    ));

    // Entry with seconds expiry
    input.push(RDBFile::EXPIRE_TIME);
    input.extend_from_slice(&1672531200u32.to_le_bytes());
    input.extend_from_slice(&create_test_string_entry(
        b"expires_sec",
        b"expires_sec_value",
    ));

    // Entry with milliseconds expiry
    input.push(RDBFile::EXPIRE_TIME_MS);
    input.extend_from_slice(&1672531300000u64.to_le_bytes());
    input.extend_from_slice(&create_test_string_entry(
        b"expires_ms",
        b"expires_ms_value",
    ));
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.hash_table.len(), 3);

    // Check permanent entry
    let permanent = database
        .hash_table
        .get(b"permanent_key".as_slice())
        .unwrap();
    assert!(permanent.expiry.is_none());

    // Check seconds expiry entry
    let sec_expiry = database.hash_table.get(b"expires_sec".as_slice()).unwrap();
    assert!(sec_expiry.expiry.is_some());
    assert!(matches!(
        sec_expiry.expiry.as_ref().unwrap().unit,
        ExpiryUnit::Seconds
    ));

    // Check milliseconds expiry entry
    let ms_expiry = database.hash_table.get(b"expires_ms".as_slice()).unwrap();
    assert!(ms_expiry.expiry.is_some());
    assert!(matches!(
        ms_expiry.expiry.as_ref().unwrap().unit,
        ExpiryUnit::Milliseconds
    ));
}

#[test]
fn test_string_with_integer_value_encoding() {
    let mut input = Vec::new();

    input.push(RDBFile::RESIZE_DB);
    input.push(3); // 3 entries
    input.push(0);

    // Test different integer encodings
    input.extend_from_slice(&create_test_integer_string_entry(b"int8", 42)); // 8-bit
    input.extend_from_slice(&create_test_integer_string_entry(b"int16", 1000)); // 16-bit
    input.extend_from_slice(&create_test_integer_string_entry(b"int32", 100000)); // 32-bit
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.hash_table.len(), 3);

    // All should be parsed as Integer values due to special encoding
    let int8_entry = database.hash_table.get(b"int8".as_slice()).unwrap();
    if let RedisValue::Integer(value) = &int8_entry.value {
        assert_eq!(*value, 42);
    } else {
        panic!("Expected integer value, got: {:?}", int8_entry.value);
    }

    let int16_entry = database.hash_table.get(b"int16".as_slice()).unwrap();
    if let RedisValue::Integer(value) = &int16_entry.value {
        assert_eq!(*value, 1000);
    } else {
        panic!("Expected integer value");
    }

    let int32_entry = database.hash_table.get(b"int32".as_slice()).unwrap();
    if let RedisValue::Integer(value) = &int32_entry.value {
        assert_eq!(*value, 100000);
    } else {
        panic!("Expected integer value");
    }
}

#[test]
fn test_large_hash_table_sizes_14bit_encoding() {
    let mut input = Vec::new();

    input.push(RDBFile::RESIZE_DB);

    // Test 14-bit length encoding (values 64-16383)
    let large_size = 16000;
    // 14-bit encoding: 0b01xxxxxx xxxxxxxx
    let first_byte = 0x40 | ((large_size >> 8) as u8);
    let second_byte = (large_size & 0xFF) as u8;
    input.extend_from_slice(&[first_byte, second_byte]);

    input.push(0); // No expired entries
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, bytes_consumed) = result.unwrap();
    assert_eq!(database.size_hints.hash_table_size, large_size);
    assert_eq!(database.size_hints.expired_hash_table_size, 0);
    assert_eq!(bytes_consumed, 4); // RESIZE_DB + 2 bytes for 14-bit size + 1 byte for expired size
}

#[test]
fn test_binary_string_data() {
    let mut input = Vec::new();

    input.push(RDBFile::RESIZE_DB);
    input.push(2);
    input.push(0);

    // Test with binary data containing null bytes and non-UTF8 sequences
    let binary_key = &[0xFF, 0x00, 0x7F, b'k', b'e', b'y'];
    let binary_value = &[0x00, 0xFF, 0x80, 0x81, 0x82];

    input.extend_from_slice(&create_test_string_entry(binary_key, binary_value));

    // Test with empty string
    input.extend_from_slice(&create_test_string_entry(b"empty_value", b""));
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.hash_table.len(), 2);

    // Verify binary data preserved
    let binary_entry = database.hash_table.get(&binary_key.to_vec()).unwrap();
    if let RedisValue::String(value) = &binary_entry.value {
        assert_eq!(value, binary_value);
    } else {
        panic!("Expected string value");
    }

    // Verify empty string
    let empty_entry = database.hash_table.get(b"empty_value".as_slice()).unwrap();
    if let RedisValue::String(value) = &empty_entry.value {
        assert!(value.is_empty());
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_very_long_strings() {
    let mut input = Vec::new();

    input.push(RDBFile::RESIZE_DB);
    input.push(1);
    input.push(0);

    // Create a long string (using 14-bit length encoding)
    let long_value = vec![b'A'; 1000];

    // Manually create the entry with proper length encoding
    let mut entry = Vec::new();
    entry.push(0); // String type
    entry.push(8); // Key length
    entry.extend_from_slice(b"long_key");

    // 14-bit length encoding for 1000 bytes
    let first_byte = 0x40 | ((1000 >> 8) as u8);
    let second_byte = (1000 & 0xFF) as u8;
    entry.extend_from_slice(&[first_byte, second_byte]);
    entry.extend_from_slice(&long_value);

    input.extend_from_slice(&entry);
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.hash_table.len(), 1);

    let long_entry = database.hash_table.get(b"long_key".as_slice()).unwrap();
    if let RedisValue::String(value) = &long_entry.value {
        assert_eq!(value.len(), 1000);
        assert!(value.iter().all(|&b| b == b'A'));
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_database_parsing_errors() {
    // Test truncated data
    let truncated = vec![RDBFile::RESIZE_DB, 1]; // Missing expired size
    let result = RDBDatabase::from_bytes(&truncated);
    assert_eq!(result.err().unwrap(), ParseError::UnexpectedEof);

    // Test truncated string entry
    let mut truncated_entry = Vec::new();
    truncated_entry.push(RDBFile::RESIZE_DB);
    truncated_entry.push(1);
    truncated_entry.push(0);
    truncated_entry.push(0); // String type
    truncated_entry.push(5); // Key length
    truncated_entry.extend_from_slice(b"key"); // Only 3 bytes instead of 5
    truncated_entry.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&truncated_entry);
    assert_eq!(result.err().unwrap(), ParseError::UnexpectedEof);
}

#[test]
fn test_utf8_and_special_characters() {
    let mut input = Vec::new();

    input.push(RDBFile::RESIZE_DB);
    input.push(3);
    input.push(0);

    // UTF-8 strings
    input.extend_from_slice(&create_test_string_entry(
        "héllo".as_bytes(),
        "wörld".as_bytes(),
    ));

    // String with emoji
    input.extend_from_slice(&create_test_string_entry(
        "emoji_key".as_bytes(),
        "🚀🔥".as_bytes(),
    ));

    // String with various special characters
    input.extend_from_slice(&create_test_string_entry(
        "special".as_bytes(),
        "!@#$%^&*()_+-=[]{}|;':\",./<>?`~".as_bytes(),
    ));
    input.push(RDBFile::EOF);

    let result = RDBDatabase::from_bytes(&input);
    assert!(result.is_ok());

    let (database, _) = result.unwrap();
    assert_eq!(database.hash_table.len(), 3);

    // Verify UTF-8 preservation
    let utf8_entry = database.hash_table.get("héllo".as_bytes()).unwrap();
    if let RedisValue::String(value) = &utf8_entry.value {
        assert_eq!(value, "wörld".as_bytes());
    } else {
        panic!("Expected string value");
    }

    // Verify emoji preservation
    let emoji_entry = database.hash_table.get("emoji_key".as_bytes()).unwrap();
    if let RedisValue::String(value) = &emoji_entry.value {
        assert_eq!(value, "🚀🔥".as_bytes());
    } else {
        panic!("Expected string value");
    }
}

#[test]
fn test_rdb_file() {
    let input: Vec<u8> = vec![
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69,
        0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65,
        0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfe, 0x00, 0xfb, 0x01, 0x00,
        0x00, 0x04, 0x70, 0x65, 0x61, 0x72, 0x06, 0x6f, 0x72, 0x61, 0x6e, 0x67, 0x65, 0xff, 0xe9,
        0xa3, 0x16, 0x7c, 0x84, 0x37, 0x9e, 0xb4,
    ];

    let result = RDBFile::from_bytes(&input);
    dbg!(&result);
    assert!(result.is_ok());

    let (rdb, _) = result.unwrap();
    assert_eq!(rdb.databases.get(&0).unwrap().hash_table.len(), 1);
}
