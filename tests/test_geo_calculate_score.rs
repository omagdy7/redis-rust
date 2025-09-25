use codecrafters_redis::frame::GeoPosition;

#[test]
fn test_city_scores() {
    let test_cases = vec![
        ("Bangkok", 100.5252, 13.7220, 3962257306574459u64),
        ("Beijing", 116.3972, 39.9075, 4069885364908765u64),
        ("Berlin", 13.4105, 52.5244, 3673983964876493u64),
        ("Copenhagen", 12.5655, 55.6759, 3685973395504349u64),
        ("New Delhi", 77.2167, 28.6667, 3631527070936756u64),
        ("Kathmandu", 85.3206, 27.7017, 3639507404773204u64),
        ("London", -0.1278, 51.5074, 2163557714755072u64),
        ("New York", -74.0060, 40.7128, 1791873974549446u64),
        ("Paris", 2.3488, 48.8534, 3663832752681684u64),
        ("Sydney", 151.2093, -33.8688, 3252046221964352u64),
        ("Tokyo", 139.6917, 35.6895, 4171231230197045u64),
        ("Vienna", 16.3707, 48.2064, 3673109836391743u64),
    ];

    for (city, lon, lat, expected) in test_cases {
        let pos = GeoPosition::new(lon, lat);
        assert!(
            pos.validate(),
            "Validation failed for city: {} (lat={}, lon={})",
            city,
            lat,
            lon
        );

        let score = pos.calculate_score();
        assert_eq!(
            score, expected,
            "Score mismatch for city: {} (lat={}, lon={})",
            city, lat, lon
        );
    }
}

#[test]
fn test_decode_city_scores() {
    const EPSILON: f64 = 1e-4;
    let test_cases = vec![
        ("Bangkok", 100.5252, 13.7220, 3962257306574459u64),
        ("Beijing", 116.3972, 39.9075, 4069885364908765u64),
        ("Berlin", 13.4105, 52.5244, 3673983964876493u64),
        ("Copenhagen", 12.5655, 55.6759, 3685973395504349u64),
        ("New Delhi", 77.2167, 28.6667, 3631527070936756u64),
        ("Kathmandu", 85.3206, 27.7017, 3639507404773204u64),
        ("London", -0.1278, 51.5074, 2163557714755072u64),
        ("New York", -74.0060, 40.7128, 1791873974549446u64),
        ("Paris", 2.3488, 48.8534, 3663832752681684u64),
        ("Sydney", 151.2093, -33.8688, 3252046221964352u64),
        ("Tokyo", 139.6917, 35.6895, 4171231230197045u64),
        ("Vienna", 16.3707, 48.2064, 3673109836391743u64),
    ];

    for (city, lon_expected, lat_expected, score) in test_cases {
        let (lon, lat) = GeoPosition::decode_score(score);

        assert!(
            (lon - lon_expected).abs() < EPSILON,
            "Longitude mismatch for {}: got {}, expected {}",
            city,
            lon,
            lon_expected
        );
        assert!(
            (lat - lat_expected).abs() < EPSILON,
            "Latitude mismatch for {}: got {}, expected {}",
            city,
            lat,
            lat_expected
        );
    }
}
