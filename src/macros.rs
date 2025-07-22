#[macro_export]
macro_rules! resp {
    // Null: resp!(null)
    (null) => {
        RespType::Null().to_resp_bytes()
    };

    // Simple String: resp!("PONG") or resp!(simple "PONG")
    (simple $s:expr) => {
        RespType::SimpleString($s.to_string()).to_resp_bytes()
    };
    ($s:expr) => {
        RespType::SimpleString($s.to_string()).to_resp_bytes()
    };

    // Simple Error: resp!(error "ERR message")
    (error $s:expr) => {
        RespType::SimpleError($s.to_string()).to_resp_bytes()
    };

    // Integer: resp!(int 123)
    (int $i:expr) => {
        RespType::Integer($i).to_resp_bytes()
    };

    // Bulk String: resp!(bulk "hello") or resp!(bulk vec![104, 101, 108, 108, 111])
    (bulk $s:expr) => {
        RespType::BulkString($s.into()).to_resp_bytes()
    };

    // Array: resp!(array [resp!("one"), resp!(int 2)])
    // FIXME: this doesn't work and errors
    (array [$($elem:expr),*]) => {
        RespType::Array(vec![$($elem),*]).to_resp_bytes()
    };

    // Boolean: resp!(bool true)
    (bool $b:expr) => {
        RespType::Boolean($b).to_resp_bytes()
    };

    // Double: resp!(double 3.14)
    (double $d:expr) => {
        RespType::Doubles($d).to_resp_bytes()
    };

    // Big Number: resp!(bignumber "123456789")
    (bignumber $n:expr) => {
        RespType::BigNumbers($n.to_string()).to_resp_bytes()
    };

    // Bulk Error: resp!(bulkerror [resp!("err1"), resp!("err2")])
    (bulkerror [$($elem:expr),*]) => {
        RespType::BulkErrors(vec![$($elem),*]).to_resp_bytes()
    };

    // Verbatim String: resp!(verbatim [resp!("txt"), resp!("example")])
    (verbatim [$($elem:expr),*]) => {
        RespType::VerbatimStrings(vec![$($elem),*]).to_resp_bytes()
    };

    // Map: resp!(map {resp!("key") => resp!("value")})
    (map {$($key:expr => $value:expr),*}) => {
        RespType::Maps({
            let mut map = HashMap::new();
            $(map.insert($key, $value);)*
            map
        }).to_resp_bytes()
    };

    // Attributes: resp!(attributes [resp!("key"), resp!("value")])
    (attributes [$($elem:expr),*]) => {
        RespType::Attributes(vec![$($elem),*]).to_resp_bytes()
    };

    // Set: resp!(set [resp!("one"), resp!("two")])
    (set [$($elem:expr),*]) => {
        RespType::Sets({
            let mut set = HashSet::new();
            $(set.insert($elem);)*
            set
        }).to_resp_bytes()
    };

    // Push: resp!(push [resp!("event"), resp!("data")])
    (push [$($elem:expr),*]) => {
        RespType::Pushes(vec![$($elem),*]).to_resp_bytes()
    };
}
