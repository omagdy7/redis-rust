#[macro_export]
macro_rules! resp_bytes {
    // Null: resp!(null)
    (null) => {
        Frame::Null.to_resp_bytes()
    };

    (null_array) => {
        Frame::NullArray.to_resp_bytes()
    };

    // Simple String: resp!("PONG") or resp!(simple "PONG")
    (simple $s:expr) => {
        Frame::SimpleString($s.to_string()).to_resp_bytes()
    };
    ($s:expr) => {
        Frame::SimpleString($s.to_string()).to_resp_bytes()
    };

    // Simple Error: resp!(error "ERR message")
    (error $s:expr) => {
        Frame::SimpleError($s.to_string()).to_resp_bytes()
    };

    // Integer: resp!(int 123)
    (int $i:expr) => {
        Frame::Integer($i as i64).to_resp_bytes()
    };

    // Bulk String: resp!(bulk "hello") or resp!(bulk vec![104, 101, 108, 108, 111])
    (bulk $s:expr) => {
        Frame::BulkString($s.into()).to_resp_bytes()
    };

    // Array: resp!(array => [resp!(bulk "one"), resp!(int 2)])
     (array => [$($elem:expr),*]) => {
         Frame::Array(vec![$($elem),*]).to_resp_bytes()
     };

     (array => $vec:expr) => {
         Frame::Array($vec).to_resp_bytes()
     };


     // Boolean: resp!(bool true)
    (bool $b:expr) => {
        Frame::Boolean($b).to_resp_bytes()
    };

    // Double: resp!(double 3.14)
    (double $d:expr) => {
        Frame::Double($d).to_resp_bytes()
    };

    // Big Number: resp!(bignumber "123456789")
    (bignumber $n:expr) => {
        Frame::BigNumber($n.to_string()).to_resp_bytes()
    };

    // Bulk Error: resp!(bulkerror [resp!("err1"), resp!("err2")])
    (bulkerror [$($elem:expr),*]) => {
        Frame::BulkError(vec![$($elem),*]).to_resp_bytes()
    };

    // Verbatim String: resp!(verbatim [resp!("txt"), resp!("example")])
    (verbatim [$($elem:expr),*]) => {
        Frame::VerbatimString(vec![$($elem),*]).to_resp_bytes()
    };

    // Map: resp!(map {resp!("key") => resp!("value")})
    (map {$($key:expr => $value:expr),*}) => {
        Frame::Map({
            let mut map = HashMap::new();
            $(map.insert($key, $value);)*
            map
        }).to_resp_bytes()
    };

    // Attributes: resp!(attributes [resp!("key"), resp!("value")])
    (attributes [$($elem:expr),*]) => {
        Frame::Attribute(vec![$($elem),*]).to_resp_bytes()
    };

    // Set: resp!(set [resp!("one"), resp!("two")])
    (set [$($elem:expr),*]) => {
        Frame::Set({
            let mut set = HashSet::new();
            $(set.insert($elem);)*
            set
        }).to_resp_bytes()
    };

    // Push: resp!(push [resp!("event"), resp!("data")])
    (push [$($elem:expr),*]) => {
        Frame::Push(vec![$($elem),*]).to_resp_bytes()
    };
}

macro_rules! resp {
    // Null: resp!(null)
    (null) => {
        Frame::Null
    };

    (null_array) => {
        Frame::NullArray
    };

    // Simple String: resp!("PONG") or resp!(simple "PONG")
    (simple $s:expr) => {
        Frame::SimpleString($s.to_string())
    };
    ($s:expr) => {
        Frame::SimpleString($s.to_string())
    };

    // Simple Error: resp!(error "ERR message")
    (error $s:expr) => {
        Frame::SimpleError($s.to_string())
    };

    // Integer: resp!(int 123)
    (int $i:expr) => {
        Frame::Integer($i as i64)
    };

    // Bulk String: resp!(bulk "hello") or resp!(bulk vec![104, 101, 108, 108, 111])
    (bulk $s:expr) => {
        Frame::BulkString(Bytes::copy_from_slice($s.as_bytes()))
    };

    // Array: resp!(array => [resp!(bulk "one"), resp!(int 2)])
    (array => [$($elem:expr),*]) => {
        Frame::Array(vec![$($elem),*])
    };

    (array => $vec:expr) => {
        Frame::Array($vec)
    };

    // Boolean: resp!(bool true)
    (bool $b:expr) => {
        Frame::Boolean($b)
    };

    // Double: resp!(double 3.14)
    (double $d:expr) => {
        Frame::Double($d)
    };

    // Big Number: resp!(bignumber "123456789")
    (bignumber $n:expr) => {
        Frame::BigNumber($n.to_string())
    };

    // Bulk Error: resp!(bulkerror [resp!("err1"), resp!("err2")])
    (bulkerror [$($elem:expr),*]) => {
        Frame::BulkError(vec![$($elem),*])
    };

    // Verbatim String: resp!(verbatim [resp!("txt"), resp!("example")])
    (verbatim [$($elem:expr),*]) => {
        Frame::VerbatimString(vec![$($elem),*])
    };

    // Map: resp!(map {resp!("key") => resp!("value")})
    (map {$($key:expr => $value:expr),*}) => {
        Frame::Map({
            let mut map = HashMap::new();
            $(map.insert($key, $value);)*
            map
        })
    };

    // Attributes: resp!(attributes [resp!("key"), resp!("value")])
    (attributes [$($elem:expr),*]) => {
        Frame::Attribute(vec![$($elem),*])
    };

    // Set: resp!(set [resp!("one"), resp!("two")])
    (set [$($elem:expr),*]) => {
        Frame::Set({
            let mut set = HashSet::new();
            $(set.insert($elem);)*
            set
        })
    };

    // Push: resp!(push [resp!("event"), resp!("data")])
    (push [$($elem:expr),*]) => {
        Frame::Push(vec![$($elem),*])
    };
}
