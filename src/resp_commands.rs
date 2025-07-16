use crate::resp_parser::*;

pub enum RespCommands {
    PING,
    ECHO(String),
    GET(String),
    SET(String),
    Invalid,
}

impl RespCommands {
    pub fn execute(self) -> Vec<u8> {
        match self {
            RespCommands::PING => b"+PONG\r\n".to_vec(),
            RespCommands::ECHO(echo_string) => echo_string.into_bytes(),
            RespCommands::GET(_) => todo!(),
            RespCommands::SET(_) => todo!(),
            RespCommands::Invalid => todo!(),
        }
    }
}

impl From<RespType> for RespCommands {
    fn from(value: RespType) -> Self {
        match value {
            RespType::Array(vec) if vec.len() > 1 => match (&vec[0], &vec[1]) {
                (RespType::BulkString(command), RespType::BulkString(argument)) => {
                    if let Ok(command) = str::from_utf8(&command) {
                        match command {
                            "PING" => Self::PING,
                            "ECHO" => Self::ECHO(format!(
                                "+{}\r\n",
                                String::from_utf8(argument.clone()).unwrap()
                            )),
                            _ => Self::Invalid,
                        }
                    } else {
                        Self::Invalid
                    }
                }
                _ => todo!(),
            },
            RespType::Array(vec) => match &vec[0] {
                RespType::BulkString(command) => {
                    if let Ok(command) = str::from_utf8(&command) {
                        match command {
                            "PING" => Self::PING,
                            _ => Self::Invalid,
                        }
                    } else {
                        Self::Invalid
                    }
                }
                _ => Self::Invalid,
            },
            _ => todo!(),
        }
    }
}
