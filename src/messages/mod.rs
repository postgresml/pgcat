pub mod authentication_md5_password;
pub mod authentication_ok;
pub mod backend_key_data;
pub mod error_response;
pub mod parameter_status;
pub mod password_message;
pub mod query;
pub mod ready_for_query;
pub mod row_description;
pub mod startup_message;

use std::fmt::Write;

#[derive(Debug, PartialEq, Clone)]
pub enum MessageName {
    SslRequest,
    StartupMessage,
    Termination,
    AuthenticationOk,
    ReadyForQuery,
    Query,
    ParameterStatus,
    BackendKeyData,
    AuthenticationMD5Password,
    ErrorResponse,
    RowDescription,
}

impl std::fmt::Display for MessageName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = match self {
            // TODO: finish these
            MessageName::SslRequest => '_',
            MessageName::StartupMessage => '_',
            MessageName::Termination => 'X',
            MessageName::AuthenticationOk => '_',
            MessageName::ReadyForQuery => 'Z',
            MessageName::Query => '_',
            MessageName::ParameterStatus => '_',
            MessageName::BackendKeyData => '_',
            MessageName::AuthenticationMD5Password => '_',
            _ => '_',
        };

        f.write_char(c)
    }
}

pub trait Message {
    fn len(&self) -> i32;
    fn parse(buf: &[u8], len: i32) -> Option<Self>
    where
        Self: Sized;
    fn to_vec(&self) -> Vec<u8>;
    fn debug(&self) -> String;
}

pub fn parse(buf: &[u8]) -> Result<(usize, MessageName), &'static str> {
    let c = buf[0] as char;
    let len = i32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
    println!("DEBUG: {:?}", &buf[0..(len as usize)]);
    match c {
        'S' => Ok((len, MessageName::ParameterStatus)),

        'K' => Ok((len, MessageName::BackendKeyData)),

        // Determines transaction state of the backend
        'Z' => Ok((len, MessageName::ReadyForQuery)),

        'R' => {
            let code = i32::from_be_bytes(buf[5..9].try_into().unwrap());
            match code {
                0 => Ok((len, MessageName::AuthenticationOk)),
                5 => Ok((len, MessageName::AuthenticationMD5Password)),
                _ => Err("ERROR: unknown auth request"),
            }
        }

        'T' => Ok((len, MessageName::RowDescription)),

        'E' => Ok((len, MessageName::ErrorResponse)),
        'Q' => Ok((len, MessageName::Query)),
        'X' => Ok((len, MessageName::Termination)),
        _ => Err("ERROR: unknown message"),
    }
}
