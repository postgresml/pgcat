pub mod authentication_ok;
pub mod ready_for_query;
pub mod startup_message;
pub mod password_message;
pub mod authentication_md5_password;
pub mod parameter_status;
pub mod backend_key_data;
pub mod query;
pub mod error_response;

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
}

impl std::fmt::Display for MessageName {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let c = match self {
			// TODO: finish these
			SslRequest => '_',
		    StartupMessage => '_',
		    Termination => '_',
		    AuthenticationOk => '_',
		    ReadyForQuery => 'Z',
		    Query => '_',
		    ParameterStatus => '_',
		    BackendKeyData => '_',
		    AuthenticationMD5Password => '_',
		};

        f.write_char(c)
    }
}

pub trait Message {
	fn len(&self) -> i32;
	fn parse(buf: &[u8], len: i32) -> Option<Self> where Self: Sized;
	fn to_vec(&self) -> Vec<u8>;
	fn debug(&self) -> String;
}

pub fn parse(buf: &[u8]) -> Option<(usize, MessageName)> {
	let c = buf[0] as char;
	let len = i32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
	println!("DEBUG: {:?}", buf);
	match c {
		'S' => Some((len, MessageName::ParameterStatus)),

		'K' => Some((len, MessageName::BackendKeyData)),

		// Determines transaction state of the backend
		'Z' => Some((len, MessageName::ReadyForQuery)),

		'R' => {
			let code = i32::from_be_bytes(buf[5..9].try_into().unwrap());
			match code {
				0 => Some((len, MessageName::AuthenticationOk)),
				5 => Some((len, MessageName::AuthenticationMD5Password)),
				_ => None,
			}
		},

		'E' => Some((len, MessageName::ErrorResponse)),
		'Q' => Some((len, MessageName::Query)),
		_ => None,
	}
}