/// The PostgreSQL backend.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use md5::{Md5, Digest};

use crate::messages::startup_message::*;
use crate::messages::password_message::*;
use crate::messages::parameter_status::*;
use crate::messages::authentication_md5_password::*;
use crate::messages::error_response::*;

use crate::messages::{self, Message, MessageName};
use crate::communication::{buffer_until_complete, check, buffer};

use bb8::ManageConnection;

pub struct Server {
	pub stream: tokio::net::TcpStream,
	buf: Vec<u8>,
}

#[derive(Debug)]
pub enum Error {
	BadServer,
	UsernamePasswordIncorrect,
}

impl Server {
	pub async fn connect(host: &str, username: &str, password: Option<&str>, database: Option<&str>) -> Result<Server, Error> {

		// TCP connection
		let mut sock = match tokio::net::TcpStream::connect(host).await {
			Ok(sock) => sock,
			Err(err) => {
				println!("ERROR: cannot connect to {}, err: {}", host, err);
				return Err(Error::BadServer);
			}
		};

		// Prepare and send startup message
		let startup: Vec<u8> = StartupMessage::new(username, database).into();
		match sock.write_all(&startup).await {
			Ok(_) => (),
			Err(err) => { return Err(Error::BadServer) },
		};


		// Our work space buffer for connection setup only
		let mut buf = vec![0u8; 1024];
		let mut n = 0; // parsed bytes

		// Expecting only one message here,
		// either AuthenticationOk if no password is required or
		// AuthenticationMd5Password challenge.
		let buffer_result = match buffer_until_complete(&mut sock, &mut buf).await {
			Ok(r) => r,
			Err(_err) => return Err(Error::BadServer),
		};

		match messages::parse(&buf) {
			Some((len, name)) => {
				n += len;

				match name {
					// No password required, what a treat!
					MessageName::AuthenticationOk => (),

					MessageName::AuthenticationMD5Password => {
						println!("DEBUG: Received MD5 challenge request");
						// Check that you gave me a password here
						match password {
							Some(_p) => (),
							None => return Err(Error::UsernamePasswordIncorrect),
						}

						match AuthenticationMD5Password::parse(&buf, len as i32) {
							Some(challenge) => {
								println!("DEBUG: Sending encrypted password");

								let password_message: Vec<u8> = PasswordMessage::new(username, password.unwrap(), &challenge.salt).into();
								match sock.write_all(&password_message).await {
									Ok(_) => (),
									Err(err) => return Err(Error::BadServer),
								};

								// Reset buffer
								n = 0;

								// We could get an error here
								buffer_until_complete(&mut sock, &mut buf).await;

								match messages::parse(&buf) {
									Some((len, name)) => {
										n += len;

										match name {
											MessageName::AuthenticationOk => (),
											MessageName::ErrorResponse => {
												// Something bad happened
												let error = ErrorResponse::parse(&buf, len as i32);

												println!("DEBUG: {:?}", error);

												return Err(Error::BadServer);
											}
											_ => return Err(Error::BadServer),
										}
									},

									None => return Err(Error::BadServer),
								}
							},
							None => return Err(Error::BadServer),
						}
					},

					_ => return Err(Error::BadServer),
				}
			},

			None => {
				println!("DEBUG: No packets received");
				return Err(Error::BadServer);
			}
		};

		// Parse the params
		// check(&buf[n..], Some(MessageName::ReadyForQuery));

		Ok(Server{
			stream: sock,
			buf: vec![0u8; 8192],
		})

		// let code = buf[0];
		// let len = i32::from_be_bytes(buf[1..5].try_into().unwrap());
		// let res = i32::from_be_bytes(buf[5..9].try_into().unwrap());


		// match res {
		// 	5 => {
		// 		let salt = &buf[9..13];
		// 		let password_message = PasswordMessage::new(username, password.unwrap(), salt);
		// 		let hash: Vec<u8> = password_message.into();

		// 	    sock.write(&hash).await;

		// 	    let (read, mut complete) = crate::communication::buffer(&mut sock, &mut buf, 3).await.unwrap();

		// 	    let mut it = 0;
		// 	    while read > it {
		// 	    	match crate::messages::parse(&buf[it..]) {
		// 	    		Some((len, name)) => {
		// 	    			println!("Message: {:?}", name);
		// 	    			it += len + 1;
		// 	    		},
		// 	    		None => break,
		// 	    	};
		// 	    }
		// 	    println!("Server buff: {:?}", String::from_utf8_lossy(&buf));
		// 	    // sock.read(&mut buf).await;
		// 	    // Expect 3 messages here:
		// 	    // Auth ok
		// 	    // Server info
		// 	    // Ready for query

		// 	    println!("DONE WITH SERVER: {}, {}", read, complete);
		// 	    println!("P: {:?}", crate::messages::parse(&buf[9..]).unwrap().1);
		// 	    println!("P: {:?}", crate::messages::parse(&buf[9+23..]).unwrap().1);
		// 	    // println!("{:?}", String::from_utf8_lossy(&buf));
		// 	    return Ok(Server {
		// 	    	stream: sock,
		// 	    })

		// 	},
		// 	0 => {
		// 		// We're in!
		// 	},
		// 	_ =>  {
		// 		println!("ERROR: Unknwon auth requested by server");
		// 	}
		// };

		// Err(Error::BadServer)
	}

	pub async fn send(&mut self, buf: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
		let _ = self.stream.write_all(&buf).await?;
		Ok(())
	}

	// async fn handle()

	async fn handle_message(&self, buf: &[u8]) {

	}
}