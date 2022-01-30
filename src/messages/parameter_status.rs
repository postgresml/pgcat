/// ParameterStatus (B)
#[derive(Debug, Clone)]
pub struct ParameterStatus {
	len: i32,
	name: String,
	value: String,
}

impl crate::messages::Message for ParameterStatus {
	fn len(&self) -> i32 {
		self.len
	}

	fn parse(buf: &[u8], len: i32) -> Option<ParameterStatus> {
		// 'S': 1 byte
		// Len: 4 bytes
		let buf = &buf[5..(len + 1) as usize];
		let args = crate::communication::parse_parameters(buf);

		let name = match args.clone().into_keys().next() {
			Some(n) => n,
			None => return None
		};

		let value = match args.into_values().next() {
			Some(v) => v,
			None => return None
		};

		Some(ParameterStatus {
			len: len,
			name: name,
			value: value,
		})
	}

	fn debug(&self) -> String {
		format!("{} = {}", self.name, self.value)
	}

	fn to_vec(&self) -> Vec<u8> {
		Vec::new()
	}
}