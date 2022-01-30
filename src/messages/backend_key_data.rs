pub struct BackendKeyData {
	len: i32,
	process_id: i32,
	secret_key: i32,
}

impl crate::messages::Message for BackendKeyData {
	fn len(&self) -> i32 {
		self.len
	}

	fn parse(buf: &[u8], len: i32) -> Option<BackendKeyData> {
		// 'K': 1 byte
		// Len: 4 bytes
		let buf = &buf[5..(len as usize) + 1];
		let process_id = i32::from_be_bytes(buf[0..4].try_into().unwrap());
		let secret_key = i32::from_be_bytes(buf[4..8].try_into().unwrap());

		Some(BackendKeyData {
			len: len,
			process_id: process_id,
			secret_key: secret_key,
		})
	}

	fn debug(&self) -> String {
		format!("process_id = {}, backend_secret = {}", self.process_id, self.secret_key)
	}

	fn to_vec(&self) -> Vec<u8> {
		Vec::new()
	}
}