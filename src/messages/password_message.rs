use md5::{Md5, Digest};

pub struct PasswordMessage {
    username: String,
    password: String,
    salt: Vec<u8>,
}

impl std::convert::Into<Vec<u8>> for PasswordMessage {
    fn into(self) -> Vec<u8> {
        Vec::<u8>::from(&self)
    }
}

/// Convert to byte stream, actual implementation.
impl std::convert::From<&PasswordMessage> for Vec<u8> {
    fn from(auth_ok: &PasswordMessage) -> Vec<u8> {
        // p
        let mut res = vec![b'p'];

        let mut encrypted_password = auth_ok.encrypt();
        encrypted_password.push(0);

        let len = ((encrypted_password.len() + 4) as i32).to_be_bytes();

        println!("{:?}", encrypted_password);

        res.extend(&len);
        res.extend(&encrypted_password);

        res
    }
}

impl PasswordMessage {
    pub fn new(username: &str, password: &str, salt: &[u8]) -> PasswordMessage {

        assert!(salt.len() == 4);

        PasswordMessage {
            username: username.to_string(),
            password: password.to_string(),
            salt: salt.to_vec(),
        }
    }

    fn encrypt(&self) -> Vec<u8> {
        let mut md5 = Md5::new();

        let password: Vec<u8> = self.password.chars().map(|x| x as u8).collect();
        let username: Vec<u8> = self.username.chars().map(|x| x as u8).collect();

        // First pass
        md5.update(&password);
        md5.update(&username);

        let output = md5.finalize_reset();

        // Second pass
        md5.update(format!("{:x}", output));
        md5.update(&self.salt);

        format!("md5{:x}", md5.finalize()).chars().map(|x| x as u8).collect::<Vec<u8>>()
    }
}