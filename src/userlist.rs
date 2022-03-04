use arc_swap::{ArcSwap, Guard};
use log::{error};
use once_cell::sync::Lazy;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use std::collections::{HashMap};
use std::sync::Arc;

use crate::errors::Error;

pub type UserList = HashMap<String, String>;
static USER_LIST: Lazy<ArcSwap<UserList>> = Lazy::new(|| ArcSwap::from_pointee(HashMap::new()));

pub fn get_user_list() -> Guard<Arc<UserList>> {
    USER_LIST.load()
}

/// Parse the user list.
pub async fn parse(path: &str) -> Result<(), Error> {
    let mut contents = String::new();
    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            error!("Could not open '{}': {}", path, err.to_string());
            return Err(Error::BadConfig);
        }
    };

    match file.read_to_string(&mut contents).await {
        Ok(_) => (),
        Err(err) => {
            error!("Could not read config file: {}", err.to_string());
            return Err(Error::BadConfig);
        }
    };

    let map: HashMap<String, String> = serde_json::from_str(&contents).expect("JSON was not well-formatted");



    USER_LIST.store(Arc::new(map.clone()));

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_config() {
        parse("userlist.json").await.unwrap();
        assert_eq!(get_user_list()["sven"], "clear_text_password");
        assert_eq!(get_user_list()["sharding_user"], "sharding_user");
    }
}
