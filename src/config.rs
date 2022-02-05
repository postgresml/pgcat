#[derive(Clone, PartialEq, Hash, std::cmp::Eq)]
pub struct Address {
    pub host: String,
    pub port: String,
}

#[derive(Clone, PartialEq, Hash, std::cmp::Eq)]
pub struct User {
    pub name: String,
    pub password: String,
}

// #[derive(Clone)]
// pub struct Config {
//     pools: HashMap<String, Pool<ServerPool>>,
// }

// impl Config {
//     pub fn new() -> Config {
//         Config {
//             pools: HashMap::new(),
//         }
//     }
// }
