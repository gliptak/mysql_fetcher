
use std::str;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WhereClauseDataType {
    UnixTime,
    ID
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KeyValConfig {
    pub key_index : usize,
    pub val_index : usize,
}

impl KeyValConfig {
    pub fn new(key_index: usize, val_index: usize) -> KeyValConfig {
        KeyValConfig {
            key_index,
            val_index
        }
    }
}

impl Default for KeyValConfig {
    fn default() -> KeyValConfig {
        KeyValConfig {
            key_index : 1,
            val_index : 2
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableConfig {
    pub kanudo_table_name : String,
    pub select_query : String,
    pub count_query : String,
    pub where_clause_type: WhereClauseDataType,
    pub id_index : usize,
    pub key_val_pairs : Vec<KeyValConfig>,
    pub offset: u64,
    pub last_updated : String,
    pub total_rows : u64

}

impl Default for TableConfig {

    fn default() -> TableConfig {
        let mut key_val = Vec::with_capacity(1);
        key_val.push(KeyValConfig::new(1,2));
        key_val.push(KeyValConfig::new(2,3));
        TableConfig {
            kanudo_table_name : "turing_vault_pan".to_string(),
            select_query : "select id, hash, token, enc_value from turing_vault_pan where id > {id} ".to_owned(),
            count_query : "select COUNT(*) from turing_vault_pan where id > {id} ".to_owned(),
            where_clause_type : WhereClauseDataType::ID,
            id_index: 0,
            key_val_pairs : key_val,
            offset : 0,
            last_updated: "".to_string(),
            total_rows: 0
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
    pub read_timeout: u64,
    pub write_timeout: u64,
    pub tcp_connect_timeout: u64,
    pub tcp_keepalive_time: u32,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            host : "localhost".to_owned(),
            port: 3306,
            user: "root".to_owned(),
            password: "root1234".to_owned(),
            db_name: "turing_db".to_owned(),
            read_timeout: 60000,
            write_timeout: 60000,
            tcp_connect_timeout: 60000,
            tcp_keepalive_time: 60000
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MySqlConfig {
    pub enabled: bool,
    pub db_config : DbConfig,
    pub tables : HashMap<String, TableConfig>,
    pub periodic_fetch_duration: u32,
    pub fetch_limit: usize,
}

impl Default for MySqlConfig {
    fn default() -> MySqlConfig {
        let mut tables = HashMap::with_capacity(1);
        tables.insert("turing_vault_pan".to_string(), TableConfig::default());
        MySqlConfig {
            enabled: true,
            db_config:DbConfig::default(),
            tables,
            periodic_fetch_duration: 10000,
            fetch_limit: 5000,

        }
    }
}


impl MySqlConfig {
    pub fn from_file(file: &str) -> MySqlConfig {
        debug!("Config file: {}", file);
        let mut f = File::open(file).unwrap();
        let mut buffer = String::new();
        f.read_to_string(&mut buffer).unwrap();
        debug!("Config file Str: {}", buffer.as_str());
        serde_json::from_str(&buffer).unwrap()
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}