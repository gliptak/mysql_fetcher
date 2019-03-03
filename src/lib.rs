#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
pub mod config;
pub mod util;

use crate::config::MySqlConfig;
use crate::util::MySqlUtils;
use std::result::Result::Err;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub trait MySqlUpdaterEvents {
    fn get_count_query(&self, db_name: &str, table_name: &str) -> String;
    fn get_fetch_query(&self, db_name: &str, table_name: &str, offset: u64, limit: usize)
        -> String;
    fn total_rows_count(&self, db_name: &str, table_name: &str, num_rows: u64);
    fn rows_fetched(&self, db_name: &str, table_name: &str, offset: u64, rows: Vec<Vec<String>>);
    fn sql_query_error(&self, query: &str, error: &str);
}
pub struct MySqlUpdater {
    config: MySqlConfig,
    shutdown: Arc<AtomicBool>,
}

impl MySqlUpdater {
    pub fn new(config: &MySqlConfig, shutdown: Arc<AtomicBool>) -> MySqlUpdater {
        MySqlUpdater {
            config: config.clone(),
            shutdown,
        }
    }
}

impl MySqlUpdater {
    pub fn run<T>(&mut self, event_handler: T) -> Result<(), String>
    where
        T: MySqlUpdaterEvents + Sized + Send + Sync,
    {
        let pool = match MySqlUtils::create_connection_pool(&self.config.db_config) {
            Ok(p) => p,
            Err(e) => {
                info!(
                    "Shutting Down. Failed to create mysql connection pool. Error: {}",
                    e
                );
                error!(
                    "Shutting Down. Failed to create mysql connection pool. Error: {}",
                    e
                );

                return Err(e);
            }
        };

        while !self.shutdown.load(Ordering::SeqCst) {
            for table_name in self.config.tables.keys() {
                debug!(
                    "get_count_query: db_name:{}, table_name:{}",
                    &self.config.db_config.db_name, &table_name
                );
                let sql_count_query =
                    event_handler.get_count_query(&self.config.db_config.db_name, &table_name);

                debug!("sql_count_query:{}", sql_count_query);
                let total_count;
                let res = MySqlUtils::get_table_count(&pool, &sql_count_query);
                if res.is_err() {
                    event_handler.sql_query_error(&sql_count_query, &res.err().unwrap());
                    continue;
                } else {
                    total_count = res.unwrap();
                }

                event_handler.total_rows_count(
                    &self.config.db_config.db_name,
                    &table_name,
                    total_count,
                );

                debug!("Number of records to fetch: {}", total_count);
                if total_count == 0 {
                    continue;
                }
                let mut iterations = total_count / self.config.fetch_limit as u64;
                if iterations == 0 {
                    iterations = 1;
                }
                let mut offset = 0u64;
                for _i in 0..iterations {
                    if self.shutdown.load(Ordering::SeqCst) {
                        info!("Shutdown received. exiting cache load thread");
                        return Err("Shutdown received. exiting..".to_owned());
                    }
                    let fetch_query = event_handler.get_fetch_query(
                        &self.config.db_config.db_name,
                        &table_name,
                        offset,
                        self.config.fetch_limit,
                    );
                    let res = MySqlUtils::fetch_rows(&pool, &fetch_query, self.config.fetch_limit);
                    if res.is_err() {
                        event_handler.sql_query_error(&sql_count_query, &res.err().unwrap());
                        continue;
                    } else {
                        let rows = res.unwrap();
                        offset += rows.len() as u64;
                        event_handler.rows_fetched(
                            &self.config.db_config.db_name,
                            &table_name,
                            offset,
                            rows,
                        );
                    }
                }
            }
            if !self.shutdown.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(
                    self.config.periodic_fetch_duration.into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
