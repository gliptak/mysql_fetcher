use crate::config::DbConfig;
use mysql;
use mysql::{from_row, from_value, Value};
use std;
pub struct MySqlUtils {}
impl MySqlUtils {
    pub fn create_connection_pool(conf: &DbConfig) -> Result<mysql::Pool, String> {
        info!("Creating a MySQL connection pool");
        let mut builder = mysql::OptsBuilder::default();
        builder
            .user(Some(conf.user.clone()))
            .pass(Some(conf.password.clone()))
            .ip_or_hostname(Some(conf.host.clone()))
            .tcp_port(conf.port)
            .db_name(Some(conf.db_name.clone()))
            .read_timeout(Some(std::time::Duration::from_millis(conf.read_timeout)))
            .write_timeout(Some(std::time::Duration::from_millis(conf.write_timeout)))
            .tcp_connect_timeout(Some(std::time::Duration::from_millis(
                conf.tcp_connect_timeout,
            )))
            .tcp_keepalive_time_ms(Some(conf.tcp_keepalive_time));
        info!("Connecting to MySQL Server");
        let pool = match mysql::Pool::new_manual(1, 2, builder) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    "Failed to create mysql connection pool. Error:{}",
                    e.to_string()
                );
                return Err(e.to_string());
            }
        };
        info!("Successfully created mysql connection pool");

        Ok(pool)
    }
    pub fn convert_to_sql_string(val: mysql::Value) -> String {
        match val {
            Value::NULL => "".into(),
            Value::Int(x) => format!("{}", x),
            Value::UInt(x) => format!("{}", x),
            Value::Float(x) => format!("{}", x),
            Value::Date(y, m, d, 0, 0, 0, 0) => format!("{:04}-{:02}-{:02}", y, m, d),
            Value::Date(y, m, d, h, i, s, 0) => {
                format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, i, s)
            }
            Value::Date(y, m, d, h, i, s, u) => format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
                y, m, d, h, i, s, u
            ),
            Value::Time(neg, d, h, i, s, 0) => {
                if neg {
                    format!("-{:03}:{:02}:{:02}", d * 24 + u32::from(h), i, s)
                } else {
                    format!("{:03}:{:02}:{:02}", d * 24 + u32::from(h), i, s)
                }
            }
            Value::Time(neg, d, h, i, s, u) => {
                if neg {
                    format!("-{:03}:{:02}:{:02}.{:06}", d * 24 + u32::from(h), i, s, u)
                } else {
                    format!("{:03}:{:02}:{:02}.{:06}", d * 24 + u32::from(h), i, s, u)
                }
            }
            Value::Bytes(bytes) => match String::from_utf8(bytes.to_vec()) {
                Ok(s) => s,
                Err(_) => {
                    let mut s = String::from("0x");
                    for c in &bytes {
                        s.extend(format!("{:02X}", *c).chars())
                    }
                    s
                }
            },
        }
    }
    pub fn get_table_count(pool: &mysql::Pool, query: &str) -> Result<u64, String> {
        debug!("sql_table_count_query:{}", query);
        let mut total_count = 0u64;
        let _res = pool
            .prep_exec(query, ())
            .map(|mut result| {
                trace!("sql_count_query result:{:?}", result);

                let r = result.next().unwrap().unwrap();
                trace!("sql_count_query row:{:?}", r);
                total_count = mysql::from_row(r);
                /*for row in result {
                    let r = row.unwrap();
                    trace!("sql_count_query row:{:?}", r);
                    total_count = mysql::from_row(r);
                    break;

                }*/
            })
            .map_err(|err| {
                // All tasks must have an `Error` type of `()`. This forces error
                // handling and helps avoid silencing failures.
                //
                // In our example, we are only going to log the error to STDOUT.
                error!("Failed to fetch row. Error {:?}", err);
                //return Err(format!("{:?}", err));
            });

        Ok(total_count)
    }

    pub fn fetch_rows(
        pool: &mysql::Pool,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Vec<String>>, String> {
        debug!("sql_fetch_rows_query:{}", query);
        let mut rows = Vec::with_capacity(limit);
        let _res = pool
            .prep_exec(&query, ())
            .map(|result| {
                for row in result {
                    let r = row.unwrap();
                    debug!("row:{:?}", r);
                    let mut row_vec = Vec::<String>::with_capacity(12);
                    for val in r.unwrap() {
                        debug!("val:{}", val.as_sql(true));
                        row_vec.push(MySqlUtils::convert_to_sql_string(val));
                    }
                    rows.push(row_vec);
                }
            })
            .map_err(|err| {
                // All tasks must have an `Error` type of `()`. This forces error
                // handling and helps avoid silencing failures.
                //
                // In our example, we are only going to log the error to STDOUT.
                error!("Failed to fetch row. Error {:?}", err);
                //Err(err.to_string())
            });
        Ok(rows)
    }
}
