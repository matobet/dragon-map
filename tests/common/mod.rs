use std::io::{BufRead, BufReader, Error, ErrorKind};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use redis::{Commands, Connection, RedisResult};
pub use redis_test_macros::*;
use std::io;

const TARGET_LIB: &'static str = "/target/debug/libgroomer.so";

const REDIS_BIN: Option<&'static str> = option_env!("REDIS_BIN");

fn get_module_path() -> String {
    format!("{}{}", env!("CARGO_MANIFEST_DIR"), TARGET_LIB)
}

static TEST_REDIS_PORT_NUMBER: AtomicUsize = AtomicUsize::new(6380);

struct ChildRedis { port: usize, child: Child }

impl ChildRedis {
    fn spawn(name: &str) -> RedisResult<ChildRedis> {
        let port = TEST_REDIS_PORT_NUMBER.fetch_add(1, Ordering::SeqCst);
        let log_file = ChildRedis::get_log_file(name)?;
        let child = Command::new(REDIS_BIN.unwrap_or("/usr/bin/redis-server"))
            .arg("--port").arg(port.to_string())
            .arg("--logfile").arg(&log_file)
            .arg("--maxmemory-policy").arg("volatile-lru")
            .spawn()?;

        let log_tail = Command::new("/usr/bin/tail")
            .arg("-F").arg(&log_file)
            .arg("--sleep-interval").arg("0.1")
            .stderr(Stdio::null())
            .stdout(Stdio::piped()).spawn()?;

        let stdout = log_tail.stdout.ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture redis logs"))?;
        let reader = BufReader::new(stdout);

        reader.lines()
            .filter_map(|result| result.ok())
            .find(|line| line.contains("Ready to accept connections"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Redis failed to initialize"))?;

        Ok(ChildRedis { port, child })
    }

    fn get_log_file(name: &str) -> io::Result<String> {
        std::fs::create_dir_all("test_outputs")?;
        let log_file = format!("test_outputs/{}.log", name);
        let _ = std::fs::remove_file(&log_file);
        Ok(log_file)
    }
}

impl Drop for ChildRedis {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

pub(crate) fn with_redis_conn<F>(name: &str, f: F) -> RedisResult<()>
    where F : FnOnce(Connection) -> RedisResult<()> {
    let redis = ChildRedis::spawn(name)?;
    let conn = get_conn(redis.port)?;
    f(conn)?;
    Ok(())
}

pub(crate) fn get_conn(port: usize) -> RedisResult<Connection> {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/", port=port))?;
    client.get_connection()
}

pub(crate) fn load_module(conn: &mut Connection) -> RedisResult<()> {
    redis::cmd("MODULE").arg("LOAD").arg(get_module_path()).query(conn)
}

const METRICS_KEY_COUNT: usize = 3;

pub fn assert_keys_count(conn: &mut Connection, count: usize) -> RedisResult<()> {
    let keys: Vec<String> = conn.keys("*")?;
    assert_eq!(keys.len(), METRICS_KEY_COUNT + count);

    Ok(())
}

pub fn assert_key_value(conn: &mut Connection, expected_value: &str, key: &str) -> RedisResult<()> {
    let value: String = conn.get(key)?;
    assert_eq!(expected_value, value);

    Ok(())
}

pub fn assert_members(conn: &mut Connection, expected: Vec<&str>, key: &str) -> RedisResult<()> {
    let values: Vec<String> = conn.smembers(key)?;
    assert_eq!(expected, values);

    Ok(())
}
