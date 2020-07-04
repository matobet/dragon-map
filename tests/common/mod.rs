#![feature(const_if_match)]

use std::collections::HashMap;

use redis::{Connection, RedisResult, Value};
use std::process::{Command, Child, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::{BufReader, BufRead, Error, ErrorKind};

const TARGET_LIB: &'static str = "/target/debug/libgroomer.so";

const REDIS_BIN: Option<&'static str> = option_env!("REDIS_BIN");

fn get_module_path() -> String {
    format!("{}{}", env!("CARGO_MANIFEST_DIR"), TARGET_LIB)
}

static TEST_REDIS_PORT_NUMBER: AtomicUsize = AtomicUsize::new(6380);

struct ChildRedis { port: usize, child: Child }

impl ChildRedis {
    fn spawn() -> RedisResult<ChildRedis> {
        let port = TEST_REDIS_PORT_NUMBER.fetch_add(1, Ordering::SeqCst);
        let mut child = Command::new(REDIS_BIN.unwrap_or("/usr/bin/redis-server"))
            .arg("--port").arg(port.to_string())
            .stdout(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.as_mut().ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture redis stdout"))?;
        let reader = BufReader::new(stdout);

        reader.lines()
            .filter_map(|result| result.ok())
            .find(|line| line.contains("Ready to accept connections"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "Redis failed to initialize"))?;

        Ok(ChildRedis { port, child })
    }
}

impl Drop for ChildRedis {
    fn drop(&mut self) {
        self.child.kill();
    }
}

pub(crate) fn with_redis_conn<F>(f: F) -> RedisResult<()>
    where F : FnOnce(Connection) -> RedisResult<()> {
    let redis = ChildRedis::spawn()?;
    let conn = get_conn(redis.port)?;
    f(conn)?;
    Ok(())
}

pub(crate) fn get_conn(port: usize) -> RedisResult<Connection> {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/", port=port))?;
    client.get_connection()
}

pub(crate) struct ModuleLoader {
    module_path: String
}

pub(crate) fn load_module(conn: &mut Connection) -> RedisResult<()> {
    redis::cmd("MODULE").arg("LOAD").arg(get_module_path()).query(conn)
}

pub const METRICS_KEY_COUNT: usize = 4;
