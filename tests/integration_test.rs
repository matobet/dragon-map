extern crate redis;

use std::path::{Path, PathBuf};

use redis::{Connection, RedisResult, Value, Commands};
use std::collections::HashMap;

const TARGET_LIB: &'static str = "/target/debug/libgroomer.so";

fn get_module_path() -> String {
    format!("{}{}", env!("CARGO_MANIFEST_DIR"), TARGET_LIB)
}

fn get_conn() -> RedisResult<Connection> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    client.get_connection()
}

fn get_conn_with_loaded_module() -> RedisResult<Connection> {
    let mut conn = get_conn()?;
    redis::cmd("FLUSHALL").query(&mut conn)?;
    ModuleLoader::new().ensure_loaded(&mut conn)?;
    Ok(conn)
}

struct ModuleLoader {
    module_path: String
}

impl ModuleLoader {
    fn new() -> ModuleLoader {
        ModuleLoader { module_path: get_module_path() }
    }

    fn safe_unload(&self, conn: &mut Connection) -> RedisResult<()> {
        let modules: Vec<HashMap<String, Value>> = redis::cmd("MODULE").arg("LIST").query(conn)?;
        let already_loaded = modules.iter()
            .map(|m| m.get("name"))
            .any(|name| name == Some(&Value::Data("map".as_bytes().to_vec())));
        if already_loaded {
            redis::cmd("MODULE").arg("UNLOAD").arg("map").query::<bool>(conn)?;
        }
        Ok(())
    }

    fn load(&self, conn: &mut Connection) -> RedisResult<bool> {
        redis::cmd("MODULE").arg("LOAD").arg(&self.module_path).query::<bool>(conn)
    }

    fn ensure_loaded(&self, conn: &mut Connection) -> RedisResult<()> {
        self.safe_unload(conn)?;
        self.load(conn)?;

        Ok(())
    }
}

#[test]
fn test_module_loads() -> RedisResult<()> {
    let mut conn = get_conn()?;

    let loader = ModuleLoader::new();
    loader.ensure_loaded(&mut conn);

    Ok(())
}

const METRICS_KEY_COUNT: usize = 4;

#[test]
fn test_groom_passes_on_empty_ks() -> RedisResult<()> {
    let mut conn = get_conn_with_loaded_module()?;

    redis::cmd("MAP.GROOM").query(&mut conn)?;

    let keys_after: Vec<String> = conn.keys("*")?;
    assert_eq!(keys_after.len(), METRICS_KEY_COUNT);

    Ok(())
}

#[test]
fn test_msetex_simple() -> RedisResult<()> {
    let mut conn = get_conn_with_loaded_module()?;

    redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(10) // expiry
        .arg(0) // num indices
        .arg("k")
        .arg("v")
        .query(&mut conn)?;

    let keys: Vec<String> = conn.keys("*")?;
    assert_eq!(keys.len(), METRICS_KEY_COUNT + 1);

    assert_eq!("v", conn.get::<&str, String>("test_ns:k")?);

    Ok(())
}

