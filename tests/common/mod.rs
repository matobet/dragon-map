use std::collections::HashMap;

use redis::{Connection, RedisResult, Value};

const TARGET_LIB: &'static str = "/target/debug/libgroomer.so";

fn get_module_path() -> String {
    format!("{}{}", env!("CARGO_MANIFEST_DIR"), TARGET_LIB)
}

pub(crate) fn get_conn() -> RedisResult<Connection> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    client.get_connection()
}

pub(crate) fn get_conn_with_loaded_module() -> RedisResult<Connection> {
    let mut conn = get_conn()?;
    redis::cmd("FLUSHALL").query(&mut conn)?;
    ModuleLoader::new().ensure_loaded(&mut conn)?;
    Ok(conn)
}

pub(crate) struct ModuleLoader {
    module_path: String
}

impl ModuleLoader {
    pub fn new() -> ModuleLoader {
        ModuleLoader { module_path: get_module_path() }
    }

    pub fn safe_unload(&self, conn: &mut Connection) -> RedisResult<()> {
        let modules: Vec<HashMap<String, Value>> = redis::cmd("MODULE").arg("LIST").query(conn)?;
        let already_loaded = modules.iter()
            .map(|m| m.get("name"))
            .any(|name| name == Some(&Value::Data("map".as_bytes().to_vec())));
        if already_loaded {
            redis::cmd("MODULE").arg("UNLOAD").arg("map").query::<bool>(conn)?;
        }
        Ok(())
    }

    pub fn load(&self, conn: &mut Connection) -> RedisResult<bool> {
        redis::cmd("MODULE").arg("LOAD").arg(&self.module_path).query::<bool>(conn)
    }

    pub fn ensure_loaded(&self, conn: &mut Connection) -> RedisResult<()> {
        self.safe_unload(conn)?;
        self.load(conn)?;

        Ok(())
    }
}

pub const METRICS_KEY_COUNT: usize = 4;
