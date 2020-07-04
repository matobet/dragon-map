extern crate redis;

use std::collections::HashMap;
use std::iter::Map;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use itertools::Itertools;
use redis::{Cmd, Commands, Connection, ConnectionLike, RedisResult, Value};

mod common;
use common::*;

#[test]
fn test_module_loads() -> RedisResult<()> {
    with_redis_conn(|mut conn| {
        load_module(&mut conn)
    })
}

#[test]
fn test_groom_passes_on_empty_ks() -> RedisResult<()> {
    with_redis_conn(|mut conn| {
        load_module(&mut conn)?;

        redis::cmd("MAP.GROOM").query(&mut conn)?;

        let keys_after: Vec<String> = conn.keys("*")?;
        assert_eq!(keys_after.len(), METRICS_KEY_COUNT);

        Ok(())
    })
}

#[test]
fn test_msetex_simple() -> RedisResult<()> {
    with_redis_conn(|mut conn| {
        load_module(&mut conn)?;

        redis::cmd("MAP.MSETEX_INDEXED")
            .arg("test_ns")
            .arg(10) // expiry
            .arg(0) // num indices
            .arg("k")
            .arg("v")
            .query(&mut conn)?;

        let keys: Vec<String> = conn.keys("*")?;
        assert_eq!(keys.len(), METRICS_KEY_COUNT + 1);

        let value: String = conn.get("test_ns:k")?;
        assert_eq!("v", value);
        Ok(())
    })
}

#[test]
fn test_msetex_multival() -> RedisResult<()> {
    with_redis_conn(|mut conn| {
        load_module(&mut conn)?;

        redis::cmd("MAP.MSETEX_INDEXED")
            .arg("test_ns")
            .arg(10)
            .arg(0)
            .arg("k1").arg("v1")
            .arg("k2").arg("v2")
            .arg("k3").arg("v3")
            .query(&mut conn)?;

        let keys: Vec<String> = conn.keys("*")?;
        assert_eq!(keys.len(), METRICS_KEY_COUNT + 3);

        let values: Vec<String> = conn.get(vec!["test_ns:k1", "test_ns:k2", "test_ns:k3"])?;
        assert_eq!(vec!["v1", "v2", "v3"], values);

        Ok(())
    })
}

#[test]
fn test_msetex_expiry() -> RedisResult<()> {
    with_redis_conn(|mut conn| {
        load_module(&mut conn)?;

        redis::cmd("MAP.MSETEX_INDEXED")
            .arg("test_ns")
            .arg(1)
            .arg(0)
            .arg("k1").arg("v1")
            .arg("k2").arg("v2")
            .query(&mut conn)?;

        sleep(Duration::from_secs(2));

        let values: Vec<String> = conn.get(vec!["test_ns:k1", "test_ns:k2"])?;
        assert!(values.is_empty());

        Ok(())
    })
}

#[test]
fn test_msetex_indexed() -> RedisResult<()> {
    with_redis_conn(|mut conn| {
        load_module(&mut conn)?;

        redis::cmd("MAP.MSETEX_INDEXED")
            .arg("test_ns")
            .arg(10)
            .arg(2).arg("first").arg("second")
            .arg("k1").arg("v1").arg("x").arg("y")
            .arg("k2").arg("v2").arg("w").arg("z")
            .query(&mut conn)?;

        let values: Vec<String> = conn.get(vec!["test_ns:k1", "test_ns:k2"])?;
        assert_eq!(vec!["v1", "v2"], values);

        let first_x_val: Vec<String> = conn.smembers("idx_test_ns:first:x")?;
        assert_eq!(vec!["k1"], first_x_val);

        let second_w_val: Vec<String> = conn.smembers("idx_test_ns:first:w")?;
        assert_eq!(vec!["k2"], second_w_val);

        let first_y_val: Vec<String> = conn.smembers("idx_test_ns:second:y")?;
        assert_eq!(vec!["k1"], first_y_val);

        let second_z_val: Vec<String> = conn.smembers("idx_test_ns:second:z")?;
        assert_eq!(vec!["k2"], second_z_val);

        let meta_k1: HashMap<String, String> = conn.hgetall("meta_test_ns:k1")?;
        assert_eq!(vec!["first", "second"], meta_k1.keys().sorted().collect::<Vec<&String>>());

        assert_eq!("x", meta_k1.get("first").unwrap());
        assert_eq!("y", meta_k1.get("second").unwrap());

        let meta_k2: HashMap<String, String> = conn.hgetall("meta_test_ns:k2")?;
        assert_eq!(vec!["first", "second"], meta_k2.keys().sorted().collect::<Vec<&String>>());

        assert_eq!("w", meta_k2.get("first").unwrap());
        assert_eq!("z", meta_k2.get("second").unwrap());

        Ok(())
    })
}
