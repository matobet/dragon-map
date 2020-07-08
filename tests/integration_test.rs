extern crate redis;

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

use itertools::Itertools;
use redis::{Commands, Connection, RedisResult};

mod common;
use common::*;
use std::thread;
use rand::Rng;

#[redis_test]
fn test_module_loads(mut conn: Connection) -> RedisResult<()> {
    load_module(&mut conn)
}

#[redis_test(loaded_module)]
fn test_groom_passes_on_empty_ks(mut conn: Connection) -> RedisResult<()> {
    redis::cmd("MAP.GROOM").query(&mut conn)?;

    assert_keys_count(&mut conn, 0)?;

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_simple(mut conn: Connection) -> RedisResult<()> {
    redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(10) // expiry
        .arg(0) // num indices
        .arg("k")
        .arg("v")
        .query(&mut conn)?;

    assert_keys_count(&mut conn, 1)?;
    assert_key_value(&mut conn, "v","test_ns:k")?;

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_multival(mut conn: Connection) -> RedisResult<()> {
    redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(10)
        .arg(0)
        .arg("k1").arg("v1")
        .arg("k2").arg("v2")
        .arg("k3").arg("v3")
        .query(&mut conn)?;

    assert_keys_count(&mut conn, 3)?;

    let values: Vec<String> = conn.get(vec!["test_ns:k1", "test_ns:k2", "test_ns:k3"])?;
    assert_eq!(vec!["v1", "v2", "v3"], values);

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_expiry(mut conn: Connection) -> RedisResult<()> {
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
}

#[redis_test(loaded_module)]
fn test_msetex_indexed(mut conn: Connection) -> RedisResult<()> {
    redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(10)
        .arg(2).arg("first").arg("second")
        .arg("k1").arg("v1").arg("x").arg("y")
        .arg("k2").arg("v2").arg("w").arg("z")
        .query(&mut conn)?;

    let values: Vec<String> = conn.get(vec!["test_ns:k1", "test_ns:k2"])?;
    assert_eq!(vec!["v1", "v2"], values);

    assert_members(&mut conn, vec!["k1"], "idx_test_ns:first:x")?;
    assert_members(&mut conn, vec!["k2"], "idx_test_ns:first:w")?;
    assert_members(&mut conn, vec!["k1"], "idx_test_ns:second:y")?;
    assert_members(&mut conn, vec!["k2"], "idx_test_ns:second:z")?;

    let meta_k1: HashMap<String, String> = conn.hgetall("meta_test_ns:k1")?;
    assert_eq!(vec!["first", "second"], meta_k1.keys().sorted().collect::<Vec<&String>>());

    assert_eq!("x", meta_k1.get("first").unwrap());
    assert_eq!("y", meta_k1.get("second").unwrap());

    let meta_k2: HashMap<String, String> = conn.hgetall("meta_test_ns:k2")?;
    assert_eq!(vec!["first", "second"], meta_k2.keys().sorted().collect::<Vec<&String>>());

    assert_eq!("w", meta_k2.get("first").unwrap());
    assert_eq!("z", meta_k2.get("second").unwrap());

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_indexed_update(mut conn: Connection) -> RedisResult<()> {
    redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(10)
        .arg(2).arg("first").arg("second")
        .arg("k").arg("v").arg("x").arg("y")
        .query(&mut conn)?;

    redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(10)
        .arg(2).arg("second").arg("third")
        .arg("k").arg("v").arg("y2").arg("z")
        .query(&mut conn)?;

    assert_key_value(&mut conn, "v", "test_ns:k")?;

    assert!(!conn.exists("idx_test_ns:first:x")?);
    assert!(!conn.exists("idx_test_ns:second:y")?);
    assert_members(&mut conn, vec!["k"], "idx_test_ns:second:y2")?;
    assert_members(&mut conn, vec!["k"], "idx_test_ns:third:z")?;

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_index_unique_validation(mut conn: Connection) -> RedisResult<()> {
    match redis::cmd("MAP.MSETEX_INDEXED")
        .arg("test_ns")
        .arg(1)
        .arg(3).arg("duplicate").arg("duplicate").arg("third")
        .arg("k").arg("v").arg("x").arg("y").arg("z")
        .query(&mut conn) {
        Ok(()) => panic!("Expected validation failure"),
        Err(err) => assert_eq!(Some("index names must be unique!"), err.detail())
    }

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_indexed_load(mut conn: Connection) -> RedisResult<()> {
    for i in 1 .. 10_000 {
        redis::cmd("MAP.MSETEX_INDEXED")
            .arg("test_ns")
            .arg(1)
            .arg(3).arg("first").arg("second").arg("third")
            .arg(format!("k{}", i)).arg(format!("v{}", i)).arg(format!("{}", i / 2)).arg(format!("{}", i / 3)).arg(format!("{}", i / 4))
            .query(&mut conn)?;
    }

    thread::sleep(Duration::from_secs(5));

    assert_keys_count(&mut conn, 0)?;

    Ok(())
}

#[redis_test(loaded_module)]
fn test_msetex_indexed_randomized(mut conn: Connection) -> RedisResult<()> {
    let mut rng = rand::thread_rng();

    for i in 1 .. 10_000 {
        let idx_base = rng.gen_range(0, 1000);
        redis::cmd("MAP.MSETEX_INDEXED")
            .arg("test_ns")
            .arg(1)
            .arg(3).arg(idx_base).arg(idx_base + 1).arg(idx_base + 2)
            .arg(format!("{}", rng.gen_range(0, 1000)))
            .arg(format!("v{}", i)).arg(rng.gen_range(0, 100)).arg(rng.gen_range(0, 100)).arg(rng.gen_range(0, 100))
            .query(&mut conn)?;
    }

    thread::sleep(Duration::from_secs(5));

    assert_keys_count(&mut conn, 0)?;

    Ok(())
}