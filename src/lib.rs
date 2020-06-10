#![feature(type_alias_impl_trait)]

#[macro_use]
extern crate redis_module;

use std::os::raw::{c_char, c_int};

use redis_module::{Context, raw as rawmod, RedisError, RedisResult, RedisString, REDIS_OK};

mod ops;

pub extern "C" fn handle_event(raw_ctx: *mut rawmod::RedisModuleCtx, t: c_int, event: *const c_char, key: *mut rawmod::RedisModuleString) -> c_int {
    let ctx = redis_module::Context::new(raw_ctx);

    let key_str = RedisString::from_ptr(key).unwrap();
    ctx.log_debug(&format!("Evicting {}", key_str));

    let ctx = Context::new(raw_ctx);
    ops::Groom::from(&ctx, key_str).perform();

    rawmod::REDISMODULE_OK as c_int
}

pub extern "C" fn init(raw_ctx: *mut rawmod::RedisModuleCtx) -> c_int {
    if unsafe {
        rawmod::RedisModule_SubscribeToKeyspaceEvents.unwrap()
            (raw_ctx, rawmod::REDISMODULE_NOTIFY_EVICTED as i32 | rawmod::REDISMODULE_NOTIFY_EXPIRED as i32, Some(handle_event))
    } == rawmod::Status::Err as c_int { return rawmod::Status::Err as c_int }

    rawmod::REDISMODULE_OK as c_int
}

// MAP.MSETEX <namespace> <expiry> <num_indices> <idx_1> <idx_2> ... <idx_n>
//               <key_1> <value_1> <idx_1_for_k_1> <idx_2_for_k_1> ... <idx_n_for_k_1>
//               <key_2> <value_2> <idx_1_for_k_2> <idx_2_for_k_2> ... <idx_n_for_k_2>
//               ...
//               <key_n> <value_n> <idx_1_for_k_n> <idx_2_for_k_n> ... <idx_n_for_k_n>
fn msetex_indexed(ctx: &Context, args: Vec<String>) -> RedisResult {
    ops::Set::from(ctx, &args)?.process()
}

// MAP.get_by_index <namespace> idx idx_val
fn get_by_index(ctx: &Context, args: Vec<String>) -> RedisResult {
    ops::Get::from(ctx, &args)?.process()
}

// MAP.rem_by_index <namespace> idx idx_val
fn rem_by_index(ctx: &Context, args: Vec<String>) -> RedisResult {
    ops::RemoveByIndex::from(ctx, &args)?.process()
}

// MAP.mrem <namespace> k1 k2 ... kn
fn mrem(ctx: &Context, args: Vec<String>) -> RedisResult {
    ops::Remove::from(ctx, &args)?.process()
}

fn groom(_: &Context, _: Vec<String>) -> RedisResult {
    ops::PeriodicGroom::spawn();

    REDIS_OK
}

redis_module! {
    name: "map",
    version: 1,
    data_types: [],
    init: init,
    commands: [
        ["map.msetex_indexed", msetex_indexed, "write deny-oom no-cluster", 1, 1, 1],
        ["map.mrem", mrem, "write no-cluster", 1, 1, 1],
        ["map.get_by_index", get_by_index, "readonly no-cluster", 1, 1, 1],
        ["map.rem_by_index", rem_by_index, "write no-cluster", 1, 1, 1],
        ["map.groom", groom, "write no-cluster", 1, 1, 1],
    ]
}