#[macro_use]
extern crate redis_module;

use redis_module::{Context, NotifyEvent, RedisResult, Status};

mod ops;

fn on_event(ctx: &Context, _event_type: NotifyEvent, _event: &str, key: &str) {
    if cfg!(debug_assetions) {
        ctx.log_debug(&format!("Evicting {}", key));
    }

    ops::EventGroom::from(&ctx, key).perform();
}

fn init(ctx: &Context, _args: &Vec<String>) -> Status {
    ops::Init::from(ctx).perform();

    Status::Ok
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
    ],
    event_handlers: [
        [@EVICTED @EXPIRED: on_event]
    ]
}
