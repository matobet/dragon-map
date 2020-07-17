# DragonMap

A module for [Redis](http://redis.io/) adding support for 2nd level indexes over keys,
supporting individual keys' expiration and automatic index grooming.

## Requirements

because of the usage of experimental [Redis Module API](https://redis.io/topics/modules-api-ref) this module
requires Redis 6+.

## Building and Installation

Build the module

    $ cargo build --release

Load the module to the already running Redis instance

    $  echo "MODULE LOAD $(pwd)/target/release/libdragon_map.so" | redis-cli

Alternatively specify the module to be loaded automatically by Redis in redis.conf

    # redis.conf
    ...
    loadmodule /path/to/built/libdragon_map.so

For details see the [official documentation](https://redis.io/topics/modules-intro).

## Overview

This module exposes the following API.

* `map.msetex_indexed`
* `map.mrem`
* `map.get_by_index`
* `map.rem_by_index`

The most complex method `msetex_indexed` takes care of batch setting of multiple key-value pairs
in a given `<namespace>` (can be used by higher-level application to logically partition/shard the dataset,
e.g. per-tennant in a multi-tenant environemnt/per functionality/per service/...) with a common `<expiry>` (in seconds).

In addition to the value, to each key can be specified a number of indices (number and names of indices must be fixed
for the entire `msetex_indexed` batch).

Example for 2 indices: `city` and `country`

| Key | Value | value of `city` index | value of `country` index |
|-----|-------|-----------------------|--------------------------|
| mendel | `{ serialized BLOB }` | Brno | Czechia |
| einstein | `{ serialized other BLOB }` | Ulm | Germany |
| kafka | `{ serialized yet another BLOB }` | Prague | Czechia |

And the module will make sure to persists such additional structures and metadata in Redis that enable
querying (`get_by_index`) as well as invalidation (`rem_by_index`) of several entries via those index values in
a single atomic operation.

    $ redis-cli # assuming map module already loaded
    127.0.0.1:6379> MAP.MSETEX_INDEXED hello 100 2 city country \ # <namespace> <expiry> <num_indexes> <idx_1_name> ... <idx_n_name>
        mendel <mendel> Brno Czechia \                            # <key> <value> <idx_1_val> ... <idx_n_val>
        einstein <einstein> Ulm Germany \                         # and same for all KV pairs ..
        kafka <kafka> Prague Czechia                              # for the entire batch
    OK
    127.0.0.1:6379> MAP.GET_BY_INDEX hello country Czechia
    1) <kafka>
    2) <mendel>
    127.0.0.1:6379> MAP.REM_BY_INDEX hello city Prague
    (integer) 1 # number of deleted elements
    127.0.0.1:6379> MAP.GET_BY_INDEX hello country Czechia
    1) <mendel>


The module automatically keeps the indexes in redis Sets for each index-value combination,
e.g. `<namespace>:city:Brno` or `<namespace>:conutry:Czechia` .etc. It also listens on internal Redis keyspace events
and when individual keys expire or are evicted, it makes sure the indices in which this key was contained are properly groomed.
It does this by storing the Index -> Value mapping for each key in a extra "meta" Redis Hash. (See `src/ops/set.rs` for details).

## Development & Running Tests

    $ cargo build
    $ cargo test

All tests are integration ones and depend on the `cargo build` being run before. So far I've found no way how to make
`cargo test` generate the same artifacts as cargo build does (namely the libdragon_map.so) which is then later
loaded by the tests into spawned test Redis instances.

## Credits

Huge shoutout to the great https://github.com/RedisLabsModules/redismodule-rs library that makes writing Redis Modules in
Rust a breeze!
