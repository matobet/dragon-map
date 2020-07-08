use redis_module::{Context, LogLevel};

use super::*;

struct RedisIterator<'a> {
    ctx: &'a Context,
    pattern: String
}

impl<'a> RedisIterator<'a> {

    const INITIAL_CURSOR: &'static str = "0";

    fn of(ctx: &'a Context) -> Self {
        Self::of_namespace(ctx, None)
    }

    fn of_namespace(ctx: &'a Context, namespace: Option<&'a str>) -> Self {
        let pattern = match namespace {
            Some(ns) => format!("{prefix}{namespace}*", prefix = INDEX_PREFIX, namespace = ns),
            None => format!("{prefix}*", prefix = INDEX_PREFIX)
        };

        RedisIterator { ctx, pattern }
    }

    fn iter<'b : 'a>(&'b self) -> impl Iterator<Item = String> + 'b {
        unfold(Self::INITIAL_CURSOR.to_string(), move |cursor| {
            if cursor.is_empty() {
                return None
            }

            let (next_cursor, batch) = Self::parse_scan(self.scan(&cursor).unwrap());
            *cursor = next_cursor;
            if cursor == Self::INITIAL_CURSOR {
                *cursor = "".to_string();
            }
            Some(batch)
        }).flatten()
    }

    fn scan(&self, cursor: &str) -> RedisResult {
        self.with_lock(|| {
            self.ctx.call("SCAN", &[cursor, "MATCH", &self.pattern])
        })
    }

    fn parse_scan(ret: RedisValue) -> (String, Vec<String>) {
        match ret {
            RedisValue::Array(mut outer) => {
                let next_cursor = outer.drain(..1).next().unwrap();
                match outer.drain(..).next().unwrap() {
                    RedisValue::Array(keys) => (is_string(next_cursor).unwrap(), extract_strings(keys)),
                    _ => panic!("SCAN return value's 2nd element should be an array!")
                }
            }
            _ => panic!("SCAN did not return a 2 element array!")
        }
    }
}

impl Contextual for RedisIterator<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl Threaded for RedisIterator<'_> {}

pub struct EventGroom<'a> {
    ctx: &'a Context,
    namespace: &'a str,
    key: &'a str,
    is_meta: bool
}

impl<'a> EventGroom<'a> {
    pub fn from(ctx: &'a Context, key_str: &'a str) -> Self {
        let is_meta = key_str.starts_with("meta_");
        let key = if is_meta {
            &key_str[META_PREFIX.len()..]
        } else {
            key_str
        };

        let (namespace, key) = split_namespace(key);
        Self { ctx, namespace, key, is_meta }
    }

    pub fn perform(&self) {
        self.incr("events").unwrap();
        self.incr(if self.is_meta { "metas_expired" } else { "keys_expired" }).unwrap();
        let exists_primary = self.is_meta && self.exists(&self.prefixed(self.key)).unwrap();

        // clean_key ensures that meta is removed in case of key expiry
        // and vice versa that key is removed in case of meta expiry
        self.clean_key(&self.key)
            .unwrap_or_else(|e| self.ctx.log(LogLevel::Warning, &format!("Error grooming key [ {} ]: {}", self.key, e)));

        // additionally in case of meta expiry we want to trigger the Targeted Groomer for this particular key
        if exists_primary {
            self.incr("targeteds").unwrap();
            TargetedGroom::spawn(self.namespace.to_string(), self.key.to_string());
        }
    }
}

impl Namespaced for EventGroom<'_> {
    fn namespace(&self) -> &str {
        self.namespace
    }
}

impl Contextual for EventGroom<'_> {
    fn context(&self) -> &Context {
        &self.ctx
    }
}

impl CleanOperation for EventGroom<'_> {}

use redis_module::RedisValue;
use std::time::Instant;
use itertools::unfold;

pub struct PeriodicGroom<'a> {
    ctx: &'a Context
}

trait Threaded: Contextual {
    fn spawn_thread<F>(name: &'static str, f: F) where F: FnOnce(&Context) + Send + 'static {
        std::thread::spawn( move || {
            let ctx = &Context::get_thread_safe_context();
            ctx.log_debug(&format!("Starting {} Thread", name));
            let start = Instant::now();
            f(ctx);
            ctx.log_debug(&format!("Finishing {} Thread in {:?}", name, start.elapsed()));
        });
    }

    fn with_lock<F, T>(&self, f: F) -> T where F : FnOnce() -> T {
        self.context().lock();
        let ret = f();
        self.context().unlock();
        ret
    }
}

impl<'a> PeriodicGroom<'a> {
    pub fn spawn() {
        Self::spawn_thread("Groomer", |ctx| {
            PeriodicGroom { ctx }.perform();
        });
    }

    fn perform(&mut self) {
        for index in RedisIterator::of(self.ctx).iter() {
            let parts: Vec<&str> = index[INDEX_PREFIX.len()..].split(SEPARATOR).collect();
            let namespace = parts[0];
            GroomIndex { ctx: self.ctx, namespace, index: &index }.perform().unwrap();
        }
    }
}

impl Contextual for PeriodicGroom<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl Threaded for PeriodicGroom<'_> {}

struct GroomIndex<'a> {
    ctx: &'a Context,
    namespace: &'a str,
    index: &'a str
}

impl GroomIndex<'_> {
    const BATCH_SIZE: usize = 256;

    fn perform(&self) -> RedisResult {
        // self.ctx.log_debug(&format!("Grooming {}", self.index));
        let keys = self.with_lock(|| self.srandmember(self.index, Self::BATCH_SIZE))?;
        for key in &keys {
            self.with_lock(|| {
                if !self.exists(&self.prefixed(key))? {
                    if cfg!(debug_assertions) {
                        self.ctx.log_debug(&format!("{} does not exist. Removing {}", self.prefixed(key), key));
                    }
                    self.srem(self.index, key)?;
                }
                REDIS_OK
            })?;
        }
        REDIS_OK
    }
}

impl Contextual for GroomIndex<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl Namespaced for GroomIndex<'_> {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Threaded for GroomIndex<'_> {}

struct TargetedGroom<'a> {
    ctx: &'a Context,
    namespace: String,
    key: String
}

impl<'a> TargetedGroom<'a> {
    fn spawn(namespace: String, key: String) {
        Self::spawn_thread("Targeted Groomer", move |ctx| {
            TargetedGroom { ctx, namespace, key }.perform()
        })
    }

    fn perform(&self) {
        for index in RedisIterator::of_namespace(self.ctx, Some(&self.namespace)).iter() {
            self.with_lock(|| {
                if self.exists(&self.key).unwrap() {
                    // the key has reappeared in Redis, abort
                    return
                }
                self.srem(&index, &self.key).unwrap();
            })
        }
    }
}

impl Contextual for TargetedGroom<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl Namespaced for TargetedGroom<'_> {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Threaded for TargetedGroom<'_> {}