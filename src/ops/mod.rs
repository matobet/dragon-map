use redis_module::{RedisValue, Context, RedisError, RedisResult, REDIS_OK};

mod get;
mod groom;
mod rem;
mod rem_by_index;
mod set;

pub use get::Get;
pub use groom::EventGroom;
pub use groom::PeriodicGroom;
pub use rem::Remove;
pub use rem_by_index::RemoveByIndex;
pub use set::Set;

const SEPARATOR: char = ':';
const META_PREFIX: &str = "meta_";
const INDEX_PREFIX: &str = "idx_";

trait Namespaced {
    fn namespace(&self) -> &str;

    fn prefixed(&self, key: &str) -> String {
        format!("{}{separator}{}", self.namespace(), key, separator = SEPARATOR)
    }

    fn prefixed_meta(&self, key: &str) -> String {
        format!("{meta}{}{separator}{}", self.namespace(), key, meta = META_PREFIX, separator = SEPARATOR)
    }

    fn prefixed_idx(&self, idx: &str, idx_val: &str) -> String {
        format!("{index}{}{separator}{}{separator}{}", self.namespace(), idx, idx_val, index = INDEX_PREFIX, separator = SEPARATOR)
    }
}

fn split_namespace(key: &str) -> (&str, &str) {
    let parts: Vec<&str> = key.split(SEPARATOR).collect();
    (parts[0], parts[1])
}

fn is_string(v: RedisValue) -> Option<String> {
    match v {
        RedisValue::SimpleString(s) => Some(s),
        RedisValue::BulkString(s) => Some(s),
        _ => None
    }
}

fn extract_strings(values: Vec<RedisValue>) -> Vec<String> {
    let mut strings = vec![];
    for val in values {
        is_string(val).map(|s| strings.push(s));
    }
    strings
}

trait Contextual {
    fn context(&self) -> &Context;

    fn exists(&self, key: &str) -> Result<bool, RedisError> {
        self.int_cmd("EXISTS", &[key]).map(|n| n == 1)
    }

    fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        self.context().call("SET", &[key, value]).map(|_|())
    }

    fn incr(&self, key: &str) -> Result<i64, RedisError> {
        self.int_cmd("INCR", &[key])
    }

    fn smembers(&self, key: &str) -> Result<Vec<String>, RedisError> {
        self.string_array_cmd("SMEMBERS", &[key], format!("Assertion failed: {} index was not a set!", key))
    }

    fn srandmember(&self, key: &str, n: usize) -> Result<Vec<String>, RedisError> {
        self.string_array_cmd("SRANDMEMBER", &[key, &n.to_string()],
                              format!("Assertion failed: {} index was not a set!", key))
    }

    fn srem(&self, key: &str, value: &str) -> Result<i64, RedisError> {
        self.int_cmd("SREM", &[key, value])
    }

    fn hgetall(&self, key: &str) -> Result<Vec<String>, RedisError> {
        self.string_array_cmd("HGETALL", &[key], format!("Assertion failed: {} meta was not a map!", key))
    }

    fn int_cmd(&self, cmd: &str, args: &[&str]) -> Result<i64, RedisError> {
        if let RedisValue::Integer(n) = self.context().call(cmd, args)? {
            Ok(n)
        } else {
            Err(RedisError::from(format!("{} returned non-integer response!", cmd)))
        }
    }

    fn string_array_cmd(&self, cmd: &str, args: &[&str], error_msg: String) -> Result<Vec<String>, RedisError> {
        if let RedisValue::Array(values) = self.context().call(cmd, args)? {
            Ok(extract_strings(values))
        } else {
            Err(RedisError::from(error_msg))
        }
    }
}

trait CleanOperation: Contextual + Namespaced {
    fn del(&self, key: &str) -> RedisResult {
        self.context().call("DEL", &[key])
    }

    fn clean_key(&self, key: &str) -> RedisResult {
        self.del(&self.prefixed(key))?;

        let meta_key = self.prefixed_meta(key);
        let meta = self.hgetall(&meta_key)?;
        if meta.is_empty() {
            self.incr("meta_missing")?;
            REDIS_OK
        } else {
            for pair in meta.chunks_exact(2) {
                let idx = &pair[0];
                let idx_val = &pair[1];
                self.rm_from_index(key, idx, idx_val)?;
            }
            self.del(&meta_key)
        }
    }

    fn rm_from_index(&self, key: &str, idx: &str, idx_val: &str) -> RedisResult {
        self.srem(&self.prefixed_idx(idx, idx_val), key)?;

        REDIS_OK
    }
}

pub struct Init<'a> {
    ctx: &'a Context
}

impl<'a> Init<'a> {
    pub fn from(ctx: &'a Context) -> Init<'a> {
        Init { ctx }
    }

    pub fn perform(&self) {
        self.set("events", "0").unwrap();
        self.set("metas_expired", "0").unwrap();
        self.set("keys_expired", "0").unwrap();
        self.set("meta_missing", "0").unwrap();
    }
}

impl<'a> Contextual for Init<'a> {
    fn context(&self) -> &Context {
        self.ctx
    }
}