use redis_module::{RedisValue, Context, RedisError, RedisResult, REDIS_OK};

mod get;
mod groom;
mod rem;
mod set;
mod periodic;

pub use get::Get;
pub use groom::Groom;
pub use rem::Remove;
pub use set::Set;
pub use periodic::PeriodicGroom;

const SEPARATOR: char = ':';
const META_PREFIX: &str = "meta_";
const INDEX_PREFIX: &str = "idx_";

trait Namespaced {
    fn namespace(&self) -> &str;

    fn prefixed<S: AsRef<str>>(&self, key: S) -> String {
        format!("{}{separator}{}", self.namespace(), key.as_ref(), separator = SEPARATOR)
    }

    fn prefixed_meta<S: AsRef<str>>(&self, key: S) -> String {
        format!("{meta}{}{separator}{}", self.namespace(), key.as_ref(), meta = META_PREFIX, separator = SEPARATOR)
    }

    fn prefixed_idx<S: AsRef<str>, T: AsRef<str>>(&self, idx: S, idx_val: T) -> String {
        format!("{index}{}{separator}{}{separator}{}", self.namespace(), idx.as_ref(), idx_val.as_ref(), index = INDEX_PREFIX, separator = SEPARATOR)
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

trait Operation {
    fn context(&self) -> &Context;

    fn exists<S: AsRef<str>>(&self, key: S) -> Result<bool, RedisError> {
        self.int_cmd("EXISTS", &[key.as_ref()]).map(|n| n == 1)
    }

    fn smembers<S: AsRef<str>>(&self, key: S) -> Result<Vec<String>, RedisError> {
        self.string_array_cmd("SMEMBERS", &[key.as_ref()], format!("Assertion failed: {} index was not a set!", key.as_ref()))
    }

    fn srandmember<S: AsRef<str>>(&self, key: S, n: usize) -> Result<Vec<String>, RedisError> {
        self.string_array_cmd("SRANDMEMBER", &[key.as_ref(), &n.to_string()],
                              format!("Assertion failed: {} index was not a set!", key.as_ref()))
    }

    fn srem<S: AsRef<str>, T: AsRef<str>>(&self, key: S, value: T) -> Result<i64, RedisError> {
        self.int_cmd("SREM", &[key.as_ref(), value.as_ref()])
    }

    fn hgetall<S: AsRef<str>>(&self, key: S) -> Result<Vec<String>, RedisError> {
        self.string_array_cmd("HGETALL", &[key.as_ref()], format!("Assertion failed: {} meta was not a map!", key.as_ref()))
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

trait CleanOperation: Operation + Namespaced {
    fn del<S: AsRef<str>>(&self, key: S) -> RedisResult {
        self.context().call("DEL", &[key.as_ref()])
    }

    fn clean_key<S: AsRef<str>>(&self, key: S) -> RedisResult {
        self.del(self.prefixed(key.as_ref()))?;

        let meta_key = self.prefixed_meta(key.as_ref());
        let meta = self.hgetall(&meta_key)?;
        if meta.is_empty() {
            REDIS_OK
        } else {
            for pair in meta.chunks_exact(2) {
                let idx = &pair[0];
                let idx_val = &pair[1];
                self.rm_from_index(key.as_ref(), idx, idx_val)?;
            }
            self.del(meta_key)
        }
    }

    fn rm_from_index<S: AsRef<str>>(&self, key: S, idx: &String, idx_val: &String) -> RedisResult {
        self.srem(self.prefixed_idx(idx, idx_val), key)?;

        REDIS_OK
    }
}