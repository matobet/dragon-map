use redis_module::{RedisValue, Context, RedisError, RedisResult, REDIS_OK};

mod get;
mod groom;
mod rem;
mod set;

pub use get::Get;
pub use groom::Groom;
pub use rem::Remove;
pub use set::Set;

const SEPARATOR: char = ':';
const META_PREFIX: &str = "meta_";

trait Namespaced {
    fn namespace(&self) -> &String;

    fn prefixed(&self, key: &String) -> String {
        format!("{}{separator}{}", self.namespace(), key, separator = SEPARATOR)
    }

    fn prefixed_meta(&self, key: &String) -> String {
        format!("{meta}{}{separator}{}", self.namespace(), key, meta = META_PREFIX, separator = SEPARATOR)
    }

    fn prefixed_idx(&self, idx: &String, idx_val: &String) -> String {
        format!("{}{separator}idx_{}{separator}{}", self.namespace(), idx, idx_val, separator = SEPARATOR)
    }
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

    fn smembers(&self, key: &str) -> Result<Vec<String>, RedisError> {
        if let RedisValue::Array(values) = self.context().call("SMEMBERS", &[key])? {
            Ok(extract_strings(values))
        } else {
            Err(RedisError::from(format!("Assertion failed: {} index was not a set!", key)))
        }
    }

    fn hgetall(&self, key: &str) -> Result<Vec<String>, RedisError> {
        if let RedisValue::Array(values) = self.context().call("HGETALL", &[key])? {
            Ok(extract_strings(values))
        } else {
            Err(RedisError::from(format!("Assertion failed: {} meta was not a map!", key)))
        }
    }
}

trait CleanOperation: Operation + Namespaced {
    fn del<T: AsRef<str>>(&self, key: T) -> RedisResult {
        self.context().call("DEL", &[key.as_ref()])
    }

    fn clean_key(&self, key: &String) -> RedisResult {
        self.del(key)?;

        let meta_key = self.prefixed_meta(key);
        let meta = self.hgetall(&meta_key)?;
        if meta.is_empty() {
            REDIS_OK
        } else {
            for pair in meta.chunks_exact(2) {
                let idx = &pair[0];
                let idx_val = &pair[1];
                self.rm_from_index(key, idx, idx_val)?;
            }
            self.del(meta_key)
        }
    }

    fn rm_from_index(&self, key: &String, idx: &String, idx_val: &String) -> RedisResult {
        self.context().call("SREM", &[&self.prefixed_idx(idx, idx_val), key])
    }
}
