use redis_module::{Context, RedisError, RedisResult, RedisValue, REDIS_OK};

mod get;
mod groom;
mod rem;
mod rem_by_index;
mod set;

pub use get::Get;
pub use groom::EventGroom;
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
        format!(
            "{meta}{}{separator}{}",
            self.namespace(),
            key,
            meta = META_PREFIX,
            separator = SEPARATOR
        )
    }

    fn prefixed_idx(&self, idx: &str, idx_val: &str) -> String {
        format!(
            "{index}{}{separator}{}{separator}{}",
            self.namespace(),
            idx,
            idx_val,
            index = INDEX_PREFIX,
            separator = SEPARATOR
        )
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
        _ => None,
    }
}

fn extract_strings(mut values: Vec<RedisValue>) -> Vec<String> {
    values.drain(..).filter_map(is_string).collect()
}

trait IntoRedisResult<T> {
    fn into_redis_result(self) -> Result<T, RedisError>;
}

impl IntoRedisResult<()> for RedisValue {
    fn into_redis_result(self) -> Result<(), RedisError> {
        Ok(())
    }
}

impl IntoRedisResult<i64> for RedisValue {
    fn into_redis_result(self) -> Result<i64, RedisError> {
        if let RedisValue::Integer(n) = self {
            Ok(n)
        } else {
            Err(RedisError::String("command returned non-integer response!".to_string()))
        }
    }
}

impl IntoRedisResult<bool> for RedisValue {
    fn into_redis_result(self) -> Result<bool, RedisError> {
        self.into_redis_result().map(|n: i64| n == 1)
    }
}

impl IntoRedisResult<Vec<String>> for RedisValue {
    fn into_redis_result(self) -> Result<Vec<String>, RedisError> {
        if let RedisValue::Array(values) = self {
            Ok(extract_strings(values))
        } else {
            Err(RedisError::String("command didn't return a list of strings!".to_string()))
        }
    }
}

trait Contextual {
    fn context(&self) -> &Context;

    fn call<R>(&self, cmd: &str, args: &[&str]) -> Result<R, RedisError>
    where
        RedisValue: IntoRedisResult<R>,
    {
        self.context().call(cmd, args)?.into_redis_result()
    }

    fn exists(&self, key: &str) -> Result<bool, RedisError> {
        self.call("EXISTS", &[key])
    }

    fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        self.call("SET", &[key, value])
    }

    fn incr(&self, key: &str) -> Result<i64, RedisError> {
        self.call("INCR", &[key])
    }

    fn smembers(&self, key: &str) -> Result<Vec<String>, RedisError> {
        self.call("SMEMBERS", &[key])
    }

    fn srandmember(&self, key: &str, n: usize) -> Result<Vec<String>, RedisError> {
        self.call("SRANDMEMBER", &[key, &n.to_string()])
    }

    fn srem(&self, key: &str, value: &str) -> Result<i64, RedisError> {
        self.call("SREM", &[key, value])
    }

    fn hgetall(&self, key: &str) -> Result<Vec<String>, RedisError> {
        self.call("HGETALL", &[key])
    }
}

trait CleanOperation: Contextual + Namespaced {
    fn del(&self, key: &str) -> Result<bool, RedisError> {
        self.call("DEL", &[key])
    }

    fn clean_key(&self, key: &str) -> Result<(), RedisError> {
        self.del(&self.prefixed(key))?;

        let meta_key = self.prefixed_meta(key);
        let meta = self.hgetall(&meta_key)?;
        if meta.is_empty() {
            self.incr("meta_missing")?;
        } else {
            for pair in meta.chunks_exact(2) {
                let idx = &pair[0];
                let idx_val = &pair[1];
                self.rm_from_index(key, idx, idx_val)?;
            }
            self.del(&meta_key)?;
        }
        Ok(())
    }

    fn rm_from_index(&self, key: &str, idx: &str, idx_val: &str) -> RedisResult {
        self.srem(&self.prefixed_idx(idx, idx_val), key)?;

        REDIS_OK
    }
}

pub struct Init<'a> {
    ctx: &'a Context,
}

impl<'a> Init<'a> {
    pub fn from(ctx: &'a Context) -> Self {
        Self { ctx }
    }

    pub fn perform(&self) {
        self.set("events", "0").unwrap();
        self.set("keys_expired", "0").unwrap();
        self.set("meta_missing", "0").unwrap();
    }
}

impl<'a> Contextual for Init<'a> {
    fn context(&self) -> &Context {
        self.ctx
    }
}
