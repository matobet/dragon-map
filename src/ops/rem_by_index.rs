use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

use super::*;

pub struct RemoveByIndex<'a> {
    ctx: &'a Context,
    namespace: String,
    idx: String,
    idx_val: String,
}

impl<'a> RemoveByIndex<'a> {
    pub fn from(ctx: &'a Context, args: Vec<RedisString>) -> Result<Self, RedisError> {
        let mut args = args.into_iter().skip(1);

        let namespace = args.next_string()?;
        let idx = args.next_string()?;
        let idx_val = args.next_string()?;

        Ok(RemoveByIndex {
            ctx,
            namespace,
            idx,
            idx_val,
        })
    }

    pub fn process(&self) -> RedisResult {
        let idx_key = &self.prefixed_idx(&self.idx, &self.idx_val);
        let keys = self.smembers(idx_key)?;
        if keys.is_empty() {
            Ok(RedisValue::Array(vec![]))
        } else {
            for key in &keys {
                self.clean_key(key)?;
            }
            Ok(RedisValue::Integer(keys.len() as i64))
        }
    }
}

impl Namespaced for RemoveByIndex<'_> {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Contextual for RemoveByIndex<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for RemoveByIndex<'_> {}
