use itertools::Itertools;
use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

use super::*;

pub struct Get<'a> {
    ctx: &'a Context,
    namespace: String,
    idx: String,
    idx_val: String,
}

impl<'a> Get<'a> {
    pub fn from(ctx: &'a Context, args: Vec<RedisString>) -> Result<Self, RedisError> {
        let mut args = args.into_iter().skip(1);

        let namespace = args.next_string()?;
        let idx = args.next_string()?;
        let idx_val = args.next_string()?;

        Ok(Get {
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
            let prefixed_keys = keys.iter().map(|k| self.prefixed(k)).collect_vec();
            let prefixed_keys_str = prefixed_keys.iter().map(AsRef::as_ref).collect_vec();
            self.ctx.call("MGET", prefixed_keys_str.as_slice())
        }
    }
}

impl Namespaced for Get<'_> {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Contextual for Get<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}
