use std::borrow::Borrow;

use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, RedisValue};

use super::*;

pub struct RemoveByIndex<'a> {
    ctx: &'a Context,
    namespace: RedisString,
    idx: RedisString,
    idx_val: RedisString,
}

impl<'a> RemoveByIndex<'a> {
    pub fn from(ctx: &'a Context, args: Vec<RedisString>) -> Result<Self, RedisError> {
        let mut args = args.into_iter().skip(1);

        let namespace = args.next_arg()?;
        let idx = args.next_arg()?;
        let idx_val = args.next_arg()?;

        Ok(RemoveByIndex {
            ctx,
            namespace,
            idx,
            idx_val,
        })
    }

    pub fn process(&self) -> RedisResult {
        let idx_key = &self.prefixed_idx(self.idx.borrow(), self.idx_val.borrow());
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
        self.namespace.borrow()
    }
}

impl Contextual for RemoveByIndex<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for RemoveByIndex<'_> {}
