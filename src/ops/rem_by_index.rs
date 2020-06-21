use redis_module::{Context, RedisError, RedisResult, RedisValue};

use super::*;

pub struct RemoveByIndex<'a> {
    ctx: &'a Context,
    namespace: &'a String,
    idx: &'a String,
    idx_val: &'a String
}

impl<'a> RemoveByIndex<'a> {
    pub fn from(ctx: &'a Context, args: &'a [String]) -> Result<RemoveByIndex<'a>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArity);
        }

        let namespace = &args[1];
        let idx = &args[2];
        let idx_val = &args[3];

        Ok(RemoveByIndex { ctx, namespace, idx, idx_val })
    }

    pub fn process(&self) -> RedisResult {
        let idx_key = &self.prefixed_idx(self.idx, self.idx_val);
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
        self.namespace
    }
}

impl Contextual for RemoveByIndex<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for RemoveByIndex<'_> {}
