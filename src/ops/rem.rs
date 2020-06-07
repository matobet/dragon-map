use redis_module::{Context, RedisError, RedisResult, RedisValue};

use super::*;

pub struct Remove<'a> {
    ctx: &'a Context,
    namespace: &'a String,
    idx: &'a String,
    idx_val: &'a String
}

impl<'a> Remove<'a> {
    pub fn from(ctx: &'a Context, args: &'a Vec<String>) -> Result<Remove<'a>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArity);
        }

        let namespace = &args[1];
        let idx = &args[2];
        let idx_val = &args[3];

        Ok(Remove { ctx, namespace, idx, idx_val })
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

impl Namespaced for Remove<'_> {
    fn namespace(&self) -> &String {
        self.namespace
    }
}

impl Operation for Remove<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for Remove<'_> {}
