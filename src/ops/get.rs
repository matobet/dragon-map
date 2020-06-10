use redis_module::{RedisValue, RedisError, Context, RedisResult};

use super::*;

pub struct Get<'a> {
    ctx: &'a Context,
    namespace: &'a String,
    idx: &'a String,
    idx_val: &'a String
}

impl<'a> Get<'a> {
    pub fn from(ctx: &'a Context, args: &'a Vec<String>) -> Result<Get<'a>, RedisError> {
        if args.len() != 4 {
            return Err(RedisError::WrongArity);
        }

        let namespace = &args[1];
        let idx = &args[2];
        let idx_val = &args[3];

        Ok(Get { ctx, namespace, idx, idx_val })
    }

    pub fn process(&self) -> RedisResult {
        let idx_key = &self.prefixed_idx(self.idx, self.idx_val);
        self.ctx.log_debug(&format!("Invoking SMEMBERS for {}", idx_key));
        let keys = self.smembers(idx_key)?;
        self.ctx.log_debug(&format!("SMEMBERS returned {:?}", keys));
        if keys.is_empty() {
            Ok(RedisValue::Array(vec![]))
        } else {
            let prefixed_keys: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
            let prefixed_keys_str: Vec<&str> = prefixed_keys.iter().map(AsRef::as_ref).collect();
            self.ctx.call("MGET", prefixed_keys_str.as_slice())
        }
    }
}

impl Namespaced for Get<'_> {
    fn namespace(&self) -> &str {
        self.namespace
    }
}

impl Contextual for Get<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}