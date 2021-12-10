use std::borrow::Borrow;

use itertools::Itertools;
use redis_module::{NextArg, RedisString};

use super::*;

pub struct Remove<'a> {
    ctx: &'a Context,
    namespace: RedisString,
    keys: Vec<RedisString>,
}

impl<'a> Remove<'a> {
    pub fn from(ctx: &'a Context, args: Vec<RedisString>) -> Result<Self, RedisError> {
        let mut args = args.into_iter().skip(1);

        let namespace = args.next_arg()?;
        let keys = args.collect_vec();

        Ok(Remove { ctx, namespace, keys })
    }

    pub fn process(&self) -> RedisResult {
        for key in &self.keys {
            self.clean_key(key.borrow())?;
        }
        Ok(RedisValue::Integer(self.keys.len() as i64))
    }
}

impl Namespaced for Remove<'_> {
    fn namespace(&self) -> &str {
        self.namespace.borrow()
    }
}

impl Contextual for Remove<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for Remove<'_> {}
