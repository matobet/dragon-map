use std::borrow::Borrow;

use redis_module::{Context, NextArg, RedisError, RedisResult, RedisString, REDIS_OK};

use itertools::{interleave, Itertools};

use super::*;

pub struct Set<'a> {
    ctx: &'a Context,
    namespace: RedisString,
    expiry: RedisString,
    indices: Vec<RedisString>,
    kv_index_lines: Vec<RedisString>,
}

impl<'a> Set<'a> {
    pub fn from(ctx: &'a Context, args: Vec<RedisString>) -> Result<Self, RedisError> {
        let mut args = args.into_iter().skip(1);

        let namespace = args.next_arg()?;
        let expiry = args.next_arg()?;
        let index_count = args.next_u64()? as usize;

        if args.len() < index_count {
            return Err(RedisError::WrongArity);
        }

        if (args.len() - index_count) % (index_count + 2) != 0 {
            return Err(RedisError::WrongArity);
        }

        let indices = (&mut args).take(index_count).map_into().collect_vec();
        if indices.len() != indices.iter().unique().count() {
            return Err(RedisError::Str("ERR index names must be unique!"));
        }

        let kv_index_lines = args.map_into().collect_vec();

        Ok(Self {
            ctx,
            namespace,
            expiry,
            indices,
            kv_index_lines,
        })
    }

    pub fn process(&self) -> RedisResult {
        for kv_index_line in self.kv_index_lines.chunks(2 + self.indices.len()) {
            self.process_kv_line(kv_index_line)?;
        }

        REDIS_OK
    }

    fn process_kv_line(&self, kv_index_line: &[RedisString]) -> RedisResult {
        let key = &kv_index_line[0];
        let value = &kv_index_line[1];
        let index_values = &kv_index_line[2..];

        // in case old value is present we need to make sure old index values are cleared
        if self.exists(&self.prefixed_meta(key.borrow()))? {
            self.clean_key(key.borrow())?;
        }

        self.ctx
            .call("SETEX", &[&self.prefixed(key.borrow()), self.expiry.borrow(), value.borrow()])?;

        for (idx, idx_val) in self.indices.iter().zip(index_values) {
            self.add_to_index(key.borrow(), idx.borrow(), idx_val.borrow())?;
        }

        self.write_meta(key, index_values)
    }

    fn add_to_index(&self, key: &str, idx: &str, idx_val: &str) -> RedisResult {
        self.ctx.call("SADD", &[&self.prefixed_idx(idx, idx_val), key])
    }

    fn write_meta(&self, key: &RedisString, index_values: &[RedisString]) -> RedisResult {
        let mut interleaved: Vec<&str> = interleave(&self.indices, index_values).map(Borrow::borrow).collect_vec();
        if !interleaved.is_empty() {
            let meta = self.prefixed_meta(key.borrow());
            interleaved.insert(0, &meta);
            self.ctx.call("HMSET", interleaved.as_slice())?;
        }
        REDIS_OK
    }
}

impl Namespaced for Set<'_> {
    fn namespace(&self) -> &str {
        self.namespace.borrow()
    }
}

impl Contextual for Set<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for Set<'_> {}
