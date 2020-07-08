use redis_module::{Context, RedisError, RedisResult, REDIS_OK};

use itertools::interleave;

use super::*;

pub struct Set<'a> {
    ctx: &'a Context,
    namespace: &'a String,
    expiry: &'a String,
    indices: &'a [String],
    kv_index_lines: &'a [String]
}

impl<'a> Set<'a> {
    pub fn from(ctx: &'a Context, args: &'a [String]) -> Result<Self, RedisError> {
        if args.len() < 3 {
            return Err(RedisError::WrongArity);
        }

        let namespace = &args[1];
        let expiry = &args[2];
        let index_count = args[3].parse()?;

        if args.len() - 4 < index_count {
            return Err(RedisError::WrongArity);
        }

        if (args.len() - 4 - index_count) % (index_count + 2) != 0 {
            return Err(RedisError::WrongArity);
        }

        let indices = &args[4..4 + index_count];
        let kv_index_lines = &args[4 + index_count..];

        Ok(Self { ctx, namespace, expiry, indices, kv_index_lines })
    }

    pub fn process(&self) -> RedisResult {
        for kv_index_line in self.kv_index_lines.chunks(2 + self.indices.len()) {
            self.process_kv_line(kv_index_line)?;
        }

        REDIS_OK
    }

    fn process_kv_line(&self, kv_index_line: &[String]) -> RedisResult {
        let key = &kv_index_line[0];
        let value = &kv_index_line[1];
        let index_values = &kv_index_line[2..];

        self.ctx.call("SETEX", &[&self.prefixed(key), self.expiry, value])?;

        for (idx, idx_val) in self.indices.iter().zip(index_values) {
            self.add_to_index(key, idx, idx_val)?;
        }

        self.write_meta(key, index_values)
    }

    fn add_to_index(&self, key: &str, idx: &str, idx_val: &str) -> RedisResult {
        self.ctx.call("SADD", &[&self.prefixed_idx(idx, idx_val), key])
    }

    fn write_meta(&self, key: &str, index_values: &[String]) -> RedisResult {
        let mut interleaved: Vec<&str> = interleave(self.indices, index_values).map(|s| s.as_str()).collect();
        if !interleaved.is_empty() {
            let meta = self.prefixed_meta(key);
            interleaved.insert(0, &meta);
            self.ctx.call("HMSET", interleaved.as_slice())?;
        }
        REDIS_OK
    }
}

impl Namespaced for Set<'_> {
    fn namespace(&self) -> &str {
        self.namespace
    }
}

impl Contextual for Set<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}
