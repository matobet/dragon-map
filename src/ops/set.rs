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
    pub fn from(ctx: &'a Context, args: &'a Vec<String>) -> Result<Set<'a>, RedisError> {
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

        Ok(Set { ctx, namespace, expiry, indices, kv_index_lines })
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

    fn add_to_index(&self, key: &String, idx: &String, idx_val: &String) -> RedisResult {
        self.ctx.call("SADD", &[&self.prefixed_idx(idx, idx_val), key])
    }

    fn write_meta(&self, key: &String, index_values: &[String]) -> RedisResult {
        let mut interleaved: Vec<&String> = interleave(self.indices, index_values).collect();
        if !interleaved.is_empty() {
            let meta = self.prefixed_meta(key);
            interleaved.insert(0, &meta);
            let index_args: Vec<&str> = interleaved.iter().map(AsRef::as_ref).collect();
            self.ctx.call("HMSET", index_args.as_slice())?;

            // expire `meta` twice later than the main key so it is evicted before meta
            // and thus has access to the index values
            let expiry_num: usize = self.expiry.parse()?;
            let expiry_double = (expiry_num * 2).to_string();
            self.ctx.call("EXPIRE", &[&meta, &expiry_double])?;
        }
        REDIS_OK
    }
}

// impl <U: AsRef<T>> AsRef<Vec<T>> for Vec<U> {
//     fn as_ref(&self) -> &Vec<T> {
//         self.iter().map(AsRef::as_ref).collect()
//     }
// }

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
