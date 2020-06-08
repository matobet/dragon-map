use super::*;
use redis_module::RedisValue;
use std::time::Instant;

pub struct PeriodicGroom<'a> {
    ctx: &'a Context,
    cursor: String
}

const INITIAL_CURSOR: &str = "0";
const BATCH_SIZE: usize = 256;

trait ThreadedOperation : Operation {
    fn with_lock<F, T>(&self, f: F) -> T where F : FnOnce() -> T {
        self.context().lock();
        let ret = f();
        self.context().unlock();
        ret
    }
}

impl<'a> PeriodicGroom<'a> {
    pub fn spawn() -> RedisResult {
        std::thread::spawn(|| {
            let ctx = &Context::get_thread_safe_context();
            ctx.log_debug("Starting Groomer Thread");
            let start = Instant::now();
            PeriodicGroom { ctx, cursor: INITIAL_CURSOR.to_string() }.perform();
            ctx.log_debug(&format!("Finishing Groomer Thread in {:?}", start.elapsed()));
        });

        REDIS_OK
    }

    fn perform(&mut self) {
        loop {
            let batch = self.scan_batch();
            for index in &batch {
                let parts: Vec<&str> = index[INDEX_PREFIX.len()..].split(SEPARATOR).collect();
                let namespace = &parts[0].to_string();
                GroomIndex { ctx: self.ctx, namespace, index }.perform().unwrap();
            }
            if &self.cursor == INITIAL_CURSOR {
                break
            }
        }
    }

    fn scan_batch(&mut self) -> Vec<String> {
        let (next_cursor, batch) = match self.with_lock(|| {
            self.ctx.call("SCAN", &[&self.cursor, "MATCH", &format!("{}*", INDEX_PREFIX)]).unwrap()
        }) {
            RedisValue::Array(mut outer) => {
                let next_cursor = outer.drain(..1).next().unwrap();
                match outer.drain(..).next().unwrap() {
                    RedisValue::Array(keys) => (next_cursor, extract_strings(keys)),
                    _ => panic!("SCAN return value's 2nd element should be an array!")
                }
            }
            _ => panic!("SCAN did not return a 2 element array!")
        };

        self.cursor = is_string(next_cursor).unwrap();

        batch
    }
}

impl Operation for PeriodicGroom<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl ThreadedOperation for PeriodicGroom<'_> {}

struct GroomIndex<'a> {
    ctx: &'a Context,
    namespace: &'a String,
    index: &'a String
}

impl GroomIndex<'_> {
    fn perform(&self) -> RedisResult {
        self.ctx.log_debug(&format!("Grooming {}", self.index));
        let keys = self.with_lock(|| self.srandmember(self.index, BATCH_SIZE))?;
        for key in &keys {
            self.with_lock(|| {
                if !self.exists(&self.prefixed(key))? {
                    self.ctx.log_debug(&format!("{} does not exist. Removing {}", self.prefixed(key), key));
                    self.srem(self.index, key)?;
                }
                REDIS_OK
            })?;
        }
        REDIS_OK
    }
}

impl Operation for GroomIndex<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl Namespaced for GroomIndex<'_> {
    fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl ThreadedOperation for GroomIndex<'_> {}