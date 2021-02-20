use redis_module::{Context, LogLevel};

use super::*;

pub struct EventGroom<'a> {
    ctx: &'a Context,
    namespace: &'a str,
    key: &'a str,
}

impl<'a> EventGroom<'a> {
    pub fn from(ctx: &'a Context, key: &'a str) -> Self {
        let (namespace, key) = split_namespace(key);
        Self { ctx, namespace, key }
    }

    pub fn perform(&self) {
        self.incr("events").unwrap();
        self.incr("keys_expired").unwrap();

        // clean_key ensures that meta is removed in case of key expiry
        // and vice versa that key is removed in case of meta expiry
        self.clean_key(&self.key).unwrap_or_else(|e| {
            self.ctx
                .log(LogLevel::Warning, &format!("Error grooming key [ {} ]: {}", self.key, e))
        });
    }
}

impl Namespaced for EventGroom<'_> {
    fn namespace(&self) -> &str {
        self.namespace
    }
}

impl Contextual for EventGroom<'_> {
    fn context(&self) -> &Context {
        &self.ctx
    }
}

impl CleanOperation for EventGroom<'_> {}
