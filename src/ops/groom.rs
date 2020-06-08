use redis_module::{Context, LogLevel};

use super::*;

pub struct Groom<'a> {
    ctx: &'a Context,
    namespace: &'a str,
    key: &'a str,
}

impl<'a> Groom<'a> {
    pub fn from(ctx: &'a Context, key_str: &'a str) -> Groom<'a> {
        let key = if key_str.starts_with("meta_") {
            &key_str[META_PREFIX.len()..]
        } else {
            key_str
        };

        let (namespace, key) = split_namespace(key);
        Groom { ctx, namespace, key }
    }

    pub fn perform(&self) {
        self.clean_key(&self.key).map(|_| ())
            .unwrap_or_else(|e| self.ctx.log(LogLevel::Warning, &format!("Error grooming key [ {} ]: {}", self.key, e)));
    }
}

impl Namespaced for Groom<'_> {
    fn namespace(&self) -> &str {
        self.namespace
    }
}

impl Operation for Groom<'_> {
    fn context(&self) -> &Context {
        &self.ctx
    }
}

impl CleanOperation for Groom<'_> {}
