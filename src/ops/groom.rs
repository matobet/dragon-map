use redis_module::{Context, LogLevel};

use super::*;

pub struct Groom<'a> {
    ctx: &'a Context,
    namespace: String,
    key: String,
}

impl<'a> Groom<'a> {
    pub fn from(ctx: &'a Context, key_str: &'a str) -> Groom<'a> {
        let key = if key_str.starts_with("meta_") {
            key_str[META_PREFIX.len()..].to_string()
        } else {
            key_str.to_string()
        };

        let parts: Vec<&str> = key.split(SEPARATOR).collect();
        let namespace = parts[0].to_string();
        let key = parts[1].to_string();

        Groom { ctx, namespace, key }
    }

    pub fn perform(&self) {
        self.clean_key(&self.key).map(|_| ())
            .unwrap_or_else(|e| self.ctx.log(LogLevel::Warning, &format!("Error grooming key [ {} ]: {}", self.key, e)));
    }
}

impl Namespaced for Groom<'_> {
    fn namespace(&self) -> &String {
        &self.namespace
    }
}

impl Operation for Groom<'_> {
    fn context(&self) -> &Context {
        &self.ctx
    }
}

impl CleanOperation for Groom<'_> {}
