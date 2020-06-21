use super::*;

pub struct Remove<'a> {
    ctx: &'a Context,
    namespace: &'a String,
    keys: &'a [String]
}

impl<'a> Remove<'a> {
    pub fn from(ctx: &'a Context, args: &'a [String]) -> Result<Remove<'a>, RedisError> {
        if args.len() < 2 {
            return Err(RedisError::WrongArity)
        }

        let namespace = &args[1];
        let keys = &args[2..];

        Ok(Remove { ctx, namespace, keys })
    }

    pub fn process(&self) -> RedisResult {
        for key in self.keys {
            self.clean_key(key)?;
        }
        Ok(RedisValue::Integer(self.keys.len() as i64))
    }
}

impl Namespaced for Remove<'_> {
    fn namespace(&self) -> &str {
        self.namespace
    }
}

impl Contextual for Remove<'_> {
    fn context(&self) -> &Context {
        self.ctx
    }
}

impl CleanOperation for Remove<'_> {}
