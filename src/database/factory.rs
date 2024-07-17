use super::engine::{mongo::Mongo, postgres::Postgres};

pub trait Executor {
    fn execute(&self, query: &str, conn_str: &str);
}

pub fn context(engine: &str) -> Result<Box<dyn Executor>, String> {
    match engine {
        "postgres" => Ok(Box::new(Postgres)),
        "mongo" => Ok(Box::new(Mongo)),
        _ => Err("NO".to_string()),
    }
}
