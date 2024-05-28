use super::engine::postgres::Postgres;

pub trait Executor {
    fn execute(&self, query: &str, conn_str: &str);
}

pub fn context(engine: &str) -> Result<Box<dyn Executor>, String> {
    match engine {
        "postgres" => Ok(Box::new(Postgres)),
        _ => Err("NO".to_string()),
    }
}
