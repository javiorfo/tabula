use database::factory::context;

mod database;

fn main() {
    context("postgres").unwrap().execute("select * from dummies", "host=localhost user=admin password=admin dbname=db_dummy");
}


