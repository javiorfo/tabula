use mongodb::{
    bson::{doc, Document},
    sync::{Client, Collection},
};

use crate::database::factory::Executor;

pub struct Mongo;

impl Executor for Mongo {
    fn execute(&self, query: &str, conn_str: &str) {
        let client = Client::with_uri_str(conn_str).unwrap();
        let db = client.database("db_dummy");

        let collection: Collection<Document> = db.collection::<Document>("dummies");

        let filter = doc! {};
        let cursor = collection.find(filter).run().unwrap();

        for v in cursor {
            println!("{}", v.unwrap());
            println!();
        }
    }
}
