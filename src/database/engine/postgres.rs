use std::collections::HashMap;

use postgres::{
    types::{FromSql, Type},
    Client, NoTls, Row,
};

use crate::database::factory::Executor;

pub struct Postgres;

impl Executor for Postgres {
    fn execute(&self, query: &str, conn_str: &str) {
        let mut client = Client::connect(
            conn_str,
            NoTls,
        )
        .unwrap();
        let rows = client.query(query, &[]).unwrap();
        let mut headers: HashMap<_, _> = rows
            .first()
            .unwrap()
            .columns()
            .iter()
            .enumerate()
            .map(|(i, v)| (i + 1, (v.name(), v.name().len())))
            .collect();

        let mut results = HashMap::<usize, Vec<String>>::new();
        for (row_index, row) in rows.iter().enumerate() {
            let string_values = row_to_strings(row);

            let mut columns = Vec::with_capacity(headers.len());
            for (column_index, value) in string_values.iter().enumerate() {
                columns.push(value.to_string());
                let column = headers.get_mut(&(column_index + 1)).unwrap();
                let length = value.len();
                if column.1 < length {
                    column.1 = length;
                }
            }
            results.insert(row_index + 1, columns);
        }
        println!("{headers:?}");
        println!("{results:?}");
    }
}

fn row_to_strings(row: &Row) -> Vec<String> {
    let mut result = Vec::new();

    for (i, column) in row.columns().iter().enumerate() {
        let value = match *column.type_() {
            Type::BOOL => get_value_or_null_string::<bool>(row, i),
            Type::INT2 => get_value_or_null_string::<i16>(row, i),
            Type::INT4 => get_value_or_null_string::<i32>(row, i),
            Type::INT8 => get_value_or_null_string::<i64>(row, i),
            Type::FLOAT4 => get_value_or_null_string::<f32>(row, i),
            Type::FLOAT8 => get_value_or_null_string::<f64>(row, i),
            Type::TEXT | Type::VARCHAR => get_value_or_null_string::<&str>(row, i),
            //             Type::BYTEA => row.try_get::<_, Option<&[u8]>>(i).ok().flatten().map_or("NULL".to_string(), |v| format!("{:?}", v)),
            Type::DATE => get_value_or_null_string::<chrono::NaiveDate>(row, i),
            Type::TIMESTAMP => get_value_or_null_string::<chrono::NaiveDateTime>(row, i),
            Type::TIMESTAMPTZ => get_value_or_null_string::<chrono::NaiveTime>(row, i),
            //             Type::UUID => row.try_get::<_, Option<uuid::Uuid>>(i).ok().flatten().map_or("NULL".to_string(), |v| v.to_string()),
            //             Type::JSON | Type::JSONB => row.try_get::<_, Option<serde_json::Value>>(i).ok().flatten().map_or("NULL".to_string(), |v| v.to_string()),
            _ => "UNKNOWN TYPE".to_string(),
        };
        result.push(value);
    }

    result
}

fn get_value_or_null_string<'a, T>(row: &'a Row, i: usize) -> String
where
    T: ToString + FromSql<'a>,
{
    row.try_get::<_, Option<T>>(i)
        .ok()
        .flatten()
        .map_or("NULL".to_string(), |v| v.to_string())
}
