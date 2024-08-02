use std::collections::HashMap;

use postgres::{
    types::{FromSql, Type},
    Client, NoTls, Row,
};

use crate::database::factory::Executor;

pub struct Postgres;

#[derive(Debug)]
struct Header {
    name: String,
    length: usize,
}

impl Executor for Postgres {
    fn execute(&self, query: &str, conn_str: &str) {
        let mut client = Client::connect(conn_str, NoTls).unwrap();

        let rows = client.query(query, &[]).unwrap();
        let mut headers: HashMap<_, _> = rows
            .first()
            .unwrap()
            .columns()
            .iter()
            .enumerate()
            .map(|(i, v)| {
                (
                    i + 1,
                    Header {
                        name: format!(" {}", v.name().to_uppercase()),
                        length: v.name().len() + 2,
                    },
                )
            })
            .collect();

        let mut results: Vec<Vec<String>> = Vec::new();
        for row in rows.iter() {
            let string_values = row_to_strings(row);

            let mut columns = Vec::with_capacity(headers.len());
            for (column_index, value) in string_values.iter().enumerate() {
                columns.push(format!(" {}", value));
                let column = headers.get_mut(&(column_index + 1)).unwrap();
                let length = value.len() + 2;
                if column.length < length {
                    column.length = length;
                }
            }
            results.push(columns);
        }
        println!("{headers:?}");
        println!("{results:?}");
        gen(&headers, &results);
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
        .map_or(String::from("NULL"), |v| v.to_string())
}

fn gen(headers: &HashMap<usize, Header>, results: &[Vec<String>]) {
    let corner_up_left = "┏";
    let corner_up_right = "┓";
    let corner_bottom_left = "┗";
    let corner_bottom_right = "┛";
    let div_up = "┳";
    let div_bottom = "┻";
    let hor = "━";
    let vert = "┃";
    let intersection = "╋";
    let vert_left = "┣";
    let vert_right = "┫";

    // header
    let mut header_up = String::from(corner_up_left);
    let mut header_mid = String::from(vert);
    let mut header_bottom = String::from(vert_left);

    let headers_len = headers.len();
    for key in 1..headers_len + 1 {
        let length = headers.get(&key).unwrap().length;
        header_up.push_str(&hor.repeat(length));
        header_bottom.push_str(&hor.repeat(length));
        header_mid.push_str(&add_spaces(&headers.get(&key).unwrap().name, length));
        header_mid.push_str(vert);

        if key < headers_len {
            header_up.push_str(div_up);
            header_bottom.push_str(intersection);
        } else {
            header_up.push_str(corner_up_right);
            header_bottom.push_str(vert_right);
        }
    }

    // results
    let mut table: Vec<String> = vec![header_up, header_mid, header_bottom];
    let rows_len = results.len() - 1;
    let row_fields_len = results.first().unwrap().len() - 1;

    for (i, row) in results.iter().enumerate() {
        let mut value = String::from(vert);
        let mut line = String::new();

        line.push_str(if i < rows_len {
            vert_left
        } else {
            corner_bottom_left
        });

        for (j, field) in row.iter().enumerate() {
            let length = headers.get(&(j + 1)).unwrap().length;

            value.push_str(&add_spaces(field, length));
            value.push_str(vert);

            line.push_str(&hor.repeat(length));

            line.push_str(match (i < rows_len, j < row_fields_len) {
                (true, true) => intersection,
                (true, false) => vert_right,
                (false, true) => div_bottom,
                (false, false) => corner_bottom_right,
            });
        }
        table.push(value);
        table.push(line);
    }

    //     table.iter().for_each(|v| println!("{v}"));
    writing(&table);
}

fn add_spaces(input_string: &str, len: usize) -> String {
    let mut result = String::from(input_string);

    if len > input_string.len() {
        let diff = len - input_string.len();
        result.push_str(&" ".repeat(diff));
    }

    result
}

use std::fs::File;
use std::io::{BufWriter, Write};

fn writing(strings: &[String]) {
    let file = File::create("tabula.out").expect("Unable to create file");
    let mut writer = BufWriter::new(file);

    for v in strings {
        let line = format!("{}\n", v);
        writer
            .write_all(line.as_bytes())
            .expect("Error writing to file");
    }

    writer.flush().expect("Error flushing buffer");
}
