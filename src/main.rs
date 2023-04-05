#![allow(dead_code)]

use std::{
    path::{PathBuf, Path},
};


use log::{debug, error, warn};

use clap::Parser;
use rusqlite::{
    Connection,
    Result,
    params,
};

// Crate modules
mod sql;
use sql::*;

mod csvcache;
use csvcache::*;

// Command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Arguments {
    /// CSV file to operate on.
    /// Use -- for stdin.
    /// TODO: add support for multiple files.
    input: String,

    /// Path to the output file.
    /// Default: use the input path, with .csv replaced by .sqlite.
    #[arg(short, long)]
    output: Option<String>,

    /// Append rows to an existing SQLite database.
    /// The header row must match if this is set.
    #[arg(short, long)]
    #[arg(default_value = "false")]
    append: bool,

    /// Use a specific column as an index.
    /// If this is set to "auto", a new column called "id" will be created with the value being the row number of the CSV file.
    /// If set to blank (""), there will be no index column.
    /// Default: "auto"
    /// [NOT IMPLEMENTED]
    #[arg(short, long)]
    #[arg(default_value = "auto")]
    index_column: Option<String>,

    /// Don't use the first row as the header.
    #[arg(long = "disable-header")]
    #[arg(default_value = "true")]
    #[arg(action = clap::ArgAction::SetFalse)]
    use_header: bool,

    /// Delimiter
    #[arg(long, short = 'd')]
    #[arg(default_value = ",")]
    delimiter: char,

    /// Table name.
    /// If this is not specified, the table name will be constructed from the CSV file name.
    #[arg(long, short = 't')]
    table_name: Option<String>,

    /// Default column name.
    /// Columns with no other name specified will be called <default-column-name><column number>, with a 1-indexed column number. For example, the 20th column will be called "column20" by default.
    /// If --use-header=true and any row has more columns than the header, this is used for the following column, with the 
    /// If headers are disabled, this will be used for all columns.
    /// Default: "column"
    #[arg(long)]
    #[arg(default_value = "column")]
    default_column_name: String,
}

fn main() {
    let args = Arguments::parse();
    env_logger::init();

    // Open the output file, whatever it is, then open the SQLite connection with it.
    let path: PathBuf = if args.output == None {
        // Use input path + .sqlite if no explicit output path is given.
        PathBuf::from(args.input.as_str()).with_extension("db")
    }
    else {
        PathBuf::from(args.output.as_ref().unwrap())
    };

    let conn = Connection::open(path).expect("Error opening sqlite database!");

    // Now, prepare the table 
    let path = PathBuf::from(args.input.clone());
    
    let table_name = match args.table_name.as_ref() {
        Some(value) => value.clone(),
        None => format!("{}", basename(&path).display()),
    };

    // Read the CSV file.
    let cached_csv = CSVCache::load(&args, &path).expect("Error loading file!");

    // Construct the table info.
    // TODO: allow specifying types for columns, or automatically guessing types rather than just using TEXT for everything.
    let header = cached_csv.header();    
    let table_columns = header.iter()
        .map(|h| (h.clone(), "TEXT"))
        .collect::<Vec<(&str, &str)>>();

    // Check index column to make sure it exists.
    if args.index_column.is_some() {
        let column = args.index_column.as_ref().unwrap();
        if column == "auto" {
            // Auto mode.
        }
        else if !header.contains(&column.as_ref()) {
            // Error!
            error!("Index column '{}' doesn't exist!", column);
            panic!("");
        }
    }

    // Make the table in the SQLite database.
    // TODO: handle the index column.
    match create_table(&conn, &table_name, table_columns) {
        Err(er) => error!("Error creating the table: {}", er),
        Ok(()) => (),
    }

    // Now, iterate through the rows from the CSV file and populate the SQLite table.
    let records = cached_csv.rows_iter()
        .map(|x| x.iter()
            .map(|y| y.as_ref())
            .collect::<Vec<&str>>()
        ).collect::<Vec<Vec<&str>>>();

    for (ii, row) in records.iter().enumerate() {
        let res = add_row(&conn, &table_name, &cached_csv.header(), row, None);
        if res.is_err() {
            error!("error adding row #{}: {}", ii + 1, res.unwrap_err());
        }
    }

    // TODO: add a REPL mode after conversion, possibly hidden behind a flag.
    // println!("Now entering REPL mode...");

    // let mut should_run = true;
    // while should_run {
    //     break;
    // }
}

/// Determine if this suffix denotes a file type which we can understand.
/// Currently, this is ".csv" or ".tsv".
fn permissible_suffix(name: &str) -> bool {
    let lower = name.to_lowercase();
    if lower.ends_with(".csv") || lower.ends_with(".tsv") {
        true
    }
    else {
        false
    }
}

/// Remove the suffix and parent directories from a path to get a basename.
fn basename(path: &Path) -> PathBuf {
    let noext_path = path.with_extension("");
    let noparent_path =  noext_path.file_name().unwrap();
    PathBuf::from(noparent_path)
}

/// Populate the table with records from an iterator.
/// `columns` should be the columns of the table, and records should contain the values to populate columns with.
pub fn populate_table(conn: Connection, table_name: &str, _index_column: Option<String>, records: Vec<Vec<&str>>, columns: &Vec<&str>, default_column_name: &str) -> Result<usize> {
    let column_len = columns.len();
    let mut records_written: usize = 0;
    
    for row in records.iter() {
        // We need to know how many columns are in this row.
        let len = row.len();
        if len == 0 
        || len != column_len {
            continue;
        }

        let result = add_row_no_index(&conn, table_name, columns, row.to_vec(), default_column_name);
        if result.is_err() {
            error!("{}", result.unwrap_err());
        } else {
            records_written += 1;
        }
    }
    Ok(records_written)
}

fn add_row_no_index(conn: &Connection, table_name: &str, columns: &Vec<&str>, values: Vec<&str>, default_column_name: &str) -> Result<(), rusqlite::Error> {
    // First, we need to prepare the number of placeholders.
    let longest_row = columns.len().max(values.len());
    let placeholders = "? ".repeat(longest_row);
    let placeholders = placeholders.strip_suffix(" ").unwrap();
    let query = format!(r#"INSERT INTO "{}" ({}) VALUES ({});"#, table_name, placeholders, placeholders);

    // Prepare the parameter arguments.
    let mut param_columns = (0..longest_row)
        .map(|ii| {
            let value = columns.get(ii);
            if value.is_some() && value.unwrap().len() > 0 {
                // If there's a column name defined, use it.
                format!("{}", value.unwrap())
            }
            else {
                // Otherwise, use a default column name.
                format!("{}{}", default_column_name, ii)
            }
        })
        .collect::<Vec<String>>();
    let mut param_values = (0..longest_row)
        .map(|ii| { 
            let value = values.get(ii);
            if value.is_some() {
                format!("{}", value.unwrap())
            }
            else {
                String::from("")
            }
        })
        .collect::<Vec<String>>();

    // Create the row.
    let params = param_columns.append(&mut param_values);
    let result = conn.prepare_cached(&query)?
        .execute(params);
    match result {
        Ok(1) => {
            // All clear!
            Ok(())
        },
        Ok(x) => {
            // A bit fishy - this should only have updated one row.
            warn!("Unexpected number of rows altered: {}", x);
            warn!("Query was: {}", query);
            Ok(())
        },
        Err(er) => {
            Err(er)
        }
    }
}

pub fn add_row_with_index(
    conn: &Connection, 
    table_name: &str, columns: Vec<&str>, values: Vec<&str>, 
    default_column_name: &str, 
    index: Option<(&str, &str)>) -> Result<(), rusqlite::Error> {
    
        // If the index is not defined, just write the row.
    match index {
        None => {
            add_row_no_index(conn, table_name, &columns, values, default_column_name)
        }
        Some((index_column, index_value)) => {
            let mut new_columns = vec![index_column];
            new_columns.extend(columns);
            let mut new_values = vec![index_value];
            new_values.extend(values);
            add_row_no_index(conn, table_name, &new_columns, new_values, default_column_name)
        }
    }
}