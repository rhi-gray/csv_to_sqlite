use std::{fs, 
    path::{PathBuf, Path},
};

use csv::StringRecord;
use log::{debug, error, warn};

use clap::Parser;
// use sqlite::{Connection, Statement};
use rusqlite::{
    Connection,
    Result,
    // types::Value, 
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

    /// Should we use a header row?
    /// Default: true
    #[arg(long)]
    #[arg(default_value="true")]
    use_header: bool,

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
    #[arg(default_value="column")]
    default_column_name: String,
}

fn main() {
    let args = Arguments::parse();
    env_logger::init();

    // Open the output file, whatever it is, then open the SQLite connection with it.
    let path: PathBuf = if args.output == None {
        // Use input path + .sqlite if no explicit output path is given.
        PathBuf::from(args.input.as_str()).with_extension("sqlite")
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

    let cached_csv = CSVCache::load(&args, &path);

    let header = cached_csv.header();
    let table_columns = header.iter()
        .map(|h| (h.clone(), "VARCHAR(256)"))
        .collect::<Vec<(&str, &str)>>();
    
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

    match populate_table(conn, &table_name, records, cached_csv.header()) {
        Ok(records_written) => {
            debug!("Wrote {} records.", records_written);
            ()
        },
        Err(err) => {
            error!("Error in populate_table: {}", err);
            ()
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