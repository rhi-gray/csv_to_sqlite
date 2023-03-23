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

// enum FileType {
//     /// Comma-separated
//     CSV,
//     /// Tab-separated
//     TSV,
//     /// Separated by some other string delimiter
//     Delimited(String),
//     /// Unsupported type!
//     Unsupported,
//     /// Empty
//     Empty,
// }

#[derive(Debug)]
struct CSVCache {
    /// The header row, if it exists.
    /// This is set with a flag, --use-header
    header: Option<Vec<String>>,

    /// The contents of the file.
    /// Due to the very loose restrictions of CSV as a format, this is necessarily vague.
    /// TODO: a more efficient representation.
    rows: Vec<Vec<String>>,

    /// Column name used for otherwise unnamed columns.
    default_column_name: String,
}

impl Default for CSVCache {
    fn default() -> Self {
        CSVCache {
            header: Some(Vec::new()),
            rows: vec![vec![]],
            default_column_name: String::from(""),
        }
    }
}

impl CSVCache {
    pub fn load(args: &Arguments, path: &PathBuf) -> CSVCache {
        // First off, try to load the CSV file.
        // TODO: error handling.
        let csv_data = fs::read_to_string(path).unwrap();
        let mut reader = csv::Reader::from_reader(csv_data.as_bytes());

        // Check the arguments.
        let use_header = args.use_header;
        let header = if use_header {
            // We need to populate the header.
            let result = get_headers(&mut reader);
            Some(result.iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
            )
        } else {
            // Default to no header.
            None
        };

        // Populate the rows.
        let mut rows = vec![];
        for row in reader.records() {
            match row {
                Ok(record) => {
                    if !record.is_empty() {
                        let record = record.iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<String>>();
                        rows.push(record);
                    }
                    ()
                },
                Err(er) => {
                    error!("error reading file: {}", er);
                    ()
                }
            }
        }

        CSVCache { 
            header, rows,
            default_column_name: args.default_column_name.to_string(),
        }
    }

    pub fn rows_iter(&self) -> std::slice::Iter<Vec<String>> {
        self.rows.iter()
    }

    pub fn header(&self) -> Vec<String> {
        if self.header.is_none() {
            vec![]
        }
        else {
            self.header.as_ref().unwrap().iter()
                .map(|x| x.clone())
                .collect::<Vec<String>>()
        }
    }
}

// Command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Arguments {
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

    let csv_data = fs::read_to_string(&args.input).unwrap();
    let mut reader = csv::Reader::from_reader(csv_data.as_bytes());

    // Open the output file, whatever it is, then open the SQLite connection with it.
    let path: PathBuf = if args.output == None {
        // Use input path + .sqlite if no explicit output path is given.
        PathBuf::from(args.input.as_str()).with_extension("sqlite")
    }
    else {
        PathBuf::from(args.output.as_ref().unwrap())
    };

    let conn = Connection::open(path).expect("error opening sqlite database!");
    rusqlite::vtab::array::load_module(&conn).expect("error loading vtab module!");

    // Now, prepare the table 
    let path = PathBuf::from(args.input.clone());
    
    let table_name = match args.table_name.as_ref() {
        Some(value) => value.clone(),
        None => format!("{}", basename(&path).display()),
    };

    // let cached_csv = CSVCache::load(&args, &path);

    let table_columns = get_headers(&mut reader).iter()
        .map(|h| (h.clone(), "VARCHAR(256)"))
        .collect::<Vec<(&str, &str)>>();
    let result = create_table(&conn, table_name.as_str(), table_columns);
    if result.is_err() {
        error!("Error creating the table: {}", result.unwrap_err());
    }

    // Now, iterate through the rows from the CSV file and populate the SQLite table.
    let mut records = vec![];
    for record in reader.records() {
        if record.is_ok() {
            records.push(record.unwrap());
        }
        else {
            // Broken record.
        }
    }
    match populate_table(conn, &table_name, &records, get_headers(&mut reader)) {
        Ok(records_written) => {
            debug!("wrote {} records.", records_written);
            ()
        },
        Err(err) => {
            error!("error in populate_table: {}", err);
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

fn get_headers<R>(rdr: &mut csv::Reader<R>) -> Vec<&str> 
where R: std::io::Read {
    match rdr.headers() {
        Err(_) => vec![],
        Ok(headers) => {
            headers.iter()
                .map(|h| h.clone())
                .collect::<Vec<&str>>()
        }
    }
}
