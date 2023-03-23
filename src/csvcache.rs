use crate::*;
 
#[derive(Debug)]
pub struct CSVCache {
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
            // let result = get_headers(&mut reader);
            let val = match reader.headers() {
                Ok(headers) => {
                    Some(
                        headers.iter()
                        .map(|h| h.to_string())
                        .collect::<Vec<String>>()
                    )
                },
                Err(er) => {
                    error!("Error while reading headers: {}", er);
                    None
                },
            };
            val
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

    pub fn header(&self) -> Vec<&str> {
        if self.header.is_none() {
            vec![]
        }
        else {
            self.header.as_ref().unwrap().iter()
                .map(|x| x.as_ref())
                .collect::<Vec<&str>>()
        }
    }

    /// Get the name and type of a column.
    /// This will return the column name, if it exists, or an automatically generated one.
    pub fn column_desc(&self, index: usize) -> (String, String) {
        let auto_name = format!("{}{}", &self.default_column_name, index + 1);
        let column_name = match &self.header {
            Some(header) => {
                header.get(index).unwrap_or(&auto_name)
            },
            None => {
                &auto_name
            },
        };

        (column_name.clone(), "TEXT".to_string())
    }
}