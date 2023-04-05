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

    /// Maximum column count.
    /// The number of columns in the row that has the most columns.
    max_column_count: usize,

    /// Column name used for otherwise unnamed columns.
    default_column_name: String,
}

impl Default for CSVCache {
    fn default() -> Self {
        CSVCache {
            header: Some(Vec::new()),
            rows: vec![vec![]],
            max_column_count: 0,
            default_column_name: String::from(""),
        }
    }
}

impl CSVCache {
    pub fn load(args: &Arguments, path: &PathBuf) -> Result<CSVCache, csv::Error> {
        // Load the CSV reader with arguments.
        // TODO: error handling.
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(args.use_header)
            .delimiter(args.delimiter as u8)
            .flexible(true)
            .comment(Some('#' as u8))
            .from_path(path)?;

        // Keep track of this throughout the function.
        let mut max_column_count = 0;

        // Check the arguments.
        let mut header = if args.use_header {
            // We need to populate the header.
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

            // Set the max column count.
            max_column_count = match val.as_ref() { Some(x) => x.len(), None => 0 };

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
                        // Make a copy.
                        let record = record.iter()
                            .map(|x| x.to_string())
                            .collect::<Vec<String>>();
                        // This might be a longer row.
                        max_column_count = max_column_count.max(record.len());
                        rows.push(record);
                    }
                    ()
                },
                Err(er) => {
                    error!("Error reading CSV file: {}", er);
                    ()
                }
            }
        }

        // Now, if the max_column_count is greater than the length of the header row, pad it.
        if let Some(ref mut header) = header { 
            if max_column_count > header.len() {
                for ii in header.len() .. max_column_count {
                    header.push(format!("{}{}", args.default_column_name, ii + 1));
                }
            }
        }

        Ok(
            CSVCache { 
                header, rows,
                max_column_count,
                default_column_name: args.default_column_name.to_string(),
            }
        )
    }

    /// Find the length of the longest row.
    pub fn longest_row(&self) -> usize {
        let mut max_len = match self.header.as_ref(){ 
            Some(vec) => vec.len(),
            None => 0,
        };
        for row in &self.rows {
            max_len = max_len.max(row.len())
        }
        max_len
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

    /// Return the nth element of each row.
    /// If a row doesn't have the right number of columns, return None.
    /// If the requested column is outside any row, return an empty vector.
    pub fn get_nth_in_rows(&self, column: usize) -> Vec<Option<&str>> {
        if column > self.max_column_count {
            return vec![];
        }

        let mut result = vec![];
        for row in &self.rows {
            result.push(row.get(column).and_then(|value| Some(value.as_str())))
        }
        result
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
