use crate::*;

/// Create a table with a given name and columns.
/// Parameters:
///     table_name          The name of the table you want to create.
///     table_columns       A vector of (column_name, column_type) tuples.
///     conn                A sqlite::Connection to work with.
pub fn create_table(conn: &Connection, table_name: &str, table_columns: Vec<(&str, &str)>) -> Result<()> {
    let columns = table_columns.iter()
        .map(|(column_name, column_type)| format!(r#""{}" {}"#, column_name, column_type))
        .collect::<Vec<String>>()
        .join(", ");
    let query = format!(r#"
    CREATE TABLE IF NOT EXISTS "{}" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, {});
    "#, table_name, columns);

    let mut stmt = conn.prepare_cached(query.as_ref())?;
    stmt.execute([])?;

    Ok(())
}

/// Get the next ID to use.
pub fn get_last_rowid(conn: &Connection) -> usize {
    let id: Result<i32> = 
    conn.prepare_cached("SELECT last_insert_rowid();").unwrap()
        .query_row(params![], |r| r.get(0));
    let id = match id {
        Ok(id) if id >= 0 => id + 1,
        // Ignore errors and just start with id = 0.
        Ok(_id) => 0,
        Err(_) => 0, 
        };
    id.max(0) as usize
}

/// Add a row to a table.
pub fn add_row(conn: &Connection, table_name: &str, columns: &[&str], values: &[&str], where_clause: Option<&str>) -> Result<(), rusqlite::Error> {
    // We need to keep track of how many columns/values we need to 
    let longest = 0
        .max(columns.len())
        .max(values.len());

    let values: Vec<String> = pad_row(&values, "", longest);
    let columns: Vec<String> = pad_row(&columns, "", longest);

    let placeholder = build_placeholder(longest);
    let column_names = columns.iter().map(|c| format!(r#""{}""#, c)).collect::<Vec<String>>().join(", ");
    let query = format!(r#"INSERT INTO "{}" ({}) VALUES ({}) {};"#, table_name, &column_names, placeholder, where_clause.unwrap_or(""));
    let mut stmt = conn.prepare(&query)?;

    // Bind the parameters.
    for (jj, val) in values.iter().enumerate() {
        stmt.raw_bind_parameter(jj + 1, val)?;
    }

    match stmt.raw_execute() {
        Ok(1) => Ok(()),
        Ok(n) => { 
            warn!("unexpected number of rows affected: {}", n); 
            Ok(())
        },
        Err(er) => {
            error!("error adding a row! {}", er);
            Err(er)
        }
    }
}

fn build_placeholder(len: usize) -> String {
    let question_marks = (0..len).map(|_| "?").collect::<Vec<&str>>();
    question_marks.join(", ")
}

fn pad_row(values: &[&str], pad: &str, pad_to: usize) -> Vec<String> {
    let mut result: Vec<String> = values.iter().map(|x| x.to_string()).collect();
    for _ii in values.len()..pad_to {
        result.push(pad.to_string());
    }

    result
}