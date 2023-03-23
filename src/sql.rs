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

/// Populate the table with records from an iterator.
/// columns should be the columns of the table, and records should contain the values to populate columns with.
pub fn populate_table(conn: Connection, table_name: &str, records: &[StringRecord], columns: Vec<&str>) -> Result<usize> {
    let mut records_written: usize = 0;
    for row in records.iter() {
        debug!("reading row {:?}", row);

        // We need to know how many columns are in this row.
        let len = row.len();
        if len == 0 
        || len != columns.len() {
            continue;
        }

        // The id of the last row added.
        let id: Result<i32> = 
        conn.prepare_cached("SELECT last_insert_rowid();")?
            .query_row(params![], |r| r.get(0));
        let id = match id {
            Ok(id) => id + 1,
            // Ignore errors and just start with id = 0.
            Err(_) => 0, 
        };
        
        // Create the row.
        conn.prepare_cached(format!(r#"INSERT INTO "{}" ("id") VALUES ({});"#, table_name, id).as_ref())?
            .execute([])?;

        // Begin a transaction.
        conn.prepare_cached("BEGIN TRANSACTION;")?
            .execute([])?;

        let mut success = true;
        for (column, value) in columns.iter().zip(row.iter()) {
            // Prepare the query.
            let query = 
            format!(r#"UPDATE "{}" SET "{}" = ? WHERE "id" = ?;"#,
                table_name,
                column,
            );

            success = match conn.execute(&query, params![value, id]){
                Ok(1) => {
                    true
                },
                Ok(x) => {
                    warn!("Unexpected number of rows altered: {}.", id);
                    warn!("Query was {}.", query);
                    true
                },
                Err(e) => {
                    error!("Error: {}", e);
                    false
                }
            };
            if !success {
                break;
            }
        }
        if success {
            records_written += 1;
        }

        // Finalise the transaction.
        conn.prepare_cached("END TRANSACTION;")?
            .execute([])?;
   }
    Ok(records_written)
}