use sqlx::{Connection, Sqlite, SqliteConnection, migrate::MigrateDatabase};
use sqlx::sqlite::SqliteRow;

use crate::Result;
use crate::err::{WorkerError, ErrKind};
use crate::file::get_cache_dir;

// Best practice when working with SQLite is to only ever have a single connection open at a time
// per program instance, and to close those connections often. When interacting with SQLite, it is
// apparently cheaper in the long run to e.g. create, query, and immediately close a connection
// then it is to just keep connections open indefinitely. This is because the time cost of opening
// a new connection every time a query needs to made is smaller than the resource exhaustion risk
// of having too many connections open at once (e.g. you might run out of file descriptors at the
// OS level). See e.g. https://stackoverflow.com/a/19187244/1993206.
//
// As such, we do not store a single connection at the struct level, preferring instead to create
// then whenever needed, on a function-by-function basis.
pub struct Database {}

impl Database {
    /// Returns the database path (e.g. the filesystem path).
    pub fn get_db_path() -> String { get_cache_dir() + "db.sqlite" }

    /// Returns the database URL (e.g. the URI that can be passed to SQLx).
    pub fn get_db_url() -> String { "file://".to_owned() + Database::get_db_path().as_str() }

    /// Connects to the database, returning an open connection usable for querying.
    pub async fn connect() -> Result<SqliteConnection> {
        let db_path = Database::get_db_path();
        let db_url = Database::get_db_url();

        if !std::path::Path::new(&db_path).exists() {
            Sqlite::create_database(&db_path).await?;
        }

        let conn: SqliteConnection = SqliteConnection::connect(&db_url).await?;
        Ok(conn)
    }

    pub async fn new() -> Result<Database> {
        // Check that the connection can successfully be made first.
        let conn = Database::connect().await?;
        conn.close();
        Ok(Database {})
    }

    /// Drops the database (e.g. clears out the database cache) if it exists.
    pub async fn drop() -> Result<()> {
        let db_url = Database::get_db_url();
        let exists: bool = Sqlite::database_exists(&db_url).await?;
        if exists {
            Sqlite::drop_database(&db_url).await?;
        }
        Ok(())
    }
}

pub struct Table {
    name: String,
    source: String
}

impl Table {
    pub fn new(name: &str, source: &str) -> Table {
        Table { name: name.to_owned(), source: source.to_owned() }
    }

    /// Dumps the contents of the file at `source` into the database instance.
    ///
    /// The header in the chosen CSV must follow the schema `name_type`, where `name` is the
    /// column name and `type` is a SQL type that SQLite understands.
    ///
    /// If the table already exists, it is assumed that the information is already cached, so this
    /// method is a no-op.
    pub async fn dump(&self) -> Result<()> {
        let mut reader = csv::Reader::from_path(&self.source)?;
        let mut conn = Database::connect().await?;

        let table_exists = sqlx::query(
            &format!(
                "SELECT name FROM sqlite_master WHERE type='table' AND name={}", self.name
            )
        ).fetch_all(&mut conn).await;
        let table_exists = table_exists.is_ok();

        if !table_exists {
            let headers = reader.headers()?;
            // Although it's headers plural, there's only one real header, which is always the
            // first column. If there is no first column (e.g. the CSV is empty) headers returns
            // an empty record.
            if headers.len() == 0 {
                Err(WorkerError::new(ErrKind::DatabaseError,"Error: CSV is empty."))?
            }

            let mut columns = vec![];
            let mut column_types = vec![];
            for col in headers {
                // Skip empty columns, in case there is one.
                if col == "" { continue }

                let splitter_idx = match col.find("_") {
                    Some(v) => v,
                    None => return Err(
                        WorkerError::new(ErrKind::DatabaseError,"Error: CSV field has no type.")
                    )?
                };

                let col_name = &col[..splitter_idx];
                columns.push(col_name);
                let col_type = &col[(splitter_idx + 1)..];
                column_types.push(col_type);
            }

            // Build the query.
            let mut create_query = format!("CREATE TABLE {} (\n", self.name).to_owned();
            for (col_name, col_type) in columns.into_iter().zip(column_types) {
                create_query += &format!("{} {},\n", col_name, col_type);
            }
            // Remove the last `,\n` to get rid of the trailing comma, which is invalid in SQL.
            create_query = create_query[..(create_query.len() - 2)].to_owned();
            create_query += "\n);";

            sqlx::query(&create_query).execute(&mut conn).await?;

            for record in reader.records() {
                let record = record?;
                let record = record.into_iter().collect::<Vec<_>>();
                let mut insert_query = format!("INSERT INTO {} VALUES (", self.name);
    
                for col in record {
                    insert_query += col;
                    insert_query += ", "
                }
                insert_query = insert_query[..(insert_query.len() - 2)].to_owned();
                insert_query += ");";
                sqlx::query(&insert_query).execute(&mut conn).await?;
            }    
        }

        conn.close();

        Ok(())
    }

    /// Drops this table from the database, if it exists.
    pub async fn drop(&self) -> Result<()> {
        let mut conn = Database::connect().await?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {}", self.name)).execute(&mut conn).await?;
        conn.close();
        Ok(())
    }

    /// Loads the contents of this table into memory.
    pub async fn load(&self) -> Result<Vec<SqliteRow>> {
        let mut conn = Database::connect().await?;
        // Here's a cool little interaction. `close` takes ownership of its caller, preventing
        // any further usage of this connection instance. Compare that with e.g. Python, where
        // I have raised from trying to call on a prematurely closed connection more than a few
        // times.
        //
        // In other words, ownership turns a runtime error into a compiler error. Pretty cool.
        // conn.close();

        let records = sqlx::query(
            &format!("SELECT * FROM {}", self.name)
        ).fetch_all(&mut conn).await?;
        conn.close();

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use serial_test::serial;
    
    // use crate::fixtures::*;
    use super::*;

    #[test]
    fn test_create_database() {
        let db = block_on(Database::new());
        assert!(db.is_ok());
        assert!(std::path::Path::new(&Database::get_db_path()).exists())
    }

    #[test]
    fn test_drop_database() {
        let db = block_on(Database::new());
        assert!(db.is_ok());

        let db_dropped = block_on(Database::drop());
        assert!(db_dropped.is_ok());
    }

    /// `test_dump_table` and `test_load_table` are `#[serial]` because Rust runs its tests in
    /// parallel. Since both of these functions manipulate the database state this creates race
    /// conditions. Took me a while to realize what was going on here...
    #[test]
    #[serial]
    fn test_dump_table() {
        let t = Table::new(
            "foo",
            "/Users/alekseybilogur/Desktop/mini-cluster/\
            rust/mini-cluster-worker/tests/artifacts/simple-csv.csv"
        );

        let drop = block_on(t.drop());
        assert!(drop.is_ok());

        let dump = block_on(t.dump());
        assert!(dump.is_ok());

        let conn = block_on(Database::connect());
        assert!(conn.is_ok());
        let mut conn = conn.unwrap();
        let result = block_on(
            sqlx::query("SELECT * FROM foo").fetch_all(&mut conn)
        );
        conn.close();

        assert!(result.is_ok());
        let result = result.unwrap();

        assert!(result.len() > 0);

        let drop = block_on(t.drop());
        assert!(drop.is_ok());
    }

    #[test]
    #[serial]
    fn test_load_table() {
        let t = Table::new(
            "foo",
            "/Users/alekseybilogur/Desktop/mini-cluster/\
            rust/mini-cluster-worker/tests/artifacts/simple-csv.csv"
        );

        let drop = block_on(t.drop());
        assert!(drop.is_ok());

        let dump = block_on(t.dump());
        assert!(dump.is_ok());

        let load = block_on(t.load());
        assert!(load.is_ok());
        let load = load.unwrap();        
        assert!(load.len() > 0);

        let drop = block_on(t.drop());
        assert!(drop.is_ok());
    }
}