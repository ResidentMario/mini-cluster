use crate::file::{WorkerS3ClientAdapter, WorkerS3ClientTrait};
use crate::workload::Workload;
use crate::db::{Database, Table};
use crate::err::Result;
use crate::file::localize_files;

use sqlx::sqlite::SqliteRow;

pub struct Job {
    pub workload: Workload,
    pub database: Database
}

impl Job {
    pub async fn new(workload: Workload) -> Result<Job> {
        let database = Database::new().await?;
        Ok(Job { workload, database })
    }

    /// Performs the build portion of the job -- namely, downloading all of the files from S3 and
    /// loading them into the SQLite database.
    pub async fn build<T: WorkerS3ClientTrait>(
        &self, client: WorkerS3ClientAdapter<T>
    ) -> Result<()> {
        let (files, file_paths) = localize_files(&self.workload, &client).await?;

        // This syntactic sugar is sweet.
        for (&file, path) in files.iter().zip(file_paths) {
            let table = Table::new(
                &("dataset_".to_owned() + &file.id.to_string()),
                &path
            );
            table.drop().await?;
            table.dump().await?;
        }
        Ok(())
    }

    /// Performs the work portion of the job, e.g. the actual job execution.
    pub async fn run(&self) -> Result<Vec<SqliteRow>> {
        let mut conn = Database::connect().await?;
        let ops = self.workload.get_ops();
        let mut result: Vec<SqliteRow> = vec![];
        for i in 0..ops.len() {
            let sql = ops[i].get_statement();

            // Only the last op in the sequence should return a result. All other ops are
            // preparatory: e.g. merging data, building new tables, and the like.
            if i != (ops.len() - 1) {
               sqlx::query(sql).execute(&mut conn).await?;
            } else {
                result = sqlx::query(sql).fetch_all(&mut conn).await?;
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use crate::fixtures::*;
    use super::*;

    #[test]
    fn test_create_new_job() {
        let workload = craft_workload_message(None);
        let job = block_on(Job::new(workload));
        assert!(job.is_ok());
    }

    // I can't easily unit test build or run execution because the `_get_object` logic associated
    // with the S3 downloader mock returns `vec![1,2,3]`. This is not valid CSV because it fails
    // the CSV parsing rules: it doesn't have a header.
    //
    // In order to unit test these functions we would have to convert the following CSV:
    //
    // ```csv
    // a_int,b_int,c_int
    // 1,2,3
    // ```
    //
    // Into UTF-8 bytes, and then reencode those bytes into decimal uint8 values. Doing this by
    // hand is extremely fraught (how the hell are you going to tell why it's not working?), and
    // I'm not about to also write my own encoder/decoder, so I'm just going to keep to
    // integration tests (`test_build_job` and `test_run_job`) instead.
    //
    // Note that encoding to bytes isn't too bad, use https://codebeautify.org/utf8-converter,
    // it's the double encoding that sucks.
}