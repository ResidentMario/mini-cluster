use std::{collections::{HashMap, HashSet}};
use std::fs;

use async_trait::async_trait;

use rusoto_s3::{GetObjectRequest, S3, S3Client};
use rusoto_core::region::Region;
use tokio::io::AsyncReadExt;

use crate::workload::{Workload,File};
use crate::Result;
use crate::{WorkerError,ErrKind};


/// Returns every file in a workload. Multiple Ops can refer to the same (normalized) file, so
/// this function deduplicates the options.
pub fn get_workload_files(workload: &Workload) -> Vec<&File> {
    let mut files: Vec<&File> = vec![];
    let mut file_ids: HashSet<i32> = HashSet::new();
    for op in workload.get_ops() {
        for file in op.get_targets() {
            if !file_ids.contains(&file.get_id()) {
                files.push(file);
                file_ids.insert(file.get_id());
            }
        }
    }
    return files
}

pub fn create_new_s3_client() -> WorkerS3ClientAdapter<S3Client> {
    let region = Region::UsEast1;
    let client = S3Client::new(region);
    WorkerS3ClientAdapter { client }
}

pub fn parse_file_path(path: &str) -> Result<HashMap<&str, String>> {
    if !path.starts_with("s3://") {
        Err(WorkerError::new(ErrKind::AWSError,"Error: path is not an S3 path."))?
    } else {
        let path = &path[5..];
        let bucket_name_offset = path.find("/").ok_or(
            WorkerError::new(
                ErrKind::AWSError, 
                &format!("Illegal S3 target {}: path must point to a bucket object!", path)
            )
        )?;
        let bucket_name = &path[..bucket_name_offset];
        let object_name = &path[(bucket_name_offset + 1)..];

        let mut resp = HashMap::new();
        resp.insert("bucket", bucket_name.to_owned());
        resp.insert("object", object_name.to_owned());
        Ok(resp)
    }
}

/// Creates the cache directory for a given bucket, if one is needed. If the expected directory
/// structure already exists, this is a no-op.
pub fn create_cache_dir(bucket: &str) -> Result<String> {
    let worker_cache_base_fp = std::path::Path::new("/tmp/mini-cluster-worker/");
    let worker_cache_fp = std::path::Path::new("/tmp/mini-cluster-worker/cache/");
    let bucket_cache_fp_str = format!("/tmp/mini-cluster-worker/cache/{}", bucket);
    let bucket_cache_fp = std::path::Path::new(&bucket_cache_fp_str);
    if !worker_cache_base_fp.exists() {
        std::fs::create_dir(worker_cache_base_fp)?;
    }
    if !worker_cache_fp.exists() {
        std::fs::create_dir(worker_cache_fp)?;
    }
    if !bucket_cache_fp.exists() {
        std::fs::create_dir(bucket_cache_fp)?;
    }
    Ok(bucket_cache_fp_str)
}

/// Returns the cache directory path. For use by other functions in the library. Does not
/// guarantee that the cache directory actually exists yet! For that, call `create_cache_dir`
/// first.
pub fn get_cache_dir() -> String {
    return "/tmp/mini-cluster-worker/cache/".to_owned()
}

// TODO: implement this method.
// pub fn get_cache_file_path(f: &File) -> String {
//     let cache_home = get_cache_dir();
//     cache_home + f.path
// }

// Our next function, `localize_file`, is what we use to download data from S3. Because it
// performs network I/O, in order to unit test it we need to stub it.
//
// `rusoto` doesn't come with any special support for stubbing built in. The correct approach,
// attested to in e.g.
// https://www.reddit.com/r/rust/comments/d2puv7/how_to_mock_calls_to_databases/ezw5wl0/,
// is to use a pattern described as "Ports and Adapters" in the literature. Basically, define a
// thin wrapper around the client object and use that object instead of calling the client
// (`S3Client` in this case) directly. This allows us to swap out the inner object at test time
// with a mock of our own design.
pub struct WorkerS3ClientAdapter<T: WorkerS3ClientTrait> {
    pub client: T
}

// This implementation does not use generics, but instead uses associated types.
//
// Generics don't work here because the stub outputs a value with a concrete type, e.g. an &[u8],
// not a generic type. Apparently when you define a generic function, its output has to itself be
// generic (which AFAIK can only be constructed from an input generic); it *cannot* be a
// concrete type implementing the generic's trait bound. E.g. the following code is invalid:
// https://discord.com/channels/442252698964721669/448238009733742612/820812312960565309.
//
// Help chat explained that I want to use a different language feature instead: associated types.
// Associated types allow one to attach functions to traits which have concrete return types
// defined in their implementation instead of in the function signature.
//
// UPDATE: I was not able to use associated types after all. `rusoto` returns an object
// with the opaque type `impl Read + Send + Sync. Associated types apparently cannot work with
// such types; they need a concrete type. This is a limitation of the compiler. See further:
// https://github.com/rust-lang/rust/issues/63063.
//
// I ended up giving up on fighting the compiler and switched to using a Vec<u8> concrete return
// type. This has the important disadvantage that it means that the file I/O is no longer under
// unit tests but there's only so much I can do...
#[async_trait]
pub trait WorkerS3ClientTrait {
    async fn _get_object(&self, input: GetObjectRequest) -> Result<Vec<u8>>;
}
pub struct WorkerS3ClientMock {}

#[async_trait]
impl WorkerS3ClientTrait for WorkerS3ClientMock {
    async fn _get_object(&self, _: GetObjectRequest) -> Result<Vec<u8>> {
        Ok(vec![1, 2, 3])
    }
}

#[async_trait]
impl WorkerS3ClientTrait for S3Client {
    async fn _get_object(&self, input: GetObjectRequest) -> Result<Vec<u8>> {
        // `get_object` is the S3Client object download function.
        // let obj = self.get_object(input).await?;
        let future = self.get_object(input);
        let obj = future.await?;

        let mut obj_reader = obj.body
            .ok_or(
            WorkerError::new(
                    ErrKind::AWSError,
                    // Printing the object key is surprisingly annoying here. `GetObjectRequest`
                    // consumes the object reference, and is in turn consumed by `get_object`. To
                    // retain the name I guess you'd have to make a copy of the original value
                    // reference? This is a good bit of code to include in the critique.
                    &format!("Error: object has no bytes.")
                )
            )
            .map(|v| { v.into_async_read() })?;

        // Another limitation here: the file size has to be sufficiently small such that it can
        // fit into main memory, since we are not spilling to disk incrementally.
        //
        // TODO: switch to incremental read-write.
        let mut buf = vec![];
        obj_reader.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}

impl<T: WorkerS3ClientTrait> WorkerS3ClientAdapter<T> {
    pub async fn get_object(&self, req: GetObjectRequest) -> Result<Vec<u8>> {
        Ok(self.client._get_object(req).await?)
    }
}

/// Downloads the file to local disk cache. If the file already exists in the cache, this is a
/// no-op. If the file path is invalid, the file does not exist, or an error occurs while trying
/// to download the file, an error is bubbled up.
pub async fn localize_file<T: WorkerS3ClientTrait>(
    file: &File, client: &WorkerS3ClientAdapter<T>
) -> Result<String> {

    // let client = create_new_s3_client();
    let path = file.get_path();
    let bucket_map = parse_file_path(path)?;

    // `clone` is necessary here because the `bucket_map` struct owns its values. `get` returns
    // a pointer to those values. Since the `String` type is a move type, not a copy type, trying
    // to move ownership of it to the `bucket` and `object` variables doesn't work, because it
    // would invalidate the `bucket_map` value references.
    //
    // Why do we need a `String` instead of an `&str` pointer anyway? Well, because
    // `GetObjectRequest` requires `String` parameters, that's why.
    let bucket = bucket_map.get("bucket").unwrap().clone();
    let object = bucket_map.get("object").unwrap().clone();

    let bucket_cache_fp = create_cache_dir(bucket.as_str())?;
    let file_cache_fp = format!("{}/{}", bucket_cache_fp, object);

    // Why is this so verbose? I have no idea, the documentation doesn't seem to have any simpler
    // constructors...ew.
    let req = GetObjectRequest {
        bucket: bucket,
        key: object,
        expected_bucket_owner: None,
        if_match: None,
        if_modified_since: None,
        if_none_match: None,
        if_unmodified_since: None,
        part_number: None,
        range: None,
        request_payer: None,
        response_cache_control: None,
        response_content_disposition: None,
        response_content_encoding: None,
        response_content_language: None,
        response_content_type: None,
        response_expires: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        version_id: None,
    };
    let buf = client.get_object(req).await?;
    fs::write(&file_cache_fp, buf)?;
    Ok(file_cache_fp)
}

/// Downloads all of the files needed by the job to the disk cache. Calls `localize_file`
/// repeatedly to do so.
pub async fn localize_files<'a, T: WorkerS3ClientTrait>(
    workload: &'a Workload, client: &WorkerS3ClientAdapter<T>
) -> Result<(Vec<&'a File>, Vec<String>)> {
    // Interesting quirk here. According to the Rust VSCode extension this vector has the
    // following contained type:
    //
    // impl Future<Output=Result(), Box<dyn Error>>>>
    //
    // This type annotation is inferred by the extension, and presumably by the compiler, from the
    // use of this vector as the target container in the `get_workload_files` iterator below.
    // However, I can't _manually_ specify this type annotation myself. Try it yourself:
    // uncomment the `futures2` variable in the following LOC and see what happens. The error
    // given is that `impl` is "not allowed outside of function and inherent method return type".
    //
    // It's probably relatively straightforward to work around this, if we need to, by using a
    // Box<T> wrapper. But it's all kind of crazy to me.
    let mut futures = vec![];
    // let mut futures2: Vec<impl Future<Output = Result<(), Box<dyn Error>>>> = vec![];

    let workload_files = get_workload_files(workload);

    // Yep, you read that right: `file` here has the type `&&File`. A double indirect reference!
    //
    // Rust has two iterator forms: `iter()`, which is a borrow, and `into_iter()`, which is a
    // move (or a copy, if the target is `Copy` type). `into_iter()` would take ownership of the
    // contents of `workload_files` and prevent us from returning it at the end of the call.
    // `iter` doesn't take ownership, but does result in this icky double pointer instead.
    //
    // However, apparently double pointers are NBD, the compiler will optimize them away.
    //
    // Also you can turn the `&&File` reference into a `&File` reference by doing `for &file in`
    // instead of `for file in`, as here. Crazy.
    //
    // Cf. https://discord.com/channels/442252698964721669/448238009733742612/822609411528720425
    for &file in workload_files.iter() {
        let future = localize_file(file, client);
        futures.push(future);
    }

    let mut file_paths: Vec<String> = vec![];
    for future in futures {
        let file_path = future.await?;
        file_paths.push(file_path);
    }
    Ok((workload_files, file_paths))
}

#[cfg(test)]
mod tests {
    use crate::fixtures::*;
    use super::*;
    use protobuf::RepeatedField;
    use futures::executor::block_on;
    use crate::workload::Op;

    #[test]
    /// Tests getting the file list from the complete buffer.
    fn test_get_workload_files() {
        // One Op, one File.
        let workload = craft_workload_message(None);

        let files = get_workload_files(&workload);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].get_path(), "s3://foo/bar");

        // Two Ops referencing different Files.
        let file1 = craft_file_message(Some(1), Some(String::from("s3://foo")));
        let file2 = craft_file_message(Some(2), Some(String::from("s3://bar")));
        let op1 = craft_op_message(
            Some(RepeatedField::from_vec(vec![file1])), None,Some(1)
        );
        let op2 = craft_op_message(
            Some(RepeatedField::from_vec(vec![file2])), None, Some(2)
        );
        let workload = craft_workload_message(
            Some(RepeatedField::from_vec(vec![op1, op2]))
        );

        let files = get_workload_files(&workload);

        let mut expected: HashSet<&str> = HashSet::new();
        expected.insert("s3://foo");
        expected.insert("s3://bar");
        assert_eq!(files.len(), 2);
        assert_eq!(
            files.into_iter().map(|file| { file.get_path() }).collect::<HashSet<&str>>(),
            expected
        );

        // Two Ops referencing the same File.
        let op1 = craft_op_message(
            None,
            None,
            Some(1),
        );
        let op2 = craft_op_message(
            None,
            None,
            Some(2),
        );
        let workload = craft_workload_message(
            Some(RepeatedField::from_vec(vec![op1, op2]))
        );

        let files = get_workload_files(&workload);

        assert_eq!(files.len(), 1);
    }

    #[test]
    /// Test the S3 file path parsing behavior.
    fn test_parse_file_path() {
        // Valid S3 path.
        let result = parse_file_path("s3://foo/bar/baz.csv");
        assert!(result.is_ok());
        let result = result.unwrap();

        let bucket = result.get("bucket");
        assert!(bucket.is_some());
        let bucket = bucket.unwrap();

        let object = result.get("object");
        assert!(object.is_some());
        let object = object.unwrap();

        assert_eq!(bucket, "foo");
        assert_eq!(object, "bar/baz.csv");

        // Invalid non-S3 path.
        let result = parse_file_path("not_s3!");
        assert!(result.is_err());

        // Invalid S3 path with no object component (e.g. a bucket).
        let result = parse_file_path("s3://foo");
        assert!(result.is_err());
    }

    #[test]
    /// Test the S3 file getter. Note that this unit test doesn't test the actual network
    /// transaction, only writing to the output file. For network tests refer to the integration
    /// tests.
    fn test_localize_file() {
        let file = craft_file_message(None, None);
        let client_mock = WorkerS3ClientMock {};
        let client_adapter = WorkerS3ClientAdapter { client: client_mock };
        let result = block_on(localize_file(&file, &client_adapter));

        assert!(result.is_ok());
    }

    #[test]
    /// Test the S3 file getter wrapper.
    fn test_localize_files() {
        let file1 = craft_file_message(Some(1), Some("s3://foo/bar".to_owned()));
        let file2 = craft_file_message(Some(2), Some("s3://foo/baz".to_owned()));
        let op1 = craft_op_message(
            Some(RepeatedField::<File>::from_vec(vec![file1])), None, None
        );
        let op2 = craft_op_message(
            Some(RepeatedField::<File>::from_vec(vec![file2])), None, None
        );
        let ops = RepeatedField::<Op>::from_vec(vec![op1, op2]);
        let workload = craft_workload_message(Some(ops));

        let client_mock = WorkerS3ClientMock {};
        let client_adapter = WorkerS3ClientAdapter { client: client_mock };

        let result = block_on(localize_files(&workload, &client_adapter));

        assert!(result.is_ok());
    }
}