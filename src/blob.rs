use std::{
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::{operation::get_object::GetObjectError, primitives::ByteStream};
use lru::LruCache;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

#[async_trait]
pub trait Blobstore: Sync + Send {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, blob: &[u8]) -> anyhow::Result<()>;

    async fn must_get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let blob = self.get(key).await?;
        Ok(blob.ok_or_else(|| anyhow!("no such blob: {}", key))?)
    }
    fn with_prefix(self, prefix: &str) -> Prefixed<Self>
    where
        Self: Sized,
    {
        Prefixed {
            underlying: self,
            prefix: prefix.to_owned(),
        }
    }
}

struct LocalFilesystem {
    pub base: PathBuf,
}

#[async_trait]
impl Blobstore for LocalFilesystem {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let mut path = self.base.clone();
        path.push(key);
        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(err) => {
                return if err.kind() == std::io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        };
        let mut blob = Vec::new();
        file.read_to_end(&mut blob).await?;
        Ok(Some(blob))
    }

    async fn put(&self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        let mut path = self.base.clone();
        path.push(PathBuf::from_str(key)?);
        let mut file = File::create(path).await?;
        file.write_all(blob).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::blob::{Blobstore, LocalFilesystem};

    #[tokio::test]
    async fn get_not_found() -> anyhow::Result<()> {
        let base = tempdir()?.into_path();
        let fs = LocalFilesystem {
            base: base.as_path().to_path_buf(),
        };
        assert_eq!(fs.get("any-key").await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn garbage_path_errors() -> anyhow::Result<()> {
        let base = tempdir()?.into_path();
        let fs = LocalFilesystem {
            base: base.as_path().to_path_buf(),
        };
        assert!(fs.get("/////").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn round_trip_test() -> anyhow::Result<()> {
        let base = tempdir()?.into_path();
        let fs = LocalFilesystem {
            base: base.as_path().to_path_buf(),
        };
        let expected = "Hello, World!".as_bytes().to_vec();

        fs.put("my-file.txt", &expected).await?;
        let actual = fs.get("my-file.txt").await?;
        assert_eq!(actual, Some(expected));
        Ok(())
    }
}

#[derive(Clone)]
pub struct S3Client {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    pub prefix: String,
}

#[async_trait]
impl Blobstore for S3Client {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(format!("{}/{}", self.prefix, key))
            .send()
            .await
            .map_err(|e| e.into_service_error());
        match resp {
            Ok(output) => Ok(Some(output.body.collect().await?.to_vec())),
            Err(GetObjectError::NoSuchKey(_)) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }

    async fn put(&self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!("{}/{}", self.prefix, key))
            .body(ByteStream::from(blob.to_vec()))
            .send()
            .await?;
        Ok(())
    }
}

pub struct Prefixed<B: Blobstore> {
    underlying: B,
    prefix: String,
}
#[async_trait]
impl<B: Blobstore> Blobstore for Prefixed<B> {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.underlying
            .get(&format!("{}/{}", self.prefix, key))
            .await
    }
    async fn put(&self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        self.underlying
            .put(&format!("{}/{}", self.prefix, key), blob)
            .await
    }
}

struct Caching {
    underlying: Box<dyn Blobstore>,
    cache: Arc<Mutex<LruCache<String, Option<Vec<u8>>>>>,
}

#[async_trait]
impl Blobstore for Caching {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        if let Some(v) = self.cache.lock().unwrap().get(key) {
            return Ok(v.clone());
        }
        let v = self.underlying.get(key).await?;
        Ok(self
            .cache
            .lock()
            .unwrap()
            .get_or_insert(key.to_owned(), || v)
            .clone())
    }
    async fn put(&self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        self.underlying.put(key, blob).await
    }
}
