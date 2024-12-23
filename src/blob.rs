use std::{borrow::Cow, io, num::NonZeroUsize, path::PathBuf, str::FromStr};

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::{operation::get_object::GetObjectError, primitives::ByteStream};
use lru::LruCache;
use once_cell::sync::OnceCell;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::debug;

#[async_trait]
pub trait Blobstore: Sync + Send + std::fmt::Debug {
    async fn get<'a>(&'a mut self, key: &str) -> anyhow::Result<Option<Cow<'a, [u8]>>>;
    async fn put(&mut self, key: &str, blob: &[u8]) -> anyhow::Result<()>;

    async fn must_get(&mut self, key: &str) -> anyhow::Result<Cow<[u8]>> {
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

    fn with_compression(self) -> Compressed<Self>
    where
        Self: Sized,
    {
        Compressed { underlying: self }
    }

    fn with_caching(self, capacity: usize) -> Caching<Self>
    where
        Self: Sized,
    {
        Caching {
            underlying: self,
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }
}

#[derive(Debug)]
pub struct LocalFilesystem {
    pub base: PathBuf,
}

#[async_trait]
impl Blobstore for LocalFilesystem {
    async fn get(&mut self, key: &str) -> anyhow::Result<Option<Cow<[u8]>>> {
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
        Ok(Some(Cow::Owned(blob)))
    }

    async fn put(&mut self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        let mut path = self.base.clone();
        path.push(PathBuf::from_str(key)?);
        let mut file = File::create(path).await?;
        file.write_all(blob).await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct S3Client {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
}

#[async_trait]
impl Blobstore for S3Client {
    async fn get(&mut self, key: &str) -> anyhow::Result<Option<Cow<[u8]>>> {
        debug!("fetching blob {}", key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| e.into_service_error());
        match resp {
            Ok(output) => Ok(Some(Cow::Owned(output.body.collect().await?.to_vec()))),
            Err(GetObjectError::NoSuchKey(_)) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }

    async fn put(&mut self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(blob.to_vec()))
            .send()
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Prefixed<B: Blobstore> {
    underlying: B,
    prefix: String,
}
#[async_trait]
impl<B: Blobstore> Blobstore for Prefixed<B> {
    async fn get(&mut self, key: &str) -> anyhow::Result<Option<Cow<[u8]>>> {
        self.underlying
            .get(&format!("{}/{}", self.prefix, key))
            .await
    }
    async fn put(&mut self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        self.underlying
            .put(&format!("{}/{}", self.prefix, key), blob)
            .await
    }
}

// This implementation does some annoying things with `once_cell` and `Cow` to avoid cloning
// the underlying blob every time it hands out the cached data. This can make a huge difference.
// When scanning ~250k items (stored across 16 blocks), these shared-reference shenanigans reduced
// runtime from
//    just scan  46.35s user 0.41s system 92% cpu 50.583 total
// to
//    just scan  0.60s user 0.29s system 11% cpu 7.749 total
#[derive(Debug)]
pub struct Caching<B: Blobstore> {
    underlying: B,
    cache: LruCache<String, OnceCell<Option<Vec<u8>>>>,
}

#[async_trait]
impl<B: Blobstore> Blobstore for Caching<B> {
    async fn get<'a>(&'a mut self, key: &str) -> anyhow::Result<Option<Cow<'a, [u8]>>> {
        let cell = self.cache.get_or_insert(key.to_owned(), OnceCell::new);
        if let Some(v) = cell.get() {
            let wrapped = v.as_ref().map(|inner| Cow::Borrowed(inner.as_slice()));
            return Ok(wrapped);
        }
        let v: Option<Vec<u8>> = self.underlying.get(key).await?.map(|v| match v {
            Cow::Borrowed(v) => v.to_vec(),
            Cow::Owned(v) => v,
        });
        Ok(cell
            .get_or_init(|| v)
            .as_ref()
            .map(|inner| Cow::Borrowed(inner.as_slice())))
    }
    async fn put(&mut self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        self.underlying.put(key, blob).await
    }
}

#[derive(Debug)]
pub struct Compressed<B: Blobstore> {
    underlying: B,
}

#[async_trait]
impl<B: Blobstore> Blobstore for Compressed<B> {
    async fn get(&mut self, key: &str) -> anyhow::Result<Option<Cow<[u8]>>> {
        let Some(blob) = self.underlying.get(key).await? else {
            return Ok(None);
        };
        debug!("decompressing blob {}", key);
        let decoded = zstd::decode_all(io::Cursor::new(blob))?;
        Ok(Some(Cow::Owned(decoded)))
    }
    async fn put(&mut self, key: &str, blob: &[u8]) -> anyhow::Result<()> {
        let encoded = zstd::encode_all(io::Cursor::new(blob), 0)?;
        self.underlying.put(key, &encoded).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use crate::blob::{Blobstore, LocalFilesystem};
    use async_trait::async_trait;
    use tempfile::tempdir;

    #[tokio::test]
    async fn get_not_found() -> anyhow::Result<()> {
        let base = tempdir()?.into_path();
        let mut fs = LocalFilesystem {
            base: base.as_path().to_path_buf(),
        };
        assert_eq!(fs.get("any-key").await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn garbage_path_errors() -> anyhow::Result<()> {
        let base = tempdir()?.into_path();
        let mut fs = LocalFilesystem {
            base: base.as_path().to_path_buf(),
        };
        assert!(fs.get("/////").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn round_trip_test() -> anyhow::Result<()> {
        let base = tempdir()?.into_path();
        let mut fs = LocalFilesystem {
            base: base.as_path().to_path_buf(),
        };
        let expected = "Hello, World!".as_bytes().to_vec();

        fs.put("my-file.txt", &expected).await?;
        let actual = fs.get("my-file.txt").await?;
        assert_eq!(actual, Some(Cow::Borrowed(expected.as_slice())));
        Ok(())
    }

    #[derive(Default, Clone, Debug)]
    struct Spystore {
        fetches: Vec<String>,
    }
    #[async_trait]
    impl Blobstore for Spystore {
        async fn get(&mut self, key: &str) -> anyhow::Result<Option<Cow<[u8]>>> {
            self.fetches.push(key.to_owned());
            Ok(None)
        }
        async fn put(&mut self, _: &str, _: &[u8]) -> anyhow::Result<()> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn caching_prevents_fetches() -> anyhow::Result<()> {
        let blob = Spystore::default();
        let mut cache = blob.with_caching(1);

        let _ = cache.get("foo").await;
        assert_eq!(cache.underlying.fetches, vec!["foo"]);

        let _ = cache.get("foo").await;
        assert_eq!(cache.underlying.fetches, vec!["foo"]);

        let _ = cache.get("bar").await;
        assert_eq!(cache.underlying.fetches, vec!["foo", "bar"]);
        Ok(())
    }

    #[tokio::test]
    async fn prefix_smoke_test() -> anyhow::Result<()> {
        let mut blob = Spystore::default().with_prefix("foo").with_prefix("bar");

        let _ = blob.get("baz").await;
        assert_eq!(blob.underlying.underlying.fetches, vec!["foo/bar/baz"]);
        Ok(())
    }
}
