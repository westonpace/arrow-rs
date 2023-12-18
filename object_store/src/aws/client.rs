// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::aws::checksum::Checksum;
use crate::aws::credential::{AwsCredential, CredentialExt};
use crate::aws::{
    AwsCredentialProvider, S3ConditionalPut, S3CopyIfNotExists, STORE, STRICT_PATH_ENCODE_SET,
};
use crate::client::get::GetClient;
use crate::client::header::HeaderConfig;
use crate::client::header::{get_put_result, get_version};
use crate::client::list::ListClient;
use crate::client::retry::RetryExt;
use crate::client::s3::{
    CompleteMultipartUpload, CompleteMultipartUploadResult, InitiateMultipartUploadResult,
    ListResponse,
};
use crate::client::GetOptionsExt;
use crate::multipart::PartId;
use crate::path::DELIMITER;
use crate::{
    ClientOptions, GetOptions, ListResult, MultipartId, Path, PutResult, Result, RetryConfig,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use hyper::http::HeaderName;
use itertools::Itertools;
use percent_encoding::{utf8_percent_encode, PercentEncode};
use quick_xml::events::{self as xml_events};
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Client as ReqwestClient, Method, RequestBuilder, Response,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

const VERSION_HEADER: &str = "x-amz-version-id";

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error fetching get response body {}: {}", path, source))]
    GetResponseBody {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing put request {}: {}", path, source))]
    PutRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing DeleteObjects request: {}", source))]
    DeleteObjectsRequest { source: crate::client::retry::Error },

    #[snafu(display(
        "DeleteObjects request failed for key {}: {} (code: {})",
        path,
        message,
        code
    ))]
    DeleteFailed {
        path: String,
        code: String,
        message: String,
    },

    #[snafu(display("Error getting DeleteObjects response body: {}", source))]
    DeleteObjectsResponse { source: reqwest::Error },

    #[snafu(display("Got invalid DeleteObjects response: {}", source))]
    InvalidDeleteObjectsResponse {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error performing copy request {}: {}", path, source))]
    CopyRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing create multipart request: {}", source))]
    CreateMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting create multipart response body: {}", source))]
    CreateMultipartResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing complete multipart request: {}", source))]
    CompleteMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting complete multipart response body: {}", source))]
    CompleteMultipartResponseBody { source: reqwest::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Got invalid multipart response: {}", source))]
    InvalidMultipartResponse { source: quick_xml::de::DeError },

    #[snafu(display("Unable to extract metadata from headers: {}", source))]
    Metadata {
        source: crate::client::header::Error,
    },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::CopyRequest { source, path }
            | Error::PutRequest { source, path } => source.error(STORE, path),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "DeleteResult")]
struct BatchDeleteResponse {
    #[serde(rename = "$value")]
    content: Vec<DeleteObjectResult>,
}

#[derive(Deserialize)]
enum DeleteObjectResult {
    Deleted(DeletedObject),
    Error(DeleteError),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Deleted")]
struct DeletedObject {
    #[allow(dead_code)]
    key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Error")]
struct DeleteError {
    key: String,
    code: String,
    message: String,
}

impl From<DeleteError> for Error {
    fn from(err: DeleteError) -> Self {
        Self::DeleteFailed {
            path: err.key,
            code: err.code,
            message: err.message,
        }
    }
}

#[derive(Debug)]
pub struct S3Config {
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub bucket_endpoint: String,
    pub credentials: AwsCredentialProvider,
    pub retry_config: RetryConfig,
    pub client_options: ClientOptions,
    pub sign_payload: bool,
    pub skip_signature: bool,
    pub disable_tagging: bool,
    pub checksum: Option<Checksum>,
    pub copy_if_not_exists: Option<S3CopyIfNotExists>,
    pub conditional_put: Option<S3ConditionalPut>,
}

impl S3Config {
    pub(crate) fn path_url(&self, path: &Path) -> String {
        format!("{}/{}", self.bucket_endpoint, encode_path(path))
    }

    async fn get_credential(&self) -> Result<Option<Arc<AwsCredential>>> {
        Ok(match self.skip_signature {
            false => Some(self.credentials.get_credential().await?),
            true => None,
        })
    }
}

/// A builder for a put request allowing customisation of the headers and query string
pub(crate) struct PutRequest<'a> {
    path: &'a Path,
    config: &'a S3Config,
    builder: RequestBuilder,
    payload_sha256: Option<Vec<u8>>,
}

impl<'a> PutRequest<'a> {
    pub fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    pub fn header(self, k: &HeaderName, v: &str) -> Self {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    pub async fn send(self) -> Result<PutResult> {
        let credential = self.config.get_credential().await?;

        let response = self
            .builder
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                self.payload_sha256.as_deref(),
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: self.path.as_ref(),
            })?;

        Ok(get_put_result(response.headers(), VERSION_HEADER).context(MetadataSnafu)?)
    }
}

#[derive(Debug)]
pub(crate) struct S3Client {
    config: S3Config,
    client: ReqwestClient,
}

impl S3Client {
    pub fn new(config: S3Config) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    /// Returns the config
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// Make an S3 PUT request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
    ///
    /// Returns the ETag
    pub fn put_request<'a>(&'a self, path: &'a Path, bytes: Bytes) -> PutRequest<'a> {
        let url = self.config.path_url(path);
        let mut builder = self.client.request(Method::PUT, url);
        let mut payload_sha256 = None;

        if let Some(checksum) = self.config().checksum {
            let digest = checksum.digest(&bytes);
            builder = builder.header(checksum.header_name(), BASE64_STANDARD.encode(&digest));
            if checksum == Checksum::SHA256 {
                payload_sha256 = Some(digest);
            }
        }

        builder = match bytes.is_empty() {
            true => builder.header(CONTENT_LENGTH, 0), // Handle empty uploads (#4514)
            false => builder.body(bytes),
        };

        if let Some(value) = self.config().client_options.get_content_type(path) {
            builder = builder.header(CONTENT_TYPE, value);
        }

        PutRequest {
            path,
            builder,
            payload_sha256,
            config: &self.config,
        }
    }

    /// Make an S3 Delete request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
    pub async fn delete_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        query: &T,
    ) -> Result<()> {
        let credential = self.config.get_credential().await?;
        let url = self.config.path_url(path);

        self.client
            .request(Method::DELETE, url)
            .query(query)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Make an S3 Delete Objects request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ///
    /// Produces a vector of results, one for each path in the input vector. If
    /// the delete was successful, the path is returned in the `Ok` variant. If
    /// there was an error for a certain path, the error will be returned in the
    /// vector. If there was an issue with making the overall request, an error
    /// will be returned at the top level.
    pub async fn bulk_delete_request(&self, paths: Vec<Path>) -> Result<Vec<Result<Path>>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let credential = self.config.get_credential().await?;
        let url = format!("{}?delete", self.config.bucket_endpoint);

        let mut buffer = Vec::new();
        let mut writer = quick_xml::Writer::new(&mut buffer);
        writer
            .write_event(xml_events::Event::Start(
                xml_events::BytesStart::new("Delete")
                    .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]),
            ))
            .unwrap();
        for path in &paths {
            // <Object><Key>{path}</Key></Object>
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new(
                    "Object",
                )))
                .unwrap();
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::Text(xml_events::BytesText::new(
                    path.as_ref(),
                )))
                .map_err(|err| crate::Error::Generic {
                    store: STORE,
                    source: Box::new(err),
                })?;
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Object")))
                .unwrap();
        }
        writer
            .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Delete")))
            .unwrap();

        let body = Bytes::from(buffer);

        let mut builder = self.client.request(Method::POST, url);

        // Compute checksum - S3 *requires* this for DeleteObjects requests, so we default to
        // their algorithm if the user hasn't specified one.
        let checksum = self.config().checksum.unwrap_or(Checksum::SHA256);
        let digest = checksum.digest(&body);
        builder = builder.header(checksum.header_name(), BASE64_STANDARD.encode(&digest));
        let payload_sha256 = if checksum == Checksum::SHA256 {
            Some(digest)
        } else {
            None
        };

        let response = builder
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                payload_sha256.as_deref(),
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteObjectsRequestSnafu {})?
            .bytes()
            .await
            .context(DeleteObjectsResponseSnafu {})?;

        let response: BatchDeleteResponse =
            quick_xml::de::from_reader(response.reader()).map_err(|err| {
                Error::InvalidDeleteObjectsResponse {
                    source: Box::new(err),
                }
            })?;

        // Assume all were ok, then fill in errors. This guarantees output order
        // matches input order.
        let mut results: Vec<Result<Path>> = paths.iter().cloned().map(Ok).collect();
        for content in response.content.into_iter() {
            if let DeleteObjectResult::Error(error) = content {
                let path =
                    Path::parse(&error.key).map_err(|err| Error::InvalidDeleteObjectsResponse {
                        source: Box::new(err),
                    })?;
                let i = paths.iter().find_position(|&p| p == &path).unwrap().0;
                results[i] = Err(Error::from(error).into());
            }
        }

        Ok(results)
    }

    /// Make an S3 Copy request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
    pub async fn copy_request(&self, from: &Path, to: &Path, overwrite: bool) -> Result<()> {
        let credential = self.config.get_credential().await?;
        let url = self.config.path_url(to);
        let source = format!("{}/{}", self.config.bucket, encode_path(from));

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header("x-amz-copy-source", source);

        if !overwrite {
            match &self.config.copy_if_not_exists {
                Some(S3CopyIfNotExists::Header(k, v)) => {
                    builder = builder.header(k, v);
                }
                Some(S3CopyIfNotExists::HeaderWithStatus(k, v, _)) => {
                    builder = builder.header(k, v);
                }
                None => {
                    return Err(crate::Error::NotSupported {
                        source: "S3 does not support copy-if-not-exists".to_string().into(),
                    })
                }
            }
        }

        let precondition_failure = match &self.config.copy_if_not_exists {
            Some(S3CopyIfNotExists::HeaderWithStatus(_, _, code)) => *code,
            _ => reqwest::StatusCode::PRECONDITION_FAILED,
        };

        builder
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|source| match source.status() {
                Some(error) if error == precondition_failure => crate::Error::AlreadyExists {
                    source: Box::new(source),
                    path: to.to_string(),
                },
                _ => Error::CopyRequest {
                    source,
                    path: from.to_string(),
                }
                .into(),
            })?;

        Ok(())
    }

    pub async fn create_multipart(&self, location: &Path) -> Result<MultipartId> {
        let credential = self.config.get_credential().await?;
        let url = format!("{}?uploads=", self.config.path_url(location),);

        let response = self
            .client
            .request(Method::POST, url)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(CreateMultipartRequestSnafu)?
            .bytes()
            .await
            .context(CreateMultipartResponseBodySnafu)?;

        let response: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(response.reader()).context(InvalidMultipartResponseSnafu)?;

        Ok(response.upload_id)
    }

    pub async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_idx: usize,
        data: Bytes,
    ) -> Result<PartId> {
        let part = (part_idx + 1).to_string();

        let result = self
            .put_request(path, data)
            .query(&[("partNumber", &part), ("uploadId", upload_id)])
            .send()
            .await?;

        Ok(PartId {
            content_id: result.e_tag.unwrap(),
        })
    }

    pub async fn complete_multipart(
        &self,
        location: &Path,
        upload_id: &str,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let request = CompleteMultipartUpload::from(parts);
        let body = quick_xml::se::to_string(&request).unwrap();

        let credential = self.config.get_credential().await?;
        let url = self.config.path_url(location);

        let response = self
            .client
            .request(Method::POST, url)
            .query(&[("uploadId", upload_id)])
            .body(body)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(CompleteMultipartRequestSnafu)?;

        let version = get_version(response.headers(), VERSION_HEADER).context(MetadataSnafu)?;

        let data = response
            .bytes()
            .await
            .context(CompleteMultipartResponseBodySnafu)?;

        let response: CompleteMultipartUploadResult =
            quick_xml::de::from_reader(data.reader()).context(InvalidMultipartResponseSnafu)?;

        Ok(PutResult {
            e_tag: Some(response.e_tag),
            version,
        })
    }

    #[cfg(test)]
    pub async fn get_object_tagging(&self, path: &Path) -> Result<Response> {
        let credential = self.config.get_credential().await?;
        let url = format!("{}?tagging", self.config.path_url(path));
        let response = self
            .client
            .request(Method::GET, url)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;
        Ok(response)
    }
}

#[async_trait]
impl GetClient for S3Client {
    const STORE: &'static str = STORE;

    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: Some(VERSION_HEADER),
    };

    /// Make an S3 GET request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
    async fn get_request(&self, path: &Path, options: GetOptions) -> Result<Response> {
        let credential = self.config.get_credential().await?;
        let url = self.config.path_url(path);
        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut builder = self.client.request(method, url);

        if let Some(v) = &options.version {
            builder = builder.query(&[("versionId", v)])
        }

        let response = builder
            .with_get_options(options)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for S3Client {
    /// Make an S3 List request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        let credential = self.config.get_credential().await?;
        let url = self.config.bucket_endpoint.clone();

        let mut query = Vec::with_capacity(4);

        if let Some(token) = token {
            query.push(("continuation-token", token))
        }

        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        query.push(("list-type", "2"));

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if let Some(offset) = offset {
            query.push(("start-after", offset))
        }

        let response = self
            .client
            .request(Method::GET, &url)
            .query(&query)
            .with_aws_sigv4(
                credential.as_deref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(ListRequestSnafu)?
            .bytes()
            .await
            .context(ListResponseBodySnafu)?;

        let mut response: ListResponse =
            quick_xml::de::from_reader(response.reader()).context(InvalidListResponseSnafu)?;
        let token = response.next_continuation_token.take();

        Ok((response.try_into()?, token))
    }
}

fn encode_path(path: &Path) -> PercentEncode<'_> {
    utf8_percent_encode(path.as_ref(), &STRICT_PATH_ENCODE_SET)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::AmazonS3Builder;
    use crate::aws::AwsCredential as ObjectStoreAwsCredential;
    use crate::path::Path;
    use crate::CredentialProvider;
    use crate::ObjectStore;
    use crate::Result as ObjectStoreResult;
    use aws_config::default_provider::credentials::DefaultCredentialsChain;
    use aws_config::meta::region::RegionProviderChain;
    use aws_credential_types::provider::error::CredentialsError;
    use aws_credential_types::provider::ProvideCredentials;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};
    use tokio::sync::RwLock;

    const AWS_CREDS_CACHE_KEY: &str = "aws_credentials";

    /// Adapt an AWS SDK cred into object_store credentials
    #[derive(Debug)]
    struct AwsCredentialAdapter {
        pub inner: Arc<dyn ProvideCredentials>,

        // RefCell can't be shared accross threads, so we use HashMap
        cache: Arc<RwLock<HashMap<String, Arc<aws_credential_types::Credentials>>>>,

        // The amount of time before expiry to refresh credentials
        credentials_refresh_offset: Duration,
    }

    impl AwsCredentialAdapter {
        fn new(
            provider: Arc<dyn ProvideCredentials>,
            credentials_refresh_offset: Duration,
        ) -> Self {
            Self {
                inner: provider,
                cache: Arc::new(RwLock::new(HashMap::new())),
                credentials_refresh_offset,
            }
        }
    }

    /// Adapt an object_store credentials into AWS SDK creds
    #[derive(Debug)]
    struct OSObjectStoreToAwsCredAdaptor(AwsCredentialProvider);

    impl ProvideCredentials for OSObjectStoreToAwsCredAdaptor {
        fn provide_credentials<'a>(
            &'a self,
        ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            aws_credential_types::provider::future::ProvideCredentials::new(async {
                let creds = self
                    .0
                    .get_credential()
                    .await
                    .map_err(|e| CredentialsError::provider_error(Box::new(e)))?;
                Ok(aws_credential_types::Credentials::new(
                    &creds.key_id,
                    &creds.secret_key,
                    creds.token.clone(),
                    Some(
                        SystemTime::now()
                            .checked_add(Duration::from_secs(
                                60 * 10, //  10 min
                            ))
                            .expect("overflow"),
                    ),
                    "",
                ))
            })
        }
    }

    #[async_trait]
    impl CredentialProvider for AwsCredentialAdapter {
        type Credential = ObjectStoreAwsCredential;

        async fn get_credential(&self) -> ObjectStoreResult<Arc<Self::Credential>> {
            let cached_creds = {
                let cache_value = self.cache.read().await.get(AWS_CREDS_CACHE_KEY).cloned();
                let expired = cache_value
                    .clone()
                    .map(|cred| {
                        cred.expiry()
                            .map(|exp| {
                                exp.checked_sub(self.credentials_refresh_offset)
                                    .expect("this time should always be valid")
                                    < SystemTime::now()
                            })
                            // no expiry is never expire
                            .unwrap_or(false)
                    })
                    .unwrap_or(true); // no cred is the same as expired;
                if expired {
                    None
                } else {
                    cache_value.clone()
                }
            };

            if let Some(creds) = cached_creds {
                Ok(Arc::new(Self::Credential {
                    key_id: creds.access_key_id().to_string(),
                    secret_key: creds.secret_access_key().to_string(),
                    token: creds.session_token().map(|s| s.to_string()),
                }))
            } else {
                let refreshed_creds = Arc::new(self.inner.provide_credentials().await.unwrap());

                self.cache
                    .write()
                    .await
                    .insert(AWS_CREDS_CACHE_KEY.to_string(), refreshed_creds.clone());

                Ok(Arc::new(Self::Credential {
                    key_id: refreshed_creds.access_key_id().to_string(),
                    secret_key: refreshed_creds.secret_access_key().to_string(),
                    token: refreshed_creds.session_token().map(|s| s.to_string()),
                }))
            }
        }
    }

    #[tokio::test]
    async fn weston_test() {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let credentials_provider = DefaultCredentialsChain::builder()
            .region(region_provider.region().await)
            .build()
            .await;

        let aws_creds = Arc::new(AwsCredentialAdapter::new(
            Arc::new(credentials_provider),
            Duration::from_secs(60),
        ));

        let s3 = AmazonS3Builder::new()
            .with_bucket_name("weston-s3-lance-test")
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_credentials(aws_creds)
            .build()
            .unwrap();
        let results = s3.list_with_delimiter(None).await.unwrap();
        dbg!(results);
    }
}
