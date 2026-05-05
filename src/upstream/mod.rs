use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::HOST;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use url::Url;

#[derive(Clone)]
pub struct UpstreamClient {
    client: Client<HttpConnector, Full<Bytes>>,
}

#[derive(Debug)]
pub enum UpstreamError {
    InvalidBackendUrl(String),
    InvalidUri(hyper::http::uri::InvalidUri),
    ReadRequestBody(String),
    Request(hyper_util::client::legacy::Error),
    ReadBody(hyper::Error),
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBackendUrl(err) => write!(f, "invalid backend URL: {err}"),
            Self::InvalidUri(err) => write!(f, "invalid upstream request URI: {err}"),
            Self::ReadRequestBody(err) => write!(f, "failed to read downstream request body: {err}"),
            Self::Request(err) => write!(f, "upstream request failed: {err}"),
            Self::ReadBody(err) => write!(f, "failed to read upstream response body: {err}"),
        }
    }
}

impl std::error::Error for UpstreamError {}

impl UpstreamClient {
    pub fn new() -> Self {
        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }

    pub async fn forward<B>(
        &self,
        backend: &str,
        request: Request<B>,
    ) -> Result<Response<Full<Bytes>>, UpstreamError>
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let upstream_uri = build_upstream_uri(backend, request.uri())?;
        let (parts, body) = request.into_parts();
        let body_bytes = body
            .collect()
            .await
            .map_err(|err| UpstreamError::ReadRequestBody(err.to_string()))?
            .to_bytes();

        let mut builder = Request::builder()
            .method(parts.method)
            .uri(upstream_uri)
            .version(parts.version);

        if let Some(headers) = builder.headers_mut() {
            for (name, value) in &parts.headers {
                if name != HOST {
                    headers.insert(name, value.clone());
                }
            }
        }

        let upstream_request = builder
            .body(Full::new(body_bytes))
            .expect("upstream request should be constructible");

        let upstream_response = self
            .client
            .request(upstream_request)
            .await
            .map_err(UpstreamError::Request)?;

        let (parts, body): (_, Incoming) = upstream_response.into_parts();
        let body_bytes = body
            .collect()
            .await
            .map_err(UpstreamError::ReadBody)?
            .to_bytes();

        let mut response = Response::builder()
            .status(parts.status)
            .version(parts.version);

        if let Some(headers) = response.headers_mut() {
            for (name, value) in &parts.headers {
                headers.insert(name, value.clone());
            }
        }

        Ok(response
            .body(Full::new(body_bytes))
            .expect("upstream response should be constructible"))
    }
}

impl Default for UpstreamClient {
    fn default() -> Self {
        Self::new()
    }
}

pub fn bad_gateway_response(err: &UpstreamError) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(Full::new(Bytes::from(format!("bad gateway: {err}\n"))))
        .expect("bad gateway response should be constructible")
}

fn build_upstream_uri(backend: &str, request_uri: &Uri) -> Result<Uri, UpstreamError> {
    let mut url = Url::parse(backend)
        .map_err(|err| UpstreamError::InvalidBackendUrl(err.to_string()))?;
    url.set_path(request_uri.path());
    url.set_query(request_uri.query());
    url.as_str().parse().map_err(UpstreamError::InvalidUri)
}

#[cfg(test)]
mod tests {
    use hyper::Uri;

    use super::build_upstream_uri;

    #[test]
    fn builds_upstream_uri_from_backend_and_request_path() {
        let request_uri: Uri = "/api/users?page=2".parse().unwrap();
        let upstream_uri = build_upstream_uri("http://127.0.0.1:3001", &request_uri).unwrap();

        assert_eq!(upstream_uri.to_string(), "http://127.0.0.1:3001/api/users?page=2");
    }
}
