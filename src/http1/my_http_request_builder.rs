use http::Method;

use super::my_http_request_builder_inner::MyHttpRequestBuilderInner;
use super::MyHttpRequest;
use crate::RequestBuildError;

/// The head of an HTTP/1.1 request: built step by step, then turned into a
/// [`MyHttpRequest`] by [`Self::build`] or [`Self::build_with_body`].
///
/// No step of it returns a `Result`. What a step can not take - a path and query or a
/// header which must not be put on the wire - becomes the state of the builder instead:
/// from the first such error on every step is skipped, and the build returns that error
/// without building anything.
///
/// ```
/// use my_http_client::http::Method;
/// use my_http_client::http1::MyHttpRequestBuilder;
///
/// // Whatever is wrong with the path or with a header comes out of `build()`
/// let request = MyHttpRequestBuilder::new(Method::GET, "/api/data")
///     .append_header("Host", "localhost")
///     .append_header("X-Api-Key", "secret")
///     .build()?;
/// # Ok::<(), my_http_client::RequestBuildError>(())
/// ```
pub struct MyHttpRequestBuilder {
    // The head of the request, for as long as everything it was given could be put on
    // the wire - and the first error once something could not. All the logic is in
    // `MyHttpRequestBuilderInner`; this type only decides whether there still is an
    // inner to hand a step to
    inner: Result<MyHttpRequestBuilderInner, RequestBuildError>,
}

impl MyHttpRequestBuilder {
    /// Never fails. CR, LF, NUL or a space in the path and query - written into the
    /// request line as they are, they would end it before its time - become the error
    /// the build returns
    pub fn new(method: Method, path_and_query: &str) -> Self {
        Self {
            inner: MyHttpRequestBuilderInner::new(method, path_and_query),
        }
    }

    /// The error the builder has met, if any - the one the build is going to return. It
    /// lets a path which came from settings be checked without building anything
    pub fn get_error(&self) -> Option<&RequestBuildError> {
        self.inner.as_ref().err()
    }

    /// An empty name, a name which is not an HTTP token and a value with CR, LF or NUL
    /// in it are not appended: the build returns the error instead. Skipped once the
    /// builder has met an error
    pub fn append_header(self, name: &str, value: &str) -> Self {
        Self {
            inner: self
                .inner
                .and_then(|inner| inner.append_header(name, value)),
        }
    }

    /// Adds `Content-Length` to a body which is not empty, unless the request has one
    pub fn build_with_body(self, body: Vec<u8>) -> Result<MyHttpRequest, RequestBuildError> {
        self.inner.map(|inner| inner.build_with_body(body))
    }

    pub fn build(self) -> Result<MyHttpRequest, RequestBuildError> {
        self.inner.map(MyHttpRequestBuilderInner::build)
    }
}
