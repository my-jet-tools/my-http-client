use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt};

#[derive(Debug)]
pub struct FullBodyReaderInner {
    pub builder: http::response::Builder,
    pub body: Vec<u8>,
}

impl FullBodyReaderInner {
    /// Fails when the builder carries an error - a header it was given and did not take
    pub fn into_body(self) -> Result<hyper::Response<BoxBody<Bytes, String>>, http::Error> {
        /*
        let builder = if set_body_size {
            self.builder
                .headers_mut()
                .unwrap()
                .remove(TRANSFER_ENCODING);
            self.builder
                .header(CONTENT_LENGTH, self.body.len().to_string())
        } else {
            self.builder
        };
         */

        let full_body = http_body_util::Full::new(hyper::body::Bytes::from(self.body));
        self.builder
            .body(full_body.map_err(|itm| itm.to_string()).boxed())
    }
}
