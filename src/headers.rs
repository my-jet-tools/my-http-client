use crate::RequestBuildError;

pub trait MyHttpClientHeaders {
    fn copy_to(&self, buf: &mut Vec<u8>);
}

pub struct HeaderValuePosition {
    pub start: usize,
    pub end: usize,
}

pub struct MyHttpClientHeadersBuilder {
    headers: Vec<u8>,
}

impl Default for MyHttpClientHeadersBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MyHttpClientHeadersBuilder {
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
        }
    }

    /// Refuses a name or a value which must not be put on the wire - see
    /// [`write_header`]. Nothing is added when the header is refused
    pub fn add_header(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<HeaderValuePosition, RequestBuildError> {
        write_header(&mut self.headers, name, value)
    }

    /// `None` is a position which is not a value of this builder: it points outside of
    /// what is written, or into the middle of a char
    pub fn get_value(&self, value_position: &HeaderValuePosition) -> Option<&str> {
        let value = self.headers.get(value_position.start..value_position.end)?;

        std::str::from_utf8(value).ok()
    }

    pub fn iter(&self) -> MyHttpClientHeadersBuilderIterator<'_> {
        MyHttpClientHeadersBuilderIterator::new(&self.headers)
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.headers) }
    }
}

impl MyHttpClientHeaders for MyHttpClientHeadersBuilder {
    fn copy_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.headers);
    }
}

pub struct MyHttpClientHeadersBuilderIterator<'s> {
    itm: &'s [u8],
    pos: usize,
}

impl<'s> MyHttpClientHeadersBuilderIterator<'s> {
    pub fn new(itm: &'s [u8]) -> Self {
        Self { itm, pos: 0 }
    }
}

impl<'s> Iterator for MyHttpClientHeadersBuilderIterator<'s> {
    type Item = (&'s str, &'s str);

    /// The iteration is over at the first header which is not UTF-8. A builder never
    /// holds one, but the iterator can be made over any bytes
    fn next(&mut self) -> Option<Self::Item> {
        let header_start = self.pos;

        let header_end;

        loop {
            if self.pos == self.itm.len() {
                return None;
            }
            if self.itm[self.pos] == b':' {
                header_end = self.pos;
                break;
            }
            self.pos += 1;
        }

        self.pos += 2;

        let value_start = self.pos;
        let value_end;

        loop {
            if self.pos >= self.itm.len() {
                return None;
            }
            if self.itm[self.pos] == b'\r' {
                value_end = self.pos;
                break;
            }
            self.pos += 1;
        }
        self.pos += 2;

        Some((
            std::str::from_utf8(&self.itm[header_start..header_end]).ok()?,
            std::str::from_utf8(&self.itm[value_start..value_end]).ok()?,
        ))
    }
}

/// Refuses an empty name and a name which is not an HTTP token
pub fn validate_header_name(name: &str) -> Result<(), RequestBuildError> {
    if name.is_empty() {
        return Err(RequestBuildError::HeaderNameIsEmpty);
    }

    match name.bytes().find(|b| !is_valid_header_name_byte(*b)) {
        Some(b) => Err(RequestBuildError::ForbiddenByteInHeaderName(b)),
        None => Ok(()),
    }
}

/// Refuses CR, LF and NUL: a value which carries one would split the header block
pub fn validate_header_value(value: &str) -> Result<(), RequestBuildError> {
    match value.bytes().find(|b| matches!(b, b'\r' | b'\n' | 0)) {
        Some(b) => Err(RequestBuildError::ForbiddenByteInHeaderValue(b)),
        None => Ok(()),
    }
}

fn is_valid_header_name_byte(b: u8) -> bool {
    matches!(
        b,
        b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.'
        | b'^' | b'_' | b'`' | b'|' | b'~'
        | b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z'
    )
}

/// Refuses an empty name, a name which is not an HTTP token and a value with CR, LF or
/// NUL in it. `dest` is left as it was when the header is refused
pub fn write_header(
    dest: &mut Vec<u8>,
    name: &str,
    value: &str,
) -> Result<HeaderValuePosition, RequestBuildError> {
    validate_header_name(name)?;
    validate_header_value(value)?;
    Ok(append_header_line(dest, name.as_bytes(), value.as_bytes()))
}

/// Writes `name: value\r\n` as it is given. It is for a header which is known to be
/// fit for the wire: checked by [`write_header`], taken out of the typed
/// `HeaderName` and `HeaderValue`, or made of the constants of the crate
pub(crate) fn append_header_line(
    dest: &mut Vec<u8>,
    name: &[u8],
    value: &[u8],
) -> HeaderValuePosition {
    dest.extend_from_slice(name);
    dest.extend_from_slice(b": ");
    let start = dest.len();
    dest.extend_from_slice(value);
    let end = dest.len();
    dest.extend_from_slice(crate::CL_CR);
    HeaderValuePosition { start, end }
}

#[cfg(test)]
mod tests {
    use super::MyHttpClientHeadersBuilder;

    #[test]
    fn test_iterators() {
        let mut headers = MyHttpClientHeadersBuilder::new();

        headers.add_header("Content-Type", "text/plain").unwrap();
        headers.add_header("Content-Length", "123").unwrap();

        let mut iter = headers.iter();
        let (name, value) = iter.next().unwrap();
        assert_eq!(name, "Content-Type");
        assert_eq!(value, "text/plain");

        let (name, value) = iter.next().unwrap();
        assert_eq!(name, "Content-Length");
        assert_eq!(value, "123");

        assert!(iter.next().is_none());
    }
}
