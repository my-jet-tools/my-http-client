pub enum DetectedBodySize {
    Unknown,
    Known(usize),
    Chunked,
    #[cfg(feature = "with-websocket")]
    WebSocketUpgrade,
}

impl DetectedBodySize {
    pub fn is_unknown(&self) -> bool {
        matches!(self, DetectedBodySize::Unknown)
    }
}
