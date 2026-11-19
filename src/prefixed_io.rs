use std::{io, pin::Pin, task::{Context, Poll}};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// An I/O stream that yields an already-consumed byte prefix before reading
/// from the underlying stream. Writes always go directly to the underlying
/// stream. This is useful when protocol detection has read a request header
/// that must subsequently be passed to an HTTP parser without losing bytes.
pub struct PrefixedIo<S> {
    inner: S,
    prefix: Vec<u8>,
    offset: usize,
}

impl<S> PrefixedIo<S> {
    pub fn new(inner: S, prefix: impl Into<Vec<u8>>) -> Self {
        Self { inner, prefix: prefix.into(), offset: 0 }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = &self.prefix[self.offset..];
        if !remaining.is_empty() && buffer.remaining() > 0 {
            let count = remaining.len().min(buffer.remaining());
            buffer.put_slice(&remaining[..count]);
            self.offset += count;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::PrefixedIo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn reads_prefix_before_underlying_stream_without_losing_bytes() {
        let (mut peer, stream) = tokio::io::duplex(64);
        peer.write_all(b"BODY").await.unwrap();
        let mut stream = PrefixedIo::new(stream, b"HEAD".to_vec());
        let mut received = [0u8; 8];
        stream.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"HEADBODY");
    }

    #[tokio::test]
    async fn writes_bypass_prefix_and_reach_underlying_stream() {
        let (mut peer, stream) = tokio::io::duplex(64);
        let mut stream = PrefixedIo::new(stream, b"unread-prefix".to_vec());
        stream.write_all(b"request").await.unwrap();
        let mut received = [0u8; 7];
        peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"request");
    }

    #[tokio::test]
    async fn supports_prefix_larger_than_each_read_buffer() {
        let (_peer, stream) = tokio::io::duplex(64);
        let mut stream = PrefixedIo::new(stream, b"abcdef".to_vec());
        let mut first = [0u8; 2];
        let mut second = [0u8; 4];
        stream.read_exact(&mut first).await.unwrap();
        stream.read_exact(&mut second).await.unwrap();
        assert_eq!(&first, b"ab");
        assert_eq!(&second, b"cdef");
    }
}
