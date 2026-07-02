pub mod types;

pub use types::Psm;

use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

type CloseHook = Box<dyn FnOnce() + Send + 'static>;
type DynTransport = dyn L2capTransport;

trait L2capTransport: Send {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>>;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>>;

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>;

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>;
}

#[cfg_attr(not(any(target_os = "linux")), allow(dead_code))]
struct StreamTransport<T> {
    inner: T,
}

#[cfg_attr(not(any(target_os = "linux")), allow(dead_code))]
impl<T> StreamTransport<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> L2capTransport for StreamTransport<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct DuplexTransport {
    inner: DuplexStream,
    close_hook: Option<CloseHook>,
}

impl DuplexTransport {
    fn new(inner: DuplexStream, close_hook: Option<CloseHook>) -> Self {
        Self { inner, close_hook }
    }

    fn trigger_close(&mut self) {
        if let Some(close_hook) = self.close_hook.take() {
            close_hook();
        }
    }
}

impl Drop for DuplexTransport {
    fn drop(&mut self) {
        self.trigger_close();
    }
}

impl L2capTransport for DuplexTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.trigger_close();
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// An open L2CAP CoC channel providing a reliable ordered byte stream.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`]. The backing transport is
/// backend-specific; use [`pair`](Self::pair) for testing.
pub struct L2capChannel {
    inner: Pin<Box<DynTransport>>,
}

impl std::fmt::Debug for L2capChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L2capChannel").finish_non_exhaustive()
    }
}

impl L2capChannel {
    /// Create a connected in-memory pair for testing.
    #[must_use]
    pub fn pair(max_buf_size: usize) -> (Self, Self) {
        let (a, b) = tokio::io::duplex(max_buf_size);
        (Self::from_duplex(a), Self::from_duplex(b))
    }

    pub async fn close(&mut self) -> std::io::Result<()> {
        poll_fn(|cx| self.inner.as_mut().poll_close(cx)).await
    }

    #[cfg_attr(not(any(target_os = "linux")), allow(dead_code))]
    pub(crate) fn from_stream<T>(inner: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self {
            inner: Box::pin(StreamTransport::new(inner)),
        }
    }

    pub(crate) fn from_duplex(inner: DuplexStream) -> Self {
        Self {
            inner: Box::pin(DuplexTransport::new(inner, None)),
        }
    }

    #[cfg_attr(
        not(any(target_os = "android", target_vendor = "apple", test)),
        allow(dead_code)
    )]
    pub(crate) fn from_duplex_with_close_hook(
        inner: DuplexStream,
        close_hook: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(DuplexTransport::new(inner, Some(Box::new(close_hook)))),
        }
    }
}

impl AsyncRead for L2capChannel {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for L2capChannel {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.inner.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn pair_communicates() {
        let (mut a, mut b) = L2capChannel::pair(1024);
        a.write_all(b"hello").await.unwrap();
        let mut buf = [0_u8; 5];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[tokio::test]
    async fn pair_bidirectional() {
        let (mut a, mut b) = L2capChannel::pair(1024);
        a.write_all(b"ping").await.unwrap();
        let mut buf = [0_u8; 4];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        b.write_all(b"pong").await.unwrap();
        a.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
    }

    #[tokio::test]
    async fn explicit_close_yields_peer_eof() {
        let (mut a, mut b) = L2capChannel::pair(1024);
        a.write_all(b"hello").await.unwrap();

        let mut buf = [0_u8; 5];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        a.close().await.unwrap();

        let mut eof_buf = [0_u8; 1];
        let n = timeout(Duration::from_millis(100), b.read(&mut eof_buf))
            .await
            .expect("peer should observe EOF")
            .unwrap();
        assert_eq!(n, 0);

        a.close().await.unwrap();
    }

    #[tokio::test]
    async fn explicit_close_triggers_hook_once() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (inner, _peer) = tokio::io::duplex(1024);
        let mut channel = L2capChannel::from_duplex_with_close_hook(inner, {
            let counter = Arc::clone(&counter);
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        channel.close().await.unwrap();
        channel.close().await.unwrap();
        drop(channel);

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn drop_triggers_hook_once() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (inner, _peer) = tokio::io::duplex(1024);
        let channel = L2capChannel::from_duplex_with_close_hook(inner, {
            let counter = Arc::clone(&counter);
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        drop(channel);

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
