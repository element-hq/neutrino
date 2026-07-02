use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// Opaque event stream returned by [`Central::events`](crate::central::Central::events)
/// and [`Peripheral::state_events`](crate::peripheral::Peripheral::state_events).
pub struct EventStream<T, S: Stream<Item = T> + Unpin>(S, std::marker::PhantomData<T>);

impl<T, S: Stream<Item = T> + Unpin> EventStream<T, S> {
    pub(crate) fn new(stream: S) -> Self {
        Self(stream, std::marker::PhantomData)
    }
}

impl<T, S: Stream<Item = T> + Unpin> Stream for EventStream<T, S> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl<T, S: Stream<Item = T> + Unpin> Unpin for EventStream<T, S> {}

/// Wraps a [`tokio::sync::broadcast`] receiver as a `Stream<Item = T>`, silently dropping
/// the lag errors emitted by [`BroadcastStream`] when a slow subscriber falls behind.
///
/// State events are low-frequency; dropping a few under extreme load is preferable to
/// propagating a `Result` wrapper to the public API.
pub struct BroadcastEventStream<T: Clone + Send + 'static>(BroadcastStream<T>);

impl<T: Clone + Send + 'static> BroadcastEventStream<T> {
    #[must_use]
    pub fn new(rx: broadcast::Receiver<T>) -> Self {
        Self(BroadcastStream::new(rx))
    }
}

impl<T: Clone + Send + 'static> Stream for BroadcastEventStream<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.0).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => return Poll::Ready(Some(item)),
                Poll::Ready(Some(Err(_))) => {}
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T: Clone + Send + 'static> Unpin for BroadcastEventStream<T> {}
