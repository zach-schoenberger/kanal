#![forbid(unsafe_code)]
use core::fmt;
use core::fmt::Debug;
/// Error type for channel send operations without timeout
#[derive(PartialEq, Eq)]
pub enum SendError<T> {
    /// Indicates that the channel is closed on both sides with
    /// call to `close()`
    Closed(T),
    /// Indicates that all receiver instances are dropped and the channel is
    /// closed from the receive side
    ReceiveClosed(T),
}
impl<T> core::error::Error for SendError<T> {}
impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed(_) => f.debug_tuple("Closed").finish(),
            Self::ReceiveClosed(_) => f.debug_tuple("ReceiveClosed").finish(),
        }
    }
}
impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(
            match *self {
                SendError::Closed(_) => "send to a closed channel",
                SendError::ReceiveClosed(_) => "send to a half closed channel",
            },
            f,
        )
    }
}

/// Error type for channel send operations with timeout
#[derive(PartialEq, Eq)]
pub enum SendErrorTimeout<T> {
    /// Indicates that the channel is closed on both sides with a call to
    /// `close()`
    Closed(T),
    /// Indicates that all receiver instances are dropped and the channel is
    /// closed from the receive side
    ReceiveClosed(T),
    /// Indicates that channel operation reached timeout and is canceled
    Timeout(T),
}
impl<T> core::error::Error for SendErrorTimeout<T> {}
impl<T> fmt::Debug for SendErrorTimeout<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed(_) => f.debug_tuple("Closed").finish(),
            Self::ReceiveClosed(_) => f.debug_tuple("ReceiveClosed").finish(),
            Self::Timeout(_) => f.debug_tuple("Timeout").finish(),
        }
    }
}
impl<T> fmt::Display for SendErrorTimeout<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(
            match *self {
                SendErrorTimeout::Closed(_) => "send to a closed channel",
                SendErrorTimeout::ReceiveClosed(_) => "send to a half closed channel",
                SendErrorTimeout::Timeout(_) => "send timeout",
            },
            f,
        )
    }
}

/// Error type for channel receive operations without timeout
#[derive(Debug, PartialEq, Eq)]
pub enum ReceiveError {
    /// Indicates that the channel is closed on both sides with a call to
    /// `close()`
    Closed,
    /// Indicates that all sender instances are dropped and the channel is
    /// closed from the send side
    SendClosed,
}
impl core::error::Error for ReceiveError {}
impl fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(
            match *self {
                ReceiveError::Closed => "receive from a closed channel",
                ReceiveError::SendClosed => "receive from a half closed channel",
            },
            f,
        )
    }
}

/// Error type for channel receive operations with timeout
#[derive(Debug, PartialEq, Eq)]
pub enum ReceiveErrorTimeout {
    /// Indicates that the channel is closed on both sides with a call to
    /// `close()`
    Closed,
    /// Indicates that all sender instances are dropped and the channel is
    /// closed from the send side
    SendClosed,
    /// Indicates that channel operation reached timeout and is canceled
    Timeout,
}
impl core::error::Error for ReceiveErrorTimeout {}
impl fmt::Display for ReceiveErrorTimeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(
            match *self {
                ReceiveErrorTimeout::Closed => "receive from a closed channel",
                ReceiveErrorTimeout::SendClosed => "receive from a half closed channel",
                ReceiveErrorTimeout::Timeout => "receive timeout",
            },
            f,
        )
    }
}

/// Error type for closing a channel when channel is already closed
#[derive(Debug, PartialEq, Eq)]
pub struct CloseError();
impl core::error::Error for CloseError {}
impl fmt::Display for CloseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt("channel is already closed", f)
    }
}
