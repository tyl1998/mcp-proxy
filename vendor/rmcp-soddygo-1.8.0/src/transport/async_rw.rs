use std::{marker::PhantomData, sync::Arc};

use futures::SinkExt;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
    sync::Mutex,
};
use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::{Decoder, Encoder, FramedWrite},
};

use super::{IntoTransport, Transport};
use crate::{
    model::ErrorData,
    service::{RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage},
};

#[non_exhaustive]
pub enum TransportAdapterAsyncRW {}

impl<Role, R, W> IntoTransport<Role, std::io::Error, TransportAdapterAsyncRW> for (R, W)
where
    Role: ServiceRole,
    R: AsyncRead + Send + 'static + Unpin,
    W: AsyncWrite + Send + 'static + Unpin,
{
    fn into_transport(self) -> impl Transport<Role, Error = std::io::Error> + 'static {
        AsyncRwTransport::new(self.0, self.1)
    }
}

#[non_exhaustive]
pub enum TransportAdapterAsyncCombinedRW {}
impl<Role, S> IntoTransport<Role, std::io::Error, TransportAdapterAsyncCombinedRW> for S
where
    Role: ServiceRole,
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    fn into_transport(self) -> impl Transport<Role, Error = std::io::Error> + 'static {
        IntoTransport::<Role, std::io::Error, TransportAdapterAsyncRW>::into_transport(
            tokio::io::split(self),
        )
    }
}

pub type TransportWriter<Role, W> = FramedWrite<W, JsonRpcMessageCodec<TxJsonRpcMessage<Role>>>;

pub struct AsyncRwTransport<Role: ServiceRole, R: AsyncRead, W: AsyncWrite> {
    read: BufReader<R>,
    line_buf: Vec<u8>,
    write: Arc<Mutex<Option<TransportWriter<Role, W>>>>,
    _role: PhantomData<fn() -> Role>,
}

impl<Role: ServiceRole, R, W> AsyncRwTransport<Role, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    pub fn new(read: R, write: W) -> Self {
        let read = BufReader::new(read);
        let write = Arc::new(Mutex::new(Some(FramedWrite::new(
            write,
            JsonRpcMessageCodec::<TxJsonRpcMessage<Role>>::default(),
        ))));
        Self {
            read,
            line_buf: Vec::new(),
            write,
            _role: PhantomData,
        }
    }
}

#[cfg(feature = "client")]
impl<R, W> AsyncRwTransport<crate::RoleClient, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    pub fn new_client(read: R, write: W) -> Self {
        Self::new(read, write)
    }
}

#[cfg(feature = "server")]
impl<R, W> AsyncRwTransport<crate::RoleServer, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    pub fn new_server(read: R, write: W) -> Self {
        Self::new(read, write)
    }
}

impl<Role: ServiceRole, R, W> Transport<Role> for AsyncRwTransport<Role, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<Role>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let lock = self.write.clone();
        async move {
            let mut write = lock.lock().await;
            if let Some(ref mut write) = *write {
                write.send(item).await.map_err(Into::into)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Transport is closed",
                ))
            }
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<Role>> {
        loop {
            self.line_buf.clear();
            match self.read.read_until(b'\n', &mut self.line_buf).await {
                Ok(0) => return None,
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("Error reading from stream: {}", e);
                    return None;
                }
            }
            let line = without_carriage_return(
                self.line_buf.strip_suffix(b"\n").unwrap_or(&self.line_buf),
            );
            if line.is_empty() {
                continue;
            }
            match try_parse_with_compatibility::<RxJsonRpcMessage<Role>>(line, "receive") {
                Ok(Some(msg)) => return Some(msg),
                Ok(None) => continue,
                Err(JsonRpcMessageCodecError::Serde(e)) => {
                    tracing::debug!("Parse error on incoming message: {e}");
                    let mut write = self.write.lock().await;
                    let framed = write.as_mut()?;
                    let response = TxJsonRpcMessage::<Role>::error(
                        ErrorData::parse_error("Parse error", None),
                        None,
                    );
                    if framed.send(response).await.is_err() {
                        return None;
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {}", e);
                    return None;
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        let mut write = self.write.lock().await;
        drop(write.take());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct JsonRpcMessageCodec<T> {
    _marker: PhantomData<fn() -> T>,
    next_index: usize,
    max_length: usize,
    is_discarding: bool,
}

impl<T> Default for JsonRpcMessageCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JsonRpcMessageCodec<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
            next_index: 0,
            max_length: usize::MAX,
            is_discarding: false,
        }
    }

    pub fn new_with_max_length(max_length: usize) -> Self {
        Self {
            max_length,
            ..Self::new()
        }
    }

    pub fn max_length(&self) -> usize {
        self.max_length
    }
}

fn without_carriage_return(s: &[u8]) -> &[u8] {
    s.strip_suffix(b"\r").unwrap_or(s)
}

/// UTF-8 byte order mark. RFC 8259 §8.1 allows JSON parsers to ignore a leading BOM.
const UTF8_BOM: &[u8; 3] = b"\xEF\xBB\xBF";

/// Check if a method is a standard MCP method (request, response, or notification).
/// This includes both requests and notifications defined in the MCP specification.
///
/// Based on MCP specification 2025-06-18: https://modelcontextprotocol.io/specification/2025-06-18
fn is_standard_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "ping"
            | "prompts/get"
            | "prompts/list"
            | "resources/list"
            | "resources/read"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "resources/templates/list"
            | "tools/call"
            | "tools/list"
            | "completion/complete"
            | "logging/setLevel"
            | "roots/list"
            | "sampling/createMessage"
    ) || is_standard_notification(method)
}

fn is_standard_notification(method: &str) -> bool {
    matches!(
        method,
        "notifications/cancelled"
            | "notifications/initialized"
            | "notifications/message"
            | "notifications/progress"
            | "notifications/prompts/list_changed"
            | "notifications/resources/list_changed"
            | "notifications/resources/updated"
            | "notifications/roots/list_changed"
            | "notifications/tools/list_changed"
    )
}

/// Determines if a notification should be ignored for compatibility.
fn should_ignore_notification(json_value: &serde_json::Value, method: &str) -> bool {
    let is_notification = json_value.get("id").is_none();

    // Ignore non-MCP notifications (like LSP messages) for compatibility
    if is_notification && !is_standard_method(method) {
        tracing::trace!(
            "Ignoring non-MCP notification '{}' for compatibility",
            method
        );
        return true;
    }

    // Ignore non-standard MCP notifications
    matches!(
        (
            method.starts_with("notifications/"),
            is_standard_notification(method)
        ),
        (true, false)
    )
}

/// Try to parse a message with compatibility handling for non-standard notifications
/// and non-JSON content (e.g., log messages from MCP servers that output to stdout
/// instead of stderr).
///
/// This function implements a tolerant parsing strategy:
/// 1. Try to parse as the expected MCP message type
/// 2. If that fails but the content is valid JSON, check if it's a non-standard
///    notification that should be ignored
/// 3. If the content is not valid JSON at all (e.g., plain text logs), skip it
///    gracefully instead of causing the connection to terminate
fn try_parse_with_compatibility<T: serde::de::DeserializeOwned>(
    line: &[u8],
    context: &str,
) -> Result<Option<T>, JsonRpcMessageCodecError> {
    let line = line.strip_prefix(UTF8_BOM.as_slice()).unwrap_or(line);
    if let Ok(line_str) = std::str::from_utf8(line) {
        // Skip empty lines
        if line_str.trim().is_empty() {
            return Ok(None);
        }

        match serde_json::from_slice(line) {
            Ok(item) => Ok(Some(item)),
            Err(e) => {
                // Check if this is a notification that should be ignored for compatibility
                if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(line_str) {
                    if let Some(method) =
                        json_value.get("method").and_then(serde_json::Value::as_str)
                    {
                        if should_ignore_notification(&json_value, method) {
                            return Ok(None);
                        }
                    }
                    // Valid JSON but not a recognized MCP message - this is an error
                    tracing::debug!(
                        "Failed to parse message {}: {} | Error: {}",
                        context,
                        line_str,
                        e
                    );
                    return Err(JsonRpcMessageCodecError::Serde(e));
                }

                // Not valid JSON - this is likely a log message or other non-JSON output
                // Skip it gracefully to maintain compatibility with MCP servers that
                // incorrectly output logs to stdout instead of stderr
                tracing::warn!(
                    "Skipping non-JSON output from MCP server ({}): {}",
                    context,
                    line_str
                );
                Ok(None)
            }
        }
    } else {
        // Non-UTF8 bytes, try to parse as JSON directly
        match serde_json::from_slice(line) {
            Ok(item) => Ok(Some(item)),
            Err(_e) => {
                tracing::warn!(
                    "Skipping invalid UTF-8 output from MCP server ({}): {:?}",
                    context,
                    line
                );
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JsonRpcMessageCodecError {
    #[error("max line length exceeded")]
    MaxLineLengthExceeded,
    #[error("serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
}

impl From<JsonRpcMessageCodecError> for std::io::Error {
    fn from(value: JsonRpcMessageCodecError) -> Self {
        match value {
            JsonRpcMessageCodecError::MaxLineLengthExceeded => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, value)
            }
            JsonRpcMessageCodecError::Serde(e) => e.into(),
            JsonRpcMessageCodecError::Io(e) => e,
        }
    }
}

impl<T: DeserializeOwned> Decoder for JsonRpcMessageCodec<T> {
    type Item = T;

    type Error = JsonRpcMessageCodecError;

    fn decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<Self::Item>, JsonRpcMessageCodecError> {
        loop {
            // Determine how far into the buffer we'll search for a newline. If
            // there's no max_length set, we'll read to the end of the buffer.
            let read_to = std::cmp::min(self.max_length.saturating_add(1), buf.len());

            let newline_offset = buf[self.next_index..read_to]
                .iter()
                .position(|b| *b == b'\n');

            match (self.is_discarding, newline_offset) {
                (true, Some(offset)) => {
                    // If we found a newline, discard up to that offset and
                    // then stop discarding. On the next iteration, we'll try
                    // to read a line normally.
                    buf.advance(offset + self.next_index + 1);
                    self.is_discarding = false;
                    self.next_index = 0;
                }
                (true, None) => {
                    // Otherwise, we didn't find a newline, so we'll discard
                    // everything we read. On the next iteration, we'll continue
                    // discarding up to max_len bytes unless we find a newline.
                    buf.advance(read_to);
                    self.next_index = 0;
                    if buf.is_empty() {
                        return Ok(None);
                    }
                }
                (false, Some(offset)) => {
                    // Found a line!
                    let newline_index = offset + self.next_index;
                    self.next_index = 0;
                    let line = buf.split_to(newline_index + 1);
                    let line = &line[..line.len() - 1];
                    let line = without_carriage_return(line);

                    // Use compatibility handling function
                    let item = match try_parse_with_compatibility(line, "decode")? {
                        Some(item) => item,
                        None => continue, // Skip non-standard message or non-JSON content, continue to next line
                    };
                    return Ok(Some(item));
                }
                (false, None) if buf.len() > self.max_length => {
                    // Reached the maximum length without finding a
                    // newline, return an error and start discarding on the
                    // next call.
                    self.is_discarding = true;
                    return Err(JsonRpcMessageCodecError::MaxLineLengthExceeded);
                }
                (false, None) => {
                    // We didn't find a line or reach the length limit, so the next
                    // call will resume searching at the current offset.
                    self.next_index = read_to;
                    return Ok(None);
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<T>, JsonRpcMessageCodecError> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                self.next_index = 0;
                // No terminating newline - return remaining data, if any
                if buf.is_empty() || buf == &b"\r"[..] {
                    None
                } else {
                    let line = buf.split_to(buf.len());
                    let line = without_carriage_return(&line);

                    // Use compatibility handling function
                    let item = match try_parse_with_compatibility(line, "decode_eof")? {
                        Some(item) => item,
                        None => return Ok(None), // Skip non-standard message
                    };
                    Some(item)
                }
            }
        })
    }
}

impl<T: Serialize> Encoder<T> for JsonRpcMessageCodec<T> {
    type Error = JsonRpcMessageCodecError;

    fn encode(&mut self, item: T, buf: &mut BytesMut) -> Result<(), JsonRpcMessageCodecError> {
        serde_json::to_writer(buf.writer(), &item)?;
        buf.put_u8(b'\n');
        Ok(())
    }
}
