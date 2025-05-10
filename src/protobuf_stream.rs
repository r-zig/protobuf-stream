use bytes::BytesMut;
use futures::stream::Stream;
use prost::Message;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader, ReadBuf};
use tracing::error;

/// message size must be at least 2 bytes (one field message)
const MINIMUM_MESSAGE_SIZE: usize = 2;

pub struct ProtobufStream<R, M>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    reader: BufReader<R>,
    buffer: BytesMut,
    _marker: std::marker::PhantomData<M>,
}

impl<R, M> ProtobufStream<R, M>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            buffer: BytesMut::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Reads a length-prefixed Protobuf message from the stream.
    fn read_length_prefix(
        reader: &mut BufReader<R>,
        mut buffer: &mut BytesMut,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<usize>, ProtobufStreamError>> {
        if buffer.is_empty() {
            let read_count = match Pin::new(&mut *reader).poll_fill_buf(cx) {
                Poll::Ready(Ok(buf)) => {
                    if buf.is_empty() {
                        return Poll::Ready(Ok(None)); // EOF
                    }
                    buffer.extend_from_slice(buf);
                    buf.len()
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(ProtobufStreamError::NonRecoverable {
                        code: ErrorCode::Other,
                        source: Some(Box::new(e)),
                    }))
                }
                Poll::Pending => return Poll::Pending,
            };
            reader.consume(read_count);
        }
        match prost::encoding::decode_varint(&mut buffer) {
            Ok(len) => {
                if len < MINIMUM_MESSAGE_SIZE as u64 {
                    return Poll::Ready(Err(ProtobufStreamError::recoverable(
                        ErrorCode::InvalidMessageLength,
                    )));
                }
                Poll::Ready(Ok(Some(len as usize)))
            }
            Err(e) => Poll::Ready(Err(ProtobufStreamError::Recoverable {
                code: ErrorCode::DecodingFailed,
                source: Some(Box::new(e)),
            })),
        }
    }

    fn read_message_payload(
        reader: &mut BufReader<R>,
        buffer: &mut BytesMut,
        message_size: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ProtobufStreamError>> {
        while buffer.len() < message_size {
            let mut temp_buf = vec![0; message_size - buffer.len()];
            let mut read_buf = ReadBuf::new(&mut temp_buf);
            match Pin::new(&mut *reader).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                    return Poll::Ready(Err(ProtobufStreamError::NonRecoverable {
                        code: ErrorCode::Other,
                        source: Some(Box::new(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Unexpected EOF",
                        ))),
                    }));
                }
                Poll::Ready(Ok(())) => {
                    buffer.extend_from_slice(read_buf.filled());
                    reader.consume(read_buf.filled().len());
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(ProtobufStreamError::Other {
                        code: ErrorCode::Other,
                        message: "Failed to read message payload".to_string(),
                        source: Some(Box::new(e)),
                    }))
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl<R, M> Stream for ProtobufStream<R, M>
where
    R: AsyncBufRead + Unpin,
    M: Message + Default + Unpin,
{
    type Item = Result<M, ProtobufStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Step 1: Read the length prefix
        let message_size = match Self::read_length_prefix(&mut this.reader, &mut this.buffer, cx) {
            Poll::Ready(Ok(Some(len))) => len,
            Poll::Ready(Ok(None)) => return Poll::Ready(None), // EOF
            Poll::Ready(Err(e)) => {
                return Poll::Ready(Some(Err(e))); // Return the error immediately
            }
            Poll::Pending => return Poll::Pending, // Wait for more data
        };

        // Step 2: Read the message payload (only if no error occurred)
        match Self::read_message_payload(&mut this.reader, &mut this.buffer, message_size, cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Pending => return Poll::Pending, // Wait for more data
            Poll::Ready(Err(e)) => {
                return Poll::Ready(Some(Err(e))); // Return the error immediately
            }
        }

        // Step 3: Decode the Protobuf message (only if no error occurred)
        let message_buffer = this.buffer.split_to(message_size);
        match M::decode(message_buffer.clone()) {
            Ok(message) => Poll::Ready(Some(Ok(message))),
            Err(e) => {
                // If decoding fails, we need to handle the error
                // by returning the message buffer and the remaining buffer
                // and the next call will try to decode the next message
                let mut new_buffer = BytesMut::new();
                new_buffer.extend_from_slice(&message_buffer);
                new_buffer.extend_from_slice(&this.buffer);
                this.buffer = new_buffer; // Update the buffer

                error!("Failed to decode message: {}", e);
                Poll::Ready(Some(Err(ProtobufStreamError::Recoverable {
                    code: ErrorCode::Other,
                    source: Some(Box::new(e)),
                })))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ErrorCode {
    ConnectionLost,
    InvalidMessageLength,
    DecodingFailed,
    Other,
}

#[derive(Debug, Error)]
pub enum ProtobufStreamError {
    /// Represents a non-recoverable error such as connection failure
    #[error("Non-recoverable error: {code:?}")]
    NonRecoverable {
        code: ErrorCode,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },

    /// Represents recoverable errors such as decoding errors
    #[error("Recoverable error: {code:?}")]
    Recoverable {
        code: ErrorCode,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },

    /// Represents other unexpected errors with a description and an internal error
    #[error("{code:?}: {message}")]
    Other {
        code: ErrorCode,
        message: String,
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },
}

impl ProtobufStreamError {
    pub fn non_recoverable(code: ErrorCode) -> Self {
        ProtobufStreamError::NonRecoverable { code, source: None }
    }

    pub fn recoverable(code: ErrorCode) -> Self {
        ProtobufStreamError::Recoverable { code, source: None }
    }

    pub fn other(code: ErrorCode, message: impl Into<String>) -> Self {
        ProtobufStreamError::Other {
            code,
            message: message.into(),
            source: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use bytes::Buf;
    use bytes::Bytes;
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;
    use tracing::trace;

    use super::*;

    #[derive(Clone, PartialEq, Message)]
    pub struct MyMessage {
        #[prost(string, tag = "1")]
        pub data: String,
    }

    // A custom AsyncRead implementation that delays data availability
    struct DelayedReader {
        data: Bytes,
        delay_steps: usize,
        step: usize,
    }

    impl DelayedReader {
        fn new(data: Bytes, delay_steps: usize) -> Self {
            Self {
                data,
                delay_steps,
                step: 0,
            }
        }
    }

    impl AsyncRead for DelayedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.step < self.delay_steps {
                // Simulate delay by returning Poll::Pending
                self.step += 1;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            // Provide data after the delay
            let len = std::cmp::min(buf.remaining(), self.data.len());
            buf.put_slice(&self.data[..len]);
            self.data.advance(len);

            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn encode_decode_simple() {
        let expected_message = MyMessage {
            data: "my data".to_owned(),
        };

        // write the message into a buffer
        let mut buf = vec![];
        expected_message.encode_length_delimited(&mut buf).unwrap();
        let buf: Bytes = Bytes::from(buf);
        let decoded_message = MyMessage::decode_length_delimited(buf).unwrap();
        assert_eq!(expected_message, decoded_message);
    }

    // test the ProtobufStream with multiple messages
    // that are written to a buffer and then read from it
    // and compare the decoded messages with the original ones
    #[tokio::test]
    async fn test_protobuf_stream() {
        let expected_messages = vec![
            MyMessage {
                data: "message 1".to_owned(),
            },
            MyMessage {
                data: "message 2".to_owned(),
            },
        ];

        // write the messages into a buffer
        let mut buf = vec![];
        for message in &expected_messages {
            message.encode_length_delimited(&mut buf).unwrap();
        }
        let buf: Bytes = Bytes::from(buf);

        // create a ProtobufStream from the buffer
        let reader = BufReader::new(buf.as_ref()); // Wrap the bytes in a BufReader
        let stream = ProtobufStream::new(reader);

        // read the messages from the stream and compare with the original ones
        let mut decoded_messages: Vec<MyMessage> = vec![];
        tokio::pin!(stream);
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => decoded_messages.push(msg),
                Err(e) => trace!("As expected - error decoding message: {}", e),
            }
        }

        assert_eq!(expected_messages, decoded_messages);
    }

    // Test with a given transport recv stream
    // and a given transport send stream
    // and compare the decoded messages with the original ones
    #[tokio::test]
    async fn test_protobuf_stream_with_transport() {
        // Create a transport with a given recv stream and send stream
        let (send_stream, recv_stream) = tokio::io::duplex(1024);
        let mut send_buf = vec![];

        // Create a ProtobufStream from the recv stream
        let reader = BufReader::new(recv_stream);
        let mut stream = ProtobufStream::new(reader);

        // Create a ProtobufStream from the send stream
        let mut writer = send_stream;

        // write the messages into the send stream in a separate task
        let expected_messages = vec![
            MyMessage {
                data: "message 1".to_owned(),
            },
            MyMessage {
                data: "message 2".to_owned(),
            },
        ];
        let expected_messages_cloned = expected_messages.clone();
        let send_task = tokio::spawn(async move {
            for message in &expected_messages_cloned {
                message.encode_length_delimited(&mut send_buf).unwrap();
            }
            writer.write_all(&send_buf).await.unwrap();
        });

        // read the messages from the stream in a separate task
        let recv_task = tokio::spawn(async move {
            let mut decoded_messages = vec![];
            while let Some(message) = stream.next().await {
                match message {
                    Ok(msg) => decoded_messages.push(msg),
                    Err(e) => panic!("Error decoding message: {}", e),
                }
            }
            decoded_messages
        });

        // Wait for both tasks to complete
        let (send_result, decoded_messages) = tokio::join!(send_task, recv_task);

        // Ensure the send task completed successfully
        send_result.unwrap();

        assert_eq!(expected_messages, decoded_messages.unwrap());
    }

    // Test on invalid data
    #[tokio::test]
    async fn test_protobuf_stream_invalid_data() {
        let invalid_data = vec![0, 1, 2, 3, 4, 5]; // Invalid varint length prefix
        let reader = BufReader::new(invalid_data.as_slice());
        let mut stream = ProtobufStream::<_, MyMessage>::new(reader);

        // Read the message and expect an error
        match stream.next().await {
            Some(Err(ProtobufStreamError::Recoverable { code, .. })) => {
                assert_eq!(code, ErrorCode::InvalidMessageLength);
            }
            Some(Err(ProtobufStreamError::NonRecoverable { code, .. })) => {
                // assert_eq!(code, ErrorCode::Other);
                panic!("Expected a recoverable error, but got a non-recoverable error with error code: {:?}", code);
            }
            Some(Err(ProtobufStreamError::Other { code, .. })) => {
                // assert_eq!(code, ErrorCode::Other);
                panic!(
                    "Expected a recoverable error, but got a other error with error code: {:?}",
                    code
                );
            }
            Some(Ok(m)) => panic!("Expected an error, but got a message: {:?}", m),
            None => panic!("Some error expected, but got None"),
        }
    }

    // test bad message with good message and verify that the good message is decoded
    #[tokio::test]
    async fn test_recover_bad_message() {
        let mut buf = vec![];

        // Add invalid data to the buffer
        buf.extend_from_slice(&[1, 0, 3]);

        // Add valid data to the buffer
        let valid_data = MyMessage {
            data: "valid message".to_owned(),
        };
        valid_data.encode_length_delimited(&mut buf).unwrap();
        let buf: Bytes = Bytes::from(buf);

        // create a ProtobufStream from the buffer
        let reader = BufReader::new(buf.as_ref());
        let stream = ProtobufStream::new(reader);

        // read the messages from the stream and compare with the original ones
        let mut decoded_messages: Vec<MyMessage> = Vec::new();
        tokio::pin!(stream);
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => decoded_messages.push(msg),
                Err(e) => trace!("As expected, error decoding message: {}", e),
            }
        }

        assert_eq!(decoded_messages.len(), 1);
    }

    #[tokio::test]
    async fn test_protobuf_stream_with_delayed_data() {
        // Prepare the expected messages
        let expected_messages = vec![
            MyMessage {
                data: "message 1".to_owned(),
            },
            MyMessage {
                data: "message 2".to_owned(),
            },
        ];

        // Encode the messages into a buffer
        let mut buf = vec![];
        for message in &expected_messages {
            message.encode_length_delimited(&mut buf).unwrap();
        }
        let buf: Bytes = Bytes::from(buf);

        // Create a DelayedReader with a delay of 3 steps
        let reader = DelayedReader::new(buf, 3);
        let reader = BufReader::new(reader);

        // Create the ProtobufStream
        let mut stream = ProtobufStream::<_, MyMessage>::new(reader);

        // Read the messages from the stream
        let mut decoded_messages = vec![];
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => decoded_messages.push(msg),
                Err(e) => panic!("Unexpected error: {}", e),
            }
        }

        // Verify the decoded messages match the expected messages
        assert_eq!(expected_messages, decoded_messages);
    }
}
