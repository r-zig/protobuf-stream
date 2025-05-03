# protobuf-stream
A Rust library for robust decoding of length-delimited Protocol Buffer messages from streams. This crate focuses on resilience, allowing continuous processing even when encountering corrupted or malformed data.
## Features

Decode length-delimited protobuf messages from any Read source
Automatic recovery from corrupted data segments
Simple, intuitive API that works with any Protobuf message type

Use Cases

Handling network streams where messages may be corrupted
Working with untrusted or potentially malformed input sources
Building robust data processing pipelines
