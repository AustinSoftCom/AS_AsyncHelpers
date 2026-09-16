# AS_AsyncHelpers

These are helpers for using async/await in Swift 6 projects. They are inspired by the behavior of Combine, with the big benefit being that it supports multiple consumers from a single source — unlike AsyncStream.

The biggest ones are AsyncBroadcast — which wraps AsyncStream and allows multiple listeners — and
AsyncThrowingBroadcast — which wraps AsyncThrowingStream and allows multiple listeners.
There are also CurrentAsyncBroadcast and CurrentAsyncThrowingBroadcast which provide a CurrentValueSubject-like stream.
