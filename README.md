# AS_AsyncHelpers

[![CI](https://github.com/AustinSoftCom/AS_AsyncHelpers/actions/workflows/ci.yml/badge.svg)](https://github.com/AustinSoftCom/AS_AsyncHelpers/actions/workflows/ci.yml)

Multi-consumer broadcasting for Swift concurrency, inspired by Combine.

Swift's built-in `AsyncStream` and `AsyncThrowingStream` only support a **single consumer** — the first task to iterate the stream wins, and everyone else gets nothing. AS_AsyncHelpers fills that gap with a small set of actor-based broadcasters that let any number of independent consumers receive every value, with Combine-like semantics but built entirely on structured concurrency (no Combine dependency).

| Type | Combine analogue | Behavior |
|------|------------------|----------|
| `AsyncBroadcast<Element>` | `PassthroughSubject` | Broadcasts each value to all current subscribers |
| `AsyncThrowingBroadcast<Element, Failure>` | `PassthroughSubject` (with failure) | Same, and can terminate all subscribers with an error |
| `CurrentAsyncBroadcast<Element>` | `CurrentValueSubject` | Holds a current value; new subscribers receive it immediately |
| `CurrentAsyncThrowingBroadcast<Element, Failure>` | `CurrentValueSubject` (with failure) | Same, and can terminate all subscribers with an error |

## Features

- **Multiple independent consumers** — each call to `subscribe()` returns its own `AsyncStream` (or `AsyncThrowingStream`); subscribers never interfere with one another.
- **No missed values** — subscriptions are registered *synchronously* before `subscribe()` returns, so there is no window in which a broadcast can be lost to a registration race.
- **Per-consumer buffering** — a slow consumer only affects its own buffer. Each subscriber gets a bounded buffer (64 elements by default, configurable per subscription) that keeps the newest values; drops are reported through [swift-log](https://github.com/apple/swift-log).
- **Current-value replay** — the `Current…` variants replay the latest value to each new subscriber before delivering subsequent broadcasts, and expose it directly via the `value` property.
- **Error propagation** — the throwing variants can finish all subscribers with an error via `finish(throwing:)`.
- **Stream wrapping** — any existing `AsyncStream`/`AsyncThrowingStream` can be wrapped to give it multi-consumer behavior.
- **Late-subscriber safety** — subscribing after `finish()` yields a stream that terminates immediately (throwing the terminal error, if there was one).
- **Swift 6 native** — strict-concurrency clean, actor-based, `Sendable` throughout.

## Requirements

- Swift 6.3 toolchain or later (builds in Swift 6 language mode)
- macOS 15+, iOS 18+, tvOS 18+, watchOS 11+, or visionOS 2+

Dependencies: [swift-log](https://github.com/apple/swift-log) (used only to report dropped elements).

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/AustinSoftCom/AS_AsyncHelpers.git", from: "2.2.0"),
],
```

and add the product to any target that needs it:

```swift
.target(
    name: "MyTarget",
    dependencies: [
        .product(name: "AS_AsyncHelpers", package: "AS_AsyncHelpers"),
    ]
),
```

Or in Xcode: **File ▸ Add Package Dependencies…** and enter the repository URL.

## Usage

### AsyncBroadcast — a `PassthroughSubject` for async/await

```swift
import AS_AsyncHelpers

let broadcaster = AsyncBroadcast<String>()

// Any number of consumers
Task {
    for await value in broadcaster.subscribe() {
        print("Consumer 1: \(value)")
    }
}

Task {
    for await value in broadcaster.subscribe() {
        print("Consumer 2: \(value)")
    }
}

// Producer
await broadcaster.yield("Hello")   // both consumers receive "Hello"
await broadcaster.finish()         // both streams end
```

If you prefer callbacks over `for await`, use `sink`:

```swift
let task = broadcaster.sink { value in
    print("Received: \(value)")
}
// later…
task.cancel()
```

### AsyncThrowingBroadcast — propagate errors to every consumer

```swift
let broadcaster = AsyncThrowingBroadcast<String, any Error>()

Task {
    do {
        for try await value in broadcaster.subscribe() {
            print("Received: \(value)")
        }
    } catch {
        print("Stream failed: \(error)")
    }
}

await broadcaster.yield("Hello")
broadcaster.finish(throwing: MyError.somethingWentWrong)  // all consumers throw
```

### CurrentAsyncBroadcast — a `CurrentValueSubject` for async/await

```swift
let status = CurrentAsyncBroadcast<String>(initialValue: "idle")

// New subscribers immediately receive the current value ("idle"),
// then all subsequent broadcasts.
Task {
    for await state in status.subscribe() {
        print("State: \(state)")
    }
}

await status.yield("running")

// The current value is also available directly:
let current = await status.value   // "running"
```

`CurrentAsyncThrowingBroadcast` works the same way and adds `finish(throwing:)`.

### Wrapping an existing stream

Any single-consumer stream can be turned into a broadcaster. The upstream stream is consumed by the broadcaster and its elements (and terminal error, for throwing streams) are re-broadcast to all subscribers:

```swift
let (stream, continuation) = AsyncStream<Int>.makeStream()
let broadcaster = AsyncBroadcast(stream: stream)

// Now any number of tasks can subscribe to values
// that originally came from a single-consumer stream.
```

## Behavior notes

- **Buffering:** each subscription uses `AsyncStream.Continuation.BufferingPolicy.bufferingNewest` with a default size of 64, configurable via `subscribe(bufferSize:)`. If a consumer falls behind, its oldest buffered values are dropped (other consumers are unaffected), and the total drop count is logged when the subscription ends.
- **Backpressure isolation:** broadcasting fans out concurrently to all subscribers; a slow consumer never blocks the producer or other consumers.
- **After `finish()`:** further `yield` calls are no-ops, and new subscribers receive a stream that finishes immediately — with the terminal error, if the broadcaster was finished with one. The `Current…` variants still replay the last value before finishing.
- **Logging:** the package logs through swift-log with the label `AS_AsyncHelpers`. It emits nothing during normal operation; only dropped-element reports at `error` level.

## Migration from 1.x

- `AS_AsyncBroadcast` and `AS_AsyncThrowingBroadcast` were renamed to `AsyncBroadcast` and `AsyncThrowingBroadcast`. Deprecated typealiases keep old code compiling.
- `broadcast(_:)` was renamed to `yield(_:)` to match the `AsyncStream` vocabulary. A deprecated alias remains.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

AS_AsyncHelpers is released under the MIT License. See [LICENSE.txt](LICENSE.txt) for details.

Copyright © 2026 Glenn L. Austin (AustinSoft.com)
