//  Copyright © 2026 Glenn L. Austin (AustinSoft.com)
//  Licensed under the MIT License. See LICENSE.txt for details.

import Foundation
import Synchronization

// MARK: - Multi-Consumer AsyncChannel Broadcaster

/// A multi-consumer broadcaster that wraps AsyncChannel for independent backpressure per consumer.
///
/// Unlike AsyncStream which only supports a single consumer, AsyncMultiChannel allows multiple
/// independent consumers to receive all broadcasted values. Each consumer gets their own
/// AsyncChannel with independent backpressure semantics.
///
/// Usage:
/// ```swift
/// let broadcaster = CurrentAsyncBroadcast<String>(initialValue: "String")
///
/// // Multiple consumers
/// Task {
///     for await value in broadcaster.subscribe() {
///         print("Consumer 1: \(value)")
///     }
/// }
///
/// Task {
///     for await value in broadcaster.subscribe() {
///         print("Consumer 2: \(value)")
///     }
/// }
///
/// // Producer
/// broadcaster.yield("Hello")
/// ```
public actor CurrentAsyncBroadcast<Element: Sendable> {
	let core: BroadcastCore<Element, Never>
	
	/// The most recently broadcast value.
	///
	/// New subscribers receive this value first, before any subsequent broadcasts.
	public var value: Element {
		core.value
	}

	/// Initialize with a starting value so you can use this as an equivalent
	/// to `CurrentValueSubject` from Combine.
	/// - Parameter initialValue: The value replayed to subscribers until the
	///   first broadcast.
	public init(initialValue: Element) {
		self.core = BroadcastCore(initialValue: .value(initialValue))
	}

	/// Initialize to wrap an existing `AsyncStream` to turn it into a
	/// broadcaster without the single-consumer aspect of `AsyncStream`.
	/// - Parameters:
	///   - initialValue: The value replayed to subscribers until the stream
	///     produces its first element.
	///   - stream: The upstream stream whose elements are rebroadcast to all
	///     subscribers.
	public init(initialValue: Element, stream: consuming AsyncStream<Element>) {
		self.core = .init(initialValue: .value(initialValue), stream: stream)
	}
	
	/// Broadcast a value to all subscribers.
	/// - Parameter element: The value to broadcast
	///
	/// Synchronous and callable from any context, like `AsyncStream.Continuation.yield`: every
	/// subscriber has the value buffered before this returns, so values arrive in the order they
	/// were yielded. Each subscriber has its own buffer, so a slow consumer doesn't hold up the
	/// others. (Before 2.3.0 this was `async`. Call it directly rather than as
	/// `Task { await broadcaster.yield(value) }`: separate Tasks can run in any order.)
	nonisolated
	public func yield(_ element: Element) {
		core.send(element)
	}
	
	/// Broadcast a value to all subscribers.
	/// - Parameter element: The value to broadcast
	///
	/// Deprecated alias for ``yield(_:)``.
	@available(*, deprecated, renamed: "yield", message: "Renamed to yield so AsyncStream code doesn't *have* to change")
	public func broadcast(_ element: Element) async {
		core.send(element)
	}
	
	/// Subscribe to the broadcast stream
	/// - Parameter bufferSize: The maximum number of elements buffered for this
	///   subscriber. When the buffer is full the newest elements are kept and the
	///   oldest are dropped.
	/// - Returns: An AsyncStream that receives the current value followed by all
	///   subsequently broadcasted values
	///
	/// Each subscriber gets an independent stream. Multiple subscribers can iterate
	/// concurrently without interfering with each other.
	///
	/// The channel is registered **synchronously** before this method returns,
	/// so no broadcasts can be missed due to a registration race.
	nonisolated
	public func subscribe(bufferSize: Int = 64) -> AsyncStream<Element> {
		let id = UUID()
		let (stream, continuation) = AsyncStream<Element>.makeStream(
			bufferingPolicy: .bufferingNewest(bufferSize)
		)
		let core = self.core
		continuation.onTermination = { @Sendable _ in core.remove(id) }   // set before register
		
		let drops = Mutex(0)
		
		core.register(
			.init(
				send: {
					if case .dropped = continuation.yield($0) {
						drops.withLock { $0 += 1 }
					}
				},
				finish: { _ in
					let n = drops.withLock { $0 }
					if n > 0 {
						logger.error("subscriber \(id) dropped \(n) elements")
					}
					continuation.finish()
				}
			),
			id: id
		)
		
		return stream
	}
	
	/// Finish all active channels and prevent new broadcasts
	nonisolated
	public func finish() {
		core.finish()
	}
	
	/// Subscribe with a callback instead of async iteration
	/// - Parameter handler: A closure called for each broadcasted value
	/// - Returns: A Task that can be cancelled to stop receiving values
	@discardableResult
	nonisolated
	public func sink(_ handler: @escaping @Sendable (Element) -> Void) -> Task<Void, Never> {
		Task {
			for await element in subscribe() {
				handler(element)
			}
		}
	}
	
	/// Get the current number of active subscribers
	public var subscriberCount: Int {
		core.subscriberCount
	}
}
