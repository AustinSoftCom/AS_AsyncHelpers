//  Copyright © 2026 AustinSoft.com. All rights reserved worldwide.
//  Created by Glenn L. Austin on 4/14/26

import Foundation
import AsyncAlgorithms
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
/// let broadcaster = AsyncBroadcast<String>()
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
/// await broadcaster.yield("Hello")
/// ```
public actor AsyncBroadcast<Element: Sendable> {
	let core: BroadcastCore<Element, Never>

	public init() {
		self.core = .init()
	}
	
	public init(stream: consuming AsyncStream<Element>) {
		self.core = .init(stream: stream)
	}

	/// Broadcast a value to all subscribers
	/// - Parameter element: The value to broadcast
	///
	/// This method implements per-consumer backpressure. If any consumer is slow,
	/// only that consumer's channel will apply backpressure. Fast consumers continue
	/// unaffected by slow consumers.
	public func yield(_ element: Element) async {
		await core.yield(element)
	}
	
	@available(*, deprecated, renamed: "yield", message: "Renamed to yield so AsyncStream code doesn't *have* to change")
	public func broadcast(_ element: Element) async {
		await core.yield(element)
	}

	/// Subscribe to the broadcast stream
	/// - Returns: An AsyncStream that receives all broadcasted values
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
				finish: { error in
					let n = drops.withLock { $0 }
					if n > 0 {
						log.error("subscriber \(id) dropped \(n) elements")
					}
					if let error {
						log.fatal("Received continuation when I should never receive one: \(error, privacy: .private)")
						fatalError("Received continuation when I should never receive one: \(error)")
					} else {
						continuation.finish()
					}
				}
			),
			id: id
		)
		
		return stream
	}

	/// Finish all active channels and prevent new broadcasts
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
