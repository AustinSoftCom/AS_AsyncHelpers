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
/// await broadcaster.broadcast("Hello")
/// ```
public actor CurrentAsyncBroadcast<Element: Sendable> {
	/// Thread-safe channel storage so `subscribe()` can register channels
	/// synchronously (before returning), eliminating the race where broadcasts
	/// arrive before the channel is registered.
	let storage = ChannelStorage<Element>()
	let currentValue: Mutex<Element>
	var isFinished = false

	public var value: Element {
		currentValue.withLock { $0 }
	}

	public init(initialValue: Element) {
		currentValue = .init(initialValue)
	}

	/// Broadcast a value to all subscribers
	/// - Parameter element: The value to broadcast
	///
	/// This method implements per-consumer backpressure. If any consumer is slow,
	/// only that consumer's channel will apply backpressure. Fast consumers continue
	/// unaffected by slow consumers.
	public func broadcast(_ element: Element) async {
		guard !isFinished else { return }

		currentValue.withLock { $0 = element }

		let channels = storage.allChannels()
		await withTaskGroup(of: Void.self) { group in
			for channel in channels {
				group.addTask {
					await channel.send(element)
				}
			}
		}
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
	public func subscribe() -> AsyncStream<Element> {
		let id = UUID()
		let channel = AsyncChannel<Element>()

		// Register the channel immediately (thread-safe, no Task deferral)
		storage.insert(channel, id: id)

		let currentValue = self.currentValue.withLock { $0 }

		// Return an AsyncStream that consumes from the channel
		return AsyncStream { continuation in
			Task { [weak self] in
				// Send the current value first
				continuation.yield(currentValue)

				// Iterate the channel and yield to the continuation
				for await element in channel {
					continuation.yield(element)
				}
				continuation.finish()

				// Clean up when iteration completes
				if let self {
					self.storage.remove(id)
				}
			}

			// Handle early cancellation
			continuation.onTermination = { @Sendable [weak self] _ in
				self?.storage.remove(id)
			}
		}
	}

	/// Finish all active channels and prevent new broadcasts
	public func finish() {
		isFinished = true
		for channel in storage.allChannels() {
			channel.finish()
		}
		storage.removeAll()
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
		storage.count
	}
}
