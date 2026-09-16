//  Copyright © 2026 AustinSoft.com. All rights reserved worldwide.
//  Created by Glenn L. Austin on 4/14/26

import Foundation
import AsyncAlgorithms
import Synchronization

// MARK: - Throwing Multi-Consumer AsyncChannel Broadcaster

/// A multi-consumer broadcaster with error propagation.
///
/// Like ``AsyncBroadcast``, this allows multiple independent consumers to
/// receive all broadcasted values. Additionally, errors can be propagated to
/// all subscribers via ``fail(with:)``.
///
/// Usage:
/// ```swift
/// let broadcaster = CurrentAsyncThrowingBroadcast<String>(initialValue: "Foo")
///
/// // Multiple consumers
/// Task {
///     do {
///         for try await value in broadcaster.subscribe() {
///             print("Consumer 1: \(value)")
///         }
///     } catch {
///         print("Consumer 1 error: \(error)")
///     }
/// }
///
/// // Producer
/// await broadcaster.yield("Hello")
/// await broadcaster.fail(with: MyError.somethingWentWrong)
/// ```
public actor CurrentAsyncThrowingBroadcast<Element: Sendable, Failure: Error> {
	let core: BroadcastCore<Element, Failure>
	
	public var value: Element {
		core.value
	}
	
	public init(initialValue: Element) {
		self.core = BroadcastCore(initialValue: .value(initialValue))
	}
	
	public init(initialValue: Element, stream: consuming AsyncThrowingStream<Element, Failure>) {
		self.core = .init(initialValue: .value(initialValue), stream: stream)
	}

	/// Broadcast a value to all subscribers
	/// - Parameter element: The value to broadcast
	public func yield(_ element: Element) async {
		await core.yield(element)
	}
	
	@available(*, deprecated, renamed: "yield", message: "Renamed to yield so AsyncStream code doesn't *have* to change")
	public func broadcast(_ element: Element) async {
		await core.yield(element)
	}
	
	/// Subscribe to the broadcast stream
	/// - Returns: An AsyncThrowingStream that receives all broadcasted values
	///
	/// Each subscriber gets an independent stream. Multiple subscribers can iterate
	/// concurrently without interfering with each other.
	///
	/// The channel is registered **synchronously** before this method returns,
	/// so no broadcasts can be missed due to a registration race.
	nonisolated
	public func subscribe(bufferSize: Int = 64) -> AsyncThrowingStream<Element, Failure> where Failure == any Error {
		let id = UUID()
		let (stream, continuation) = AsyncThrowingStream<Element, Failure>.makeStream(
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
						continuation.finish(throwing: error)
					} else {
						continuation.finish()
					}
				}
			),
			id: id
		)
		
		return stream
	}
	
	/// Finish all active channels normally
	public func finish() {
		core.finish()
	}
	
	/// Finish all active channels with an error
	/// - Parameter error: The error to propagate to all subscribers
	public func finish(throwing error: Failure) {
		core.finish(throwing: error)
	}
	
	/// Subscribe with a callback instead of async iteration
	/// - Parameters:
	///   - handler: A closure called for each broadcasted value
	///   - onError: A closure called if the stream fails with an error
	/// - Returns: A Task that can be cancelled to stop receiving values
	@discardableResult
	nonisolated
	public func sink(
		onValue handler: @escaping @Sendable (Element) -> Void,
		onError: @escaping @Sendable (Failure) -> Void = { _ in }
	) -> Task<Void, Never> where Failure == any Error {
		Task {
			do {
				for try await element in subscribe() {
					handler(element)
				}
			} catch {
				onError(error)
			}
		}
	}
	
	/// Get the current number of active subscribers
	public var subscriberCount: Int {
		core.subscriberCount
	}
}
