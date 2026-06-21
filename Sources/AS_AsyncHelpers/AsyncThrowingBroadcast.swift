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
/// let broadcaster = AsyncThrowingBroadcast<String>()
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
/// await broadcaster.broadcast("Hello")
/// await broadcaster.fail(with: MyError.somethingWentWrong)
/// ```
public actor AsyncThrowingBroadcast<Element: Sendable> {
	let storage = ThrowingChannelStorage<Element>()
	var isFinished = false

	public init() {}

	/// Broadcast a value to all subscribers
	/// - Parameter element: The value to broadcast
	public func broadcast(_ element: Element) async {
		guard !isFinished else { return }

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
	/// - Returns: An AsyncThrowingStream that receives all broadcasted values
	///
	/// Each subscriber gets an independent stream. Multiple subscribers can iterate
	/// concurrently without interfering with each other.
	///
	/// The channel is registered **synchronously** before this method returns,
	/// so no broadcasts can be missed due to a registration race.
	nonisolated
	public func subscribe() -> AsyncThrowingStream<Element, any Error> {
		let id = UUID()
		let channel = AsyncThrowingChannel<Element, any Error>()

		storage.insert(channel, id: id)

		return AsyncThrowingStream { continuation in
			Task { [weak self] in
				do {
					for try await element in channel {
						continuation.yield(element)
					}
					continuation.finish()
				} catch {
					continuation.finish(throwing: error)
				}

				if let self {
					self.storage.remove(id)
				}
			}

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

	/// Finish all active channels with an error
	/// - Parameter error: The error to propagate to all subscribers
	public func fail(with error: any Error) {
		isFinished = true
		for channel in storage.allChannels() {
			channel.fail(error)
		}
		storage.removeAll()
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
		onError: @escaping @Sendable (any Error) -> Void = { _ in }
	) -> Task<Void, Never> {
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
		storage.count
	}
}
