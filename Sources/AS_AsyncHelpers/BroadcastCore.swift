//  Copyright © 2026 Glenn L. Austin (AustinSoft.com)
//  Licensed under the MIT License. See LICENSE.txt for details.

import Foundation
import Synchronization

final class BroadcastCore<Element: Sendable, Failure: Error>: Sendable {
	/// How we pass the values to the stream
	struct Sink: Sendable {
		/// Send values to the stream
		let send:   @Sendable (Element) -> Void
		/// Send the finish to the stream
		let finish: @Sendable (Failure?) -> Void
	}

	enum CurrentValue {
		case none
		case value(Element)
	}
	
	/// Stores the internal state so we can lock everything at once and don't need an external struct.
	struct State {
		/// For each stream we broadcast to, this is how we send it.
		var sinks: [UUID: Sink] = [:]
		/// The last element seen
		var last: CurrentValue = .none
		/// Whether our stream is finished, so we send at least one element
		var isFinished = false
		/// The error that terminated the stream
		var terminalError: Failure?
	}

	/// Protected storage for our state
	private let state = Mutex(State())
	
	let pump: Mutex<Task<Void, Never>?> = .init(nil)
	
	var isCancelled: Bool { pump.withLock({ $0?.isCancelled ?? true }) }
	
	var value: Element {
		state.withLock { s in
			guard case let .value(value) = s.last else {
				fatalError("Getting last value not supported for this stream")
			}
			return value
		}
	}
	
	init(initialValue: CurrentValue = .none) {
		state.withLock { s in
			s.last = initialValue
		}
	}

	init(
		initialValue: CurrentValue = .none,
		stream: consuming AsyncStream<Element>
	) {
		state.withLock { s in
			s.last = initialValue
		}
		self.pump.withLock { [upstream = stream] in
			$0 = Task { @Sendable [weak self] in
				for await element in upstream {
					guard let self else {
						break
					}
					await self.yield(element)
					state.withLock {
						if case .value = $0.last {
							$0.last = .value(element)
						}
					}
				}
				await self?.finish()
			}
		}
	}

	init(
		initialValue: CurrentValue = .none,
		stream: consuming AsyncThrowingStream<Element, Failure>
	) {
		state.withLock { s in
			s.last = initialValue
		}
		self.pump.withLock { [upstream = stream] in
			$0 = Task { @Sendable [weak self] in
				do {
					for try await element in upstream {
						guard let self else {
							break
						}
						await self.yield(element)
						state.withLock {
							if case .value = $0.last {
								$0.last = .value(element)
							}
						}
					}
					await self?.finish()
				} catch let error as Failure {
					await self?.finish(throwing: error)
				} catch {
					fatalError("Should never get here")
				}
			}
		}
	}

	deinit {
		self.pump.withLock { $0 }?.cancel()
	}
	
	/// Register the broadcast stream handler
	///
	/// Each subscriber gets an independent stream. Multiple subscribers can iterate
	/// concurrently without interfering with each other.
	///
	/// The channel is registered **synchronously** before this method returns,
	/// so no broadcasts can be missed due to a registration race.
	func register(_ sink: Sink, id: UUID) {
		var terminal: (finished: Bool, error: Failure?) = (false, nil)
		state.withLock { s in
			if case let .value(last) = s.last {
				sink.send(last)						// replay first
			}
			if s.isFinished {
				terminal = (true, s.terminalError)	// late to a finished broadcast
			} else {
				s.sinks[id] = sink					// then become visible
			}
		}
		if terminal.finished {
			sink.finish(terminal.error)
		}
	}
	
	func remove(_ id: UUID) {
		state.withLock {
			_ = $0.sinks.removeValue(forKey: id)
		}
	}
	
	/// Broadcast a value to all subscribers
	/// - Parameter element: The value to broadcast
	///
	/// This method implements per-consumer backpressure. If any consumer is slow,
	/// only that consumer's channel will apply backpressure. Fast consumers continue
	/// unaffected by slow consumers.
	func yield(_ element: Element) async {
		let sinkValues = state.withLock { s -> [Sink] in
			guard !s.isFinished else {
				return []
			}
			if case .value = s.last {
				s.last = .value(element)
			}
			return s.sinks.values.map({ $0 })
		}
		await withTaskGroup(of: Void.self) { group in
			for sink in sinkValues {
				group.addTask {
					sink.send(element)
				}
			}
		}
	}
	
	/// Finish all active channels and prevent new broadcasts
	func finish(throwing error: Failure? = nil) async {
		let sinks: [Sink] = state.withLock { s in
			guard !s.isFinished else {
				return []
			}
			s.isFinished = true
			s.terminalError = error
			let all = Array(s.sinks.values)
			s.sinks.removeAll()          // so a re-entrant remove() finds nothing
			return all
		}
		sinks.forEach {
			$0.finish(error)
		}
	}
	
	/// Finish all active channels and prevent new broadcasts
	func finish(throwing error: Failure? = nil) {
		let sinks: [Sink] = state.withLock { s in
			guard !s.isFinished else {
				return []
			}
			s.isFinished = true
			s.terminalError = error
			let all = Array(s.sinks.values)
			s.sinks.removeAll()          // so a re-entrant remove() finds nothing
			return all
		}
		sinks.forEach {
			$0.finish(error)
		}
	}
	
	/// Get the current number of active subscribers
	var subscriberCount: Int {
		state.withLock { s in
			s.sinks.count
		}
	}
}
