//  Copyright © 2026 Glenn L. Austin (AustinSoft.com)
//  Licensed under the MIT License. See LICENSE.txt for details.

import Foundation
import Testing
@testable import AS_AsyncHelpers

/// `yield(_:)` and `finish()` are synchronous and nonisolated (2.3.0), so producers that aren't
/// async (delegate callbacks, an actor's synchronous methods) call them directly. Wrapping an
/// async `yield` in a `Task` per value lost ordering, because separate Tasks can run in any
/// order; `yield` delivers every value to every subscriber before it returns.
@Suite struct SyncYieldTests {
	/// Yields `values` from synchronous code, as a delegate callback would.
	private static func produce(_ values: Range<Int>, into yield: (Int) -> Void, then finish: () -> Void) {
		for value in values {
			yield(value)
		}
		finish()
	}

	@Test func syncYieldsArriveInOrderForEverySubscriber() async {
		let stream = AsyncBroadcast<Int>()
		let first = stream.subscribe(bufferSize: 10_000)
		let second = stream.subscribe(bufferSize: 10_000)

		Self.produce(0..<10_000, into: { stream.yield($0) }, then: { stream.finish() })

		var a: [Int] = [], b: [Int] = []
		for await value in first { a.append(value) }
		for await value in second { b.append(value) }
		#expect(a == Array(0..<10_000))
		#expect(b == Array(0..<10_000))
	}

	@Test func syncYieldsFromAnotherThreadArriveInOrder() async {
		let stream = AsyncBroadcast<Int>()
		let subscription = stream.subscribe(bufferSize: 1_000)

		// A delegate queue, as CoreLocation or CoreMotion would call back on.
		let queue = DispatchQueue(label: "SyncYieldTests.delegate")
		await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
			queue.async {
				for value in 0..<1_000 { stream.yield(value) }
				stream.finish()
				done.resume()
			}
		}

		var received: [Int] = []
		for await value in subscription { received.append(value) }
		#expect(received == Array(0..<1_000))
	}

	@Test func yieldsFromAsyncCodeArriveInCallOrder() async {
		let stream = AsyncBroadcast<Int>()
		let subscription = stream.subscribe()

		stream.yield(1)
		stream.yield(2)
		stream.yield(3)
		stream.finish()

		var received: [Int] = []
		for await value in subscription { received.append(value) }
		#expect(received == [1, 2, 3])
	}

	@Test func syncYieldAfterSyncFinishIsIgnored() async {
		let stream = AsyncBroadcast<Int>()
		let subscription = stream.subscribe()

		stream.yield(1)
		stream.finish()
		stream.yield(2)

		var received: [Int] = []
		for await value in subscription { received.append(value) }
		#expect(received == [1])
	}

	@Test func currentValueFollowsSyncYields() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 0)
		stream.yield(1)
		stream.yield(2)
		#expect(await stream.value == 2)

		// A late subscriber gets the latest value first, then what follows, in order.
		let late = stream.subscribe()
		stream.yield(3)
		stream.finish()
		var received: [Int] = []
		for await value in late { received.append(value) }
		#expect(received == [2, 3])
	}

	@Test func throwingSyncYieldsThenErrorArriveInOrder() async {
		let stream = AsyncThrowingBroadcast<Int, any Error>()
		let subscription = stream.subscribe()

		stream.yield(1)
		stream.yield(2)
		stream.finish(throwing: TestError.intentional)

		var received: [Int] = []
		var thrown: (any Error)?
		do {
			for try await value in subscription { received.append(value) }
		} catch {
			thrown = error
		}
		#expect(received == [1, 2])
		#expect(thrown is TestError)
	}

	@Test func currentThrowingFollowsSyncYields() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, any Error>(initialValue: 0)
		stream.yield(5)
		#expect(await stream.value == 5)
		let subscription = stream.subscribe()
		stream.yield(6)
		stream.finish()
		var received: [Int] = []
		do {
			for try await value in subscription { received.append(value) }
		} catch {
			Issue.record("Unexpected error \(error)")
		}
		#expect(received == [5, 6])
	}
}
