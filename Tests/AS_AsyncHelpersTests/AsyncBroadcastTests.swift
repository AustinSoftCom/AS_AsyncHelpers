//  Copyright © 2026 Glenn L. Austin (AustinSoft.com)
//  Licensed under the MIT License. See LICENSE.txt for details.

import Testing
import Synchronization
@testable import AS_AsyncHelpers

@Suite struct AsyncBroadcastSingleSubscriberTests {
	@Test func receivesAllBroadcastedValues() async {
		let stream = AsyncBroadcast<Int>()
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		var received: [Int] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == [1, 2, 3])
	}

	@Test func finishEndsStream() async {
		let stream = AsyncBroadcast<String>()
		let subscription = stream.subscribe()

		await stream.yield("hello")
		await stream.finish()

		var received: [String] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == ["hello"])
	}

	@Test func emptyStreamFinishesImmediately() async {
		let stream = AsyncBroadcast<Int>()
		let subscription = stream.subscribe()

		await stream.finish()

		var received: [Int] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received.isEmpty)
	}

	@Test func broadcastAfterFinishIsIgnored() async {
		let stream = AsyncBroadcast<Int>()
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.finish()
		await stream.yield(2)

		var received: [Int] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == [1])
	}
}

@Suite struct AsyncBroadcastMultipleSubscriberTests {
	@Test func twoSubscribersReceiveSameValues() async {
		let stream = AsyncBroadcast<Int>()
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		await stream.yield(10)
		await stream.yield(20)
		await stream.finish()

		var received1: [Int] = []
		for await value in sub1 { received1.append(value) }

		var received2: [Int] = []
		for await value in sub2 { received2.append(value) }

		#expect(received1 == [10, 20])
		#expect(received2 == [10, 20])
	}

	@Test func threeSubscribersReceiveSameValues() async {
		let stream = AsyncBroadcast<Int>()
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()
		let sub3 = stream.subscribe()

		await stream.yield(1)
		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		var results: [[Int]] = [[], [], []]
		for await value in sub1 { results[0].append(value) }
		for await value in sub2 { results[1].append(value) }
		for await value in sub3 { results[2].append(value) }

		for result in results {
			#expect(result == [1, 2, 3])
		}
	}

	@Test func subscriberCountTracksActiveSubscriptions() async {
		let stream = AsyncBroadcast<Int>()

		let count0 = await stream.subscriberCount
		#expect(count0 == 0)

		// Hold references so subscriptions stay alive
		let sub1 = stream.subscribe()
		let count1 = await stream.subscriberCount
		#expect(count1 == 1)

		let sub2 = stream.subscribe()
		let count2 = await stream.subscriberCount
		#expect(count2 == 2)

		// Consume to avoid warnings
		await stream.finish()
		for await _ in sub1 {}
		for await _ in sub2 {}
	}

	@Test func concurrentSubscribersReceiveAllValues() async {
		let stream = AsyncBroadcast<Int>()
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		let values = Array(1...5)

		async let collected1: [Int] = {
			var result: [Int] = []
			for await value in sub1 { result.append(value) }
			return result
		}()
		async let collected2: [Int] = {
			var result: [Int] = []
			for await value in sub2 { result.append(value) }
			return result
		}()

		for value in values {
			await stream.yield(value)
		}
		await stream.finish()

		let result1 = await collected1
		let result2 = await collected2

		#expect(result1 == values)
		#expect(result2 == values)
	}
}

@Suite struct AsyncBroadcastLateSubscriberTests {
	@Test func lateSubscriberOnlyReceivesSubsequentValues() async {
		let stream = AsyncBroadcast<Int>()
		let earlySub = stream.subscribe()

		await stream.yield(1)

		let lateSub = stream.subscribe()

		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		var earlyReceived: [Int] = []
		for await value in earlySub { earlyReceived.append(value) }

		var lateReceived: [Int] = []
		for await value in lateSub { lateReceived.append(value) }

		#expect(earlyReceived == [1, 2, 3])
		#expect(lateReceived == [2, 3])
	}

	@Test func subscribingAfterFinishReceivesNothing() async {
		let stream = AsyncBroadcast<Int>()
		let earlySub = stream.subscribe()

		await stream.yield(1)
		await stream.finish()

		var earlyReceived: [Int] = []
		for await value in earlySub { earlyReceived.append(value) }
		#expect(earlyReceived == [1])

		// Late subscriber after finish - channel won't receive broadcasts
		let lateSub = stream.subscribe()
		let task = Task<[Int], Never> {
			var result: [Int] = []
			for await value in lateSub { result.append(value) }
			return result
		}
		try? await Task.sleep(for: .milliseconds(100))
		task.cancel()
		let lateReceived = await task.value
		#expect(lateReceived.isEmpty)
	}
}

@Suite struct AsyncBroadcastCancellationTests {
	@Test func cancelledSubscriberDoesNotAffectOthers() async {
		let stream = AsyncBroadcast<Int>()
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		let task1 = Task<[Int], Never> {
			var result: [Int] = []
			for await value in sub1 { result.append(value) }
			return result
		}

		async let collected2: [Int] = {
			var result: [Int] = []
			for await value in sub2 { result.append(value) }
			return result
		}()

		await stream.yield(1)

		task1.cancel()
		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		let result2 = await collected2
		#expect(result2 == [1, 2, 3])
	}
}

@Suite struct AsyncBroadcastTypeTests {
	@Test func worksWithStrings() async {
		let stream = AsyncBroadcast<String>()
		let sub = stream.subscribe()

		await stream.yield("hello")
		await stream.yield("world")
		await stream.finish()

		var received: [String] = []
		for await value in sub { received.append(value) }
		#expect(received == ["hello", "world"])
	}

	@Test func worksWithStructs() async {
		struct Point: Sendable, Equatable {
			let x: Int
			let y: Int
		}

		let stream = AsyncBroadcast<Point>()
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		await stream.yield(Point(x: 1, y: 2))
		await stream.yield(Point(x: 3, y: 4))
		await stream.finish()

		var received1: [Point] = []
		for await value in sub1 { received1.append(value) }

		var received2: [Point] = []
		for await value in sub2 { received2.append(value) }

		let expected = [Point(x: 1, y: 2), Point(x: 3, y: 4)]
		#expect(received1 == expected)
		#expect(received2 == expected)
	}

	@Test func worksWithOptionals() async {
		let stream = AsyncBroadcast<Int?>()
		let sub = stream.subscribe()

		await stream.yield(1)
		await stream.yield(nil)
		await stream.yield(3)
		await stream.finish()

		var received: [Int?] = []
		for await value in sub { received.append(value) }
		#expect(received.count == 3)
		#expect(received[0] == 1)
		#expect(received[1] == nil)
		#expect(received[2] == 3)
	}
}

@Suite struct AsyncBroadcastSinkTests {
	@Test func sinkReceivesAllValues() async {
		let stream = AsyncBroadcast<Int>()
		let received = Mutex<[Int]>([])

		let token = stream.sink { value in
			received.withLock { $0.append(value) }
		}

		// Give the sink task time to start iterating
		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(1)
		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		// Wait for the sink task to complete
		await token.value

		let result = received.withLock { $0 }
		#expect(result == [1, 2, 3])
	}

	@Test func sinkCancellation() async {
		let stream = AsyncBroadcast<Int>()
		let received = Mutex<[Int]>([])

		let token = stream.sink { value in
			received.withLock { $0.append(value) }
		}

		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(1)
		token.cancel()
		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(2)
		await stream.finish()

		let result = received.withLock { $0 }
		#expect(result == [1])
	}

	@Test func multipleSinksReceiveSameValues() async {
		let stream = AsyncBroadcast<Int>()
		let received1 = Mutex<[Int]>([])
		let received2 = Mutex<[Int]>([])

		let token1 = stream.sink { value in
			received1.withLock { $0.append(value) }
		}
		let token2 = stream.sink { value in
			received2.withLock { $0.append(value) }
		}

		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(10)
		await stream.yield(20)
		await stream.finish()

		await token1.value
		await token2.value

		let result1 = received1.withLock { $0 }
		let result2 = received2.withLock { $0 }
		#expect(result1 == [10, 20])
		#expect(result2 == [10, 20])
	}
}

@Suite struct AsyncBroadcastFinishBehaviorTests {
	@Test func finishClearsSubscriberCount() async {
		let stream = AsyncBroadcast<Int>()

		// Start consuming in background tasks to keep subscriptions alive
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		async let drain1: Void = { for await _ in sub1 {} }()
		async let drain2: Void = { for await _ in sub2 {} }()

		// Give the iteration tasks a moment to start
		try? await Task.sleep(for: .milliseconds(50))

		let countBefore = await stream.subscriberCount
		#expect(countBefore == 2)

		await stream.finish()

		let countAfter = await stream.subscriberCount
		#expect(countAfter == 0)

		await drain1
		await drain2
	}

	@Test func multipleFinishCallsAreSafe() async {
		let stream = AsyncBroadcast<Int>()
		let sub = stream.subscribe()

		await stream.yield(1)
		await stream.finish()
		await stream.finish()
		await stream.finish()

		var received: [Int] = []
		for await value in sub { received.append(value) }
		#expect(received == [1])
	}

	@Test func receivingStream() async {
		let (stream, continuation) = AsyncStream<Int>.makeStream()
		let broadcast = AsyncBroadcast(stream: stream)
		let probe = broadcast.subscribe()
		var probeIter = probe.makeAsyncIterator()

		let a = broadcast.subscribe()
		let taskA = Task { await a.reduce(into: [Int]()) { $0.append($1) } }

		continuation.yield(1)
		let probeIterValue = await probeIter.next()
		#expect(probeIterValue == 1)     // ← pump has now run

		let b = broadcast.subscribe()                 // provably after 1 was broadcast
		let taskB = Task { await b.reduce(into: [Int]()) { $0.append($1) } }

		continuation.yield(2)
		continuation.finish()

		let aValues = await taskA.value
		let bValues = await taskB.value

		#expect(aValues == [1, 2])
		#expect(bValues == [2])
	}
}
