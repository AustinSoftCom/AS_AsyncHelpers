//  Copyright © 2026 Glenn L. Austin (AustinSoft.com)
//  Licensed under the MIT License. See LICENSE.txt for details.

import Testing
import Synchronization
@testable import AS_AsyncHelpers

@Suite struct CurrentAsyncBroadcastSingleSubscriberTests {
	@Test func receivesAllBroadcastedValues() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		var received: [Int] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == [42, 1, 2, 3])
	}

	@Test func finishEndsStream() async {
		let stream = CurrentAsyncBroadcast<String>(initialValue: "world")
		let subscription = stream.subscribe()

		await stream.yield("hello")
		await stream.finish()

		var received: [String] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == ["world", "hello"])
	}

	@Test func emptyStreamFinishesImmediately() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.finish()

		var received: [Int] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == [42])
	}

	@Test func broadcastAfterFinishIsIgnored() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.finish()
		await stream.yield(2)

		var received: [Int] = []
		for await value in subscription {
			received.append(value)
		}
		#expect(received == [42, 1])
	}
}

@Suite struct CurrentAsyncBroadcastMultipleSubscriberTests {
	@Test func twoSubscribersReceiveSameValues() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		await stream.yield(10)
		await stream.yield(20)
		await stream.finish()

		var received1: [Int] = []
		for await value in sub1 { received1.append(value) }

		var received2: [Int] = []
		for await value in sub2 { received2.append(value) }

		#expect(received1 == [42, 10, 20])
		#expect(received2 == [42, 10, 20])
	}

	@Test func threeSubscribersReceiveSameValues() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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
			#expect(result == [42, 1, 2, 3])
		}
	}

	@Test func subscriberCountTracksActiveSubscriptions() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)

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
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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

		#expect(result1 == [42] + values)
		#expect(result2 == [42] + values)
	}
}

@Suite struct CurrentAsyncBroadcastLateSubscriberTests {
	@Test func lateSubscriberOnlyReceivesSubsequentValues() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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

		#expect(earlyReceived == [42, 1, 2, 3])
		#expect(lateReceived == [1, 2, 3])
	}

	@Test func subscribingAfterFinishReceivesNothing() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
		let earlySub = stream.subscribe()

		await stream.yield(1)
		await stream.finish()

		var earlyReceived: [Int] = []
		for await value in earlySub { earlyReceived.append(value) }
		#expect(earlyReceived == [42, 1])

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
		#expect(lateReceived == [1])
	}
}

@Suite struct CurrentAsyncBroadcastCancellationTests {
	@Test func cancelledSubscriberDoesNotAffectOthers() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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
		#expect(result2 == [42, 1, 2, 3])
	}
}

@Suite struct CurrentAsyncBroadcastTypeTests {
	@Test func worksWithStrings() async {
		let stream = CurrentAsyncBroadcast<String>(initialValue: "Boo!")
		let sub = stream.subscribe()

		await stream.yield("hello")
		await stream.yield("world")
		await stream.finish()

		var received: [String] = []
		for await value in sub { received.append(value) }
		#expect(received == ["Boo!", "hello", "world"])
	}

	@Test func worksWithStructs() async {
		struct Point: Sendable, Equatable {
			let x: Int
			let y: Int
		}

		let stream = CurrentAsyncBroadcast<Point>(initialValue: .init(x: 0, y: 0))
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		await stream.yield(Point(x: 1, y: 2))
		await stream.yield(Point(x: 3, y: 4))
		await stream.finish()

		var received1: [Point] = []
		for await value in sub1 { received1.append(value) }

		var received2: [Point] = []
		for await value in sub2 { received2.append(value) }

		let expected = [Point(x: 0, y: 0), Point(x: 1, y: 2), Point(x: 3, y: 4)]
		#expect(received1 == expected)
		#expect(received2 == expected)
	}

	@Test func worksWithOptionals() async {
		let stream = CurrentAsyncBroadcast<Int?>(initialValue: nil)
		let sub = stream.subscribe()

		await stream.yield(1)
		await stream.yield(nil)
		await stream.yield(3)
		await stream.finish()

		var received: [Int?] = []
		for await value in sub { received.append(value) }
		#expect(received.count == 4)
		#expect(received[0] == nil)
		#expect(received[1] == 1)
		#expect(received[2] == nil)
		#expect(received[3] == 3)
	}
}

@Suite struct CurrentAsyncBroadcastSinkTests {
	@Test func sinkReceivesAllValues() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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
		#expect(result == [42, 1, 2, 3])
	}

	@Test func sinkCancellation() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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
		#expect(result == [42, 1])
	}

	@Test func multipleSinksReceiveSameValues() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
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
		#expect(result1 == [42, 10, 20])
		#expect(result2 == [42, 10, 20])
	}
}

@Suite struct CurrentAsyncBroadcastFinishBehaviorTests {
	@Test func finishClearsSubscriberCount() async {
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)

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
		let stream = CurrentAsyncBroadcast<Int>(initialValue: 42)
		let sub = stream.subscribe()

		await stream.yield(1)
		await stream.finish()
		await stream.finish()
		await stream.finish()

		var received: [Int] = []
		for await value in sub { received.append(value) }
		#expect(received == [42, 1])
	}
}
