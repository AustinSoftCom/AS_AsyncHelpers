import XCTest
import Synchronization
@testable import AS_AsyncHelpers

final class AS_AsyncBroadcastSingleSubscriberTests: XCTestCase {
    func testReceivesAllBroadcastedValues() async {
        let stream = AS_AsyncBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.broadcast(1)
        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        var received: [Int] = []
        for await value in subscription {
            received.append(value)
        }
        XCTAssertEqual(received, [1, 2, 3])
    }

    func testFinishEndsStream() async {
        let stream = AS_AsyncBroadcast<String>()
        let subscription = stream.subscribe()

        await stream.broadcast("hello")
        await stream.finish()

        var received: [String] = []
        for await value in subscription {
            received.append(value)
        }
        XCTAssertEqual(received, ["hello"])
    }

    func testEmptyStreamFinishesImmediately() async {
        let stream = AS_AsyncBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.finish()

        var received: [Int] = []
        for await value in subscription {
            received.append(value)
        }
        XCTAssertTrue(received.isEmpty)
    }

    func testBroadcastAfterFinishIsIgnored() async {
        let stream = AS_AsyncBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.broadcast(1)
        await stream.finish()
        await stream.broadcast(2)

        var received: [Int] = []
        for await value in subscription {
            received.append(value)
        }
        XCTAssertEqual(received, [1])
    }
}

final class AS_AsyncBroadcastMultipleSubscriberTests: XCTestCase {
    func testTwoSubscribersReceiveSameValues() async {
        let stream = AS_AsyncBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        await stream.broadcast(10)
        await stream.broadcast(20)
        await stream.finish()

        var received1: [Int] = []
        for await value in sub1 { received1.append(value) }

        var received2: [Int] = []
        for await value in sub2 { received2.append(value) }

        XCTAssertEqual(received1, [10, 20])
        XCTAssertEqual(received2, [10, 20])
    }

    func testThreeSubscribersReceiveSameValues() async {
        let stream = AS_AsyncBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()
        let sub3 = stream.subscribe()

        await stream.broadcast(1)
        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        var results: [[Int]] = [[], [], []]
        for await value in sub1 { results[0].append(value) }
        for await value in sub2 { results[1].append(value) }
        for await value in sub3 { results[2].append(value) }

        for result in results {
            XCTAssertEqual(result, [1, 2, 3])
        }
    }

    func testSubscriberCountTracksActiveSubscriptions() async {
        let stream = AS_AsyncBroadcast<Int>()

        let count0 = await stream.subscriberCount
        XCTAssertEqual(count0, 0)

        // Hold references so subscriptions stay alive
        let sub1 = stream.subscribe()
        let count1 = await stream.subscriberCount
        XCTAssertEqual(count1, 1)

        let sub2 = stream.subscribe()
        let count2 = await stream.subscriberCount
        XCTAssertEqual(count2, 2)

        // Consume to avoid warnings
        await stream.finish()
        for await _ in sub1 {}
        for await _ in sub2 {}
    }

    func testConcurrentSubscribersReceiveAllValues() async {
        let stream = AS_AsyncBroadcast<Int>()
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
            await stream.broadcast(value)
        }
        await stream.finish()

        let result1 = await collected1
        let result2 = await collected2

        XCTAssertEqual(result1, values)
        XCTAssertEqual(result2, values)
    }
}

final class AS_AsyncBroadcastLateSubscriberTests: XCTestCase {
    func testLateSubscriberOnlyReceivesSubsequentValues() async {
        let stream = AS_AsyncBroadcast<Int>()
        let earlySub = stream.subscribe()

        await stream.broadcast(1)

        let lateSub = stream.subscribe()

        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        var earlyReceived: [Int] = []
        for await value in earlySub { earlyReceived.append(value) }

        var lateReceived: [Int] = []
        for await value in lateSub { lateReceived.append(value) }

        XCTAssertEqual(earlyReceived, [1, 2, 3])
        XCTAssertEqual(lateReceived, [2, 3])
    }

    func testSubscribingAfterFinishReceivesNothing() async {
        let stream = AS_AsyncBroadcast<Int>()
        let earlySub = stream.subscribe()

        await stream.broadcast(1)
        await stream.finish()

        var earlyReceived: [Int] = []
        for await value in earlySub { earlyReceived.append(value) }
        XCTAssertEqual(earlyReceived, [1])

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
        XCTAssertTrue(lateReceived.isEmpty)
    }
}

final class AS_AsyncBroadcastCancellationTests: XCTestCase {
    func testCancelledSubscriberDoesNotAffectOthers() async {
        let stream = AS_AsyncBroadcast<Int>()
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

        await stream.broadcast(1)

        task1.cancel()
        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        let result2 = await collected2
        XCTAssertEqual(result2, [1, 2, 3])
    }
}

final class AS_AsyncBroadcastTypeTests: XCTestCase {
    func testWorksWithStrings() async {
        let stream = AS_AsyncBroadcast<String>()
        let sub = stream.subscribe()

        await stream.broadcast("hello")
        await stream.broadcast("world")
        await stream.finish()

        var received: [String] = []
        for await value in sub { received.append(value) }
        XCTAssertEqual(received, ["hello", "world"])
    }

    func testWorksWithStructs() async {
        struct Point: Sendable, Equatable {
            let x: Int
            let y: Int
        }

        let stream = AS_AsyncBroadcast<Point>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        await stream.broadcast(Point(x: 1, y: 2))
        await stream.broadcast(Point(x: 3, y: 4))
        await stream.finish()

        var received1: [Point] = []
        for await value in sub1 { received1.append(value) }

        var received2: [Point] = []
        for await value in sub2 { received2.append(value) }

        let expected = [Point(x: 1, y: 2), Point(x: 3, y: 4)]
        XCTAssertEqual(received1, expected)
        XCTAssertEqual(received2, expected)
    }

    func testWorksWithOptionals() async {
        let stream = AS_AsyncBroadcast<Int?>()
        let sub = stream.subscribe()

        await stream.broadcast(1)
        await stream.broadcast(nil)
        await stream.broadcast(3)
        await stream.finish()

        var received: [Int?] = []
        for await value in sub { received.append(value) }
        XCTAssertEqual(received.count, 3)
        XCTAssertEqual(received[0], 1)
        XCTAssertNil(received[1])
        XCTAssertEqual(received[2], 3)
    }
}

final class AS_AsyncBroadcastSinkTests: XCTestCase {
    func testSinkReceivesAllValues() async {
        let stream = AS_AsyncBroadcast<Int>()
        let received = Mutex<[Int]>([])

        let token = stream.sink { value in
            received.withLock { $0.append(value) }
        }

        // Give the sink task time to start iterating
        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(1)
        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        // Wait for the sink task to complete
        await token.value

        let result = received.withLock { $0 }
        XCTAssertEqual(result, [1, 2, 3])
    }

    func testSinkCancellation() async {
        let stream = AS_AsyncBroadcast<Int>()
        let received = Mutex<[Int]>([])

        let token = stream.sink { value in
            received.withLock { $0.append(value) }
        }

        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(1)
        token.cancel()
        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(2)
        await stream.finish()

        let result = received.withLock { $0 }
        XCTAssertEqual(result, [1])
    }

    func testMultipleSinksReceiveSameValues() async {
        let stream = AS_AsyncBroadcast<Int>()
        let received1 = Mutex<[Int]>([])
        let received2 = Mutex<[Int]>([])

        let token1 = stream.sink { value in
            received1.withLock { $0.append(value) }
        }
        let token2 = stream.sink { value in
            received2.withLock { $0.append(value) }
        }

        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(10)
        await stream.broadcast(20)
        await stream.finish()

        await token1.value
        await token2.value

        let result1 = received1.withLock { $0 }
        let result2 = received2.withLock { $0 }
        XCTAssertEqual(result1, [10, 20])
        XCTAssertEqual(result2, [10, 20])
    }
}

final class AS_AsyncBroadcastFinishBehaviorTests: XCTestCase {
    func testFinishClearsSubscriberCount() async {
        let stream = AS_AsyncBroadcast<Int>()

        // Start consuming in background tasks to keep subscriptions alive
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        async let drain1: Void = { for await _ in sub1 {} }()
        async let drain2: Void = { for await _ in sub2 {} }()

        // Give the iteration tasks a moment to start
        try? await Task.sleep(for: .milliseconds(50))

        let countBefore = await stream.subscriberCount
        XCTAssertEqual(countBefore, 2)

        await stream.finish()

        let countAfter = await stream.subscriberCount
        XCTAssertEqual(countAfter, 0)

        await drain1
        await drain2
    }

    func testMultipleFinishCallsAreSafe() async {
        let stream = AS_AsyncBroadcast<Int>()
        let sub = stream.subscribe()

        await stream.broadcast(1)
        await stream.finish()
        await stream.finish()
        await stream.finish()

        var received: [Int] = []
        for await value in sub { received.append(value) }
        XCTAssertEqual(received, [1])
    }
}
