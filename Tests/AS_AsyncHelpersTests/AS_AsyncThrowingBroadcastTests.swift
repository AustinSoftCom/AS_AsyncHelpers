import XCTest
import Synchronization
@testable import AS_AsyncHelpers

enum TestError: Error, Equatable {
    case intentional
    case other
}

// MARK: - Single Subscriber Tests

final class AS_AsyncThrowingBroadcastSingleSubscriberTests: XCTestCase {
    func testReceivesAllBroadcastedValues() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.broadcast(1)
        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        var received: [Int] = []
        do {
            for try await value in subscription {
                received.append(value)
            }
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(received, [1, 2, 3])
    }

    func testFinishEndsStream() async {
        let stream = AS_AsyncThrowingBroadcast<String>()
        let subscription = stream.subscribe()

        await stream.broadcast("hello")
        await stream.finish()

        var received: [String] = []
        do {
            for try await value in subscription {
                received.append(value)
            }
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(received, ["hello"])
    }

    func testEmptyStreamFinishesImmediately() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.finish()

        var received: [Int] = []
        do {
            for try await value in subscription {
                received.append(value)
            }
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertTrue(received.isEmpty)
    }

    func testBroadcastAfterFinishIsIgnored() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.broadcast(1)
        await stream.finish()
        await stream.broadcast(2)

        var received: [Int] = []
        do {
            for try await value in subscription {
                received.append(value)
            }
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(received, [1])
    }
}

// MARK: - Error Propagation Tests

final class AS_AsyncThrowingBroadcastErrorTests: XCTestCase {
    func testFailPropagatesErrorToSubscriber() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.broadcast(1)
        await stream.fail(with: TestError.intentional)

        var received: [Int] = []
        var caughtError: (any Error)?
        do {
            for try await value in subscription {
                received.append(value)
            }
        } catch {
            caughtError = error
        }
        XCTAssertEqual(received, [1])
        XCTAssertEqual(caughtError as? TestError, TestError.intentional)
    }

    func testFailPropagatesErrorToAllSubscribers() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        await stream.broadcast(42)
        await stream.fail(with: TestError.intentional)

        var error1: (any Error)?
        var error2: (any Error)?

        var received1: [Int] = []
        do {
            for try await value in sub1 { received1.append(value) }
        } catch { error1 = error }

        var received2: [Int] = []
        do {
            for try await value in sub2 { received2.append(value) }
        } catch { error2 = error }

        XCTAssertEqual(received1, [42])
        XCTAssertEqual(received2, [42])
        XCTAssertEqual(error1 as? TestError, TestError.intentional)
        XCTAssertEqual(error2 as? TestError, TestError.intentional)
    }

    func testBroadcastAfterFailIsIgnored() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let subscription = stream.subscribe()

        await stream.fail(with: TestError.intentional)
        await stream.broadcast(1)

        var received: [Int] = []
        var caughtError: (any Error)?
        do {
            for try await value in subscription {
                received.append(value)
            }
        } catch {
            caughtError = error
        }
        XCTAssertTrue(received.isEmpty)
        XCTAssertEqual(caughtError as? TestError, TestError.intentional)
    }

    func testFailClearsSubscriberCount() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        async let drain1: Void = {
            do { for try await _ in sub1 {} } catch {}
        }()
        async let drain2: Void = {
            do { for try await _ in sub2 {} } catch {}
        }()

        try? await Task.sleep(for: .milliseconds(50))

        let countBefore = await stream.subscriberCount
        XCTAssertEqual(countBefore, 2)

        await stream.fail(with: TestError.intentional)

        let countAfter = await stream.subscriberCount
        XCTAssertEqual(countAfter, 0)

        await drain1
        await drain2
    }
}

// MARK: - Multiple Subscriber Tests

final class AS_AsyncThrowingBroadcastMultipleSubscriberTests: XCTestCase {
    func testTwoSubscribersReceiveSameValues() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        await stream.broadcast(10)
        await stream.broadcast(20)
        await stream.finish()

        var received1: [Int] = []
        do { for try await value in sub1 { received1.append(value) } } catch {}

        var received2: [Int] = []
        do { for try await value in sub2 { received2.append(value) } } catch {}

        XCTAssertEqual(received1, [10, 20])
        XCTAssertEqual(received2, [10, 20])
    }

    func testConcurrentSubscribersReceiveAllValues() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        let values = Array(1...5)

        async let collected1: [Int] = {
            var result: [Int] = []
            do { for try await value in sub1 { result.append(value) } } catch {}
            return result
        }()
        async let collected2: [Int] = {
            var result: [Int] = []
            do { for try await value in sub2 { result.append(value) } } catch {}
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

// MARK: - Cancellation Tests

final class AS_AsyncThrowingBroadcastCancellationTests: XCTestCase {
    func testCancelledSubscriberDoesNotAffectOthers() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        let task1 = Task<[Int], any Error> {
            var result: [Int] = []
            for try await value in sub1 { result.append(value) }
            return result
        }

        async let collected2: [Int] = {
            var result: [Int] = []
            do { for try await value in sub2 { result.append(value) } } catch {}
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

// MARK: - Sink Tests

final class AS_AsyncThrowingBroadcastSinkTests: XCTestCase {
    func testSinkReceivesAllValues() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let received = Mutex<[Int]>([])

        let token = stream.sink(onValue: { value in
            received.withLock { $0.append(value) }
        })

        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(1)
        await stream.broadcast(2)
        await stream.broadcast(3)
        await stream.finish()

        await token.value

        let result = received.withLock { $0 }
        XCTAssertEqual(result, [1, 2, 3])
    }

    func testSinkReceivesError() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let received = Mutex<[Int]>([])
        let caughtError = Mutex<(any Error)?>(nil)

        let token = stream.sink(
            onValue: { value in
                received.withLock { $0.append(value) }
            },
            onError: { error in
                caughtError.withLock { $0 = error }
            }
        )

        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(1)
        await stream.fail(with: TestError.intentional)

        await token.value

        let result = received.withLock { $0 }
        let error = caughtError.withLock { $0 }
        XCTAssertEqual(result, [1])
        XCTAssertEqual(error as? TestError, TestError.intentional)
    }

    func testSinkCancellation() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let received = Mutex<[Int]>([])

        let token = stream.sink(onValue: { value in
            received.withLock { $0.append(value) }
        })

        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(1)
        token.cancel()
        try? await Task.sleep(for: .milliseconds(50))

        await stream.broadcast(2)
        await stream.finish()

        let result = received.withLock { $0 }
        XCTAssertEqual(result, [1])
    }
}

// MARK: - Finish Behavior Tests

final class AS_AsyncThrowingBroadcastFinishBehaviorTests: XCTestCase {
    func testFinishClearsSubscriberCount() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub1 = stream.subscribe()
        let sub2 = stream.subscribe()

        async let drain1: Void = {
            do { for try await _ in sub1 {} } catch {}
        }()
        async let drain2: Void = {
            do { for try await _ in sub2 {} } catch {}
        }()

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
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub = stream.subscribe()

        await stream.broadcast(1)
        await stream.finish()
        await stream.finish()
        await stream.finish()

        var received: [Int] = []
        do { for try await value in sub { received.append(value) } } catch {}
        XCTAssertEqual(received, [1])
    }

    func testMultipleFailCallsAreSafe() async {
        let stream = AS_AsyncThrowingBroadcast<Int>()
        let sub = stream.subscribe()

        await stream.broadcast(1)
        await stream.fail(with: TestError.intentional)
        await stream.fail(with: TestError.other)

        var received: [Int] = []
        var caughtError: (any Error)?
        do {
            for try await value in sub { received.append(value) }
        } catch {
            caughtError = error
        }
        XCTAssertEqual(received, [1])
        XCTAssertEqual(caughtError as? TestError, TestError.intentional)
    }
}
