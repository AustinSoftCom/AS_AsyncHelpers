//  Copyright © 2026 Glenn L. Austin (AustinSoft.com)
//  Licensed under the MIT License. See LICENSE.txt for details.

import Testing
import Synchronization
@testable import AS_AsyncHelpers

// MARK: - Single Subscriber Tests

@Suite struct CurrentAsyncThrowingBroadcastSingleSubscriberTests {
	@Test func receivesAllBroadcastedValues() async throws {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		var received: [Int] = []
		for try await value in subscription {
			received.append(value)
		}
		#expect(received == [42, 1, 2, 3])
	}

	@Test func finishEndsStream() async throws {
		let stream = CurrentAsyncThrowingBroadcast<String, Error>(initialValue: "world")
		let subscription = stream.subscribe()

		await stream.yield("hello")
		await stream.finish()

		var received: [String] = []
		for try await value in subscription {
			received.append(value)
		}
		#expect(received == ["world", "hello"])
	}

	@Test func emptyStreamFinishesImmediately() async throws {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.finish()

		var received: [Int] = []
		for try await value in subscription {
			received.append(value)
		}
		#expect(received == [42])
	}

	@Test func broadcastAfterFinishIsIgnored() async throws {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.finish()
		await stream.yield(2)

		var received: [Int] = []
		for try await value in subscription {
			received.append(value)
		}
		#expect(received == [42, 1])
	}
}

// MARK: - Error Propagation Tests

@Suite struct CurrentAsyncThrowingBroadcastErrorTests {
	@Test func failPropagatesErrorToSubscriber() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.yield(1)
		await stream.finish(throwing: TestError.intentional)

		var received: [Int] = []
		var caughtError: (any Error)?
		do {
			for try await value in subscription {
				received.append(value)
			}
		} catch {
			caughtError = error
		}
		#expect(received == [42, 1])
		#expect(caughtError as? TestError == TestError.intentional)
	}

	@Test func failPropagatesErrorToAllSubscribers() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		await stream.yield(42)
		await stream.finish(throwing: TestError.intentional)

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

		#expect(received1 == [42, 42])
		#expect(received2 == [42, 42])
		#expect(error1 as? TestError == TestError.intentional)
		#expect(error2 as? TestError == TestError.intentional)
	}

	@Test func broadcastAfterFailIsIgnored() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let subscription = stream.subscribe()

		await stream.finish(throwing: TestError.intentional)
		await stream.yield(1)

		var received: [Int] = []
		var caughtError: (any Error)?
		do {
			for try await value in subscription {
				received.append(value)
			}
		} catch {
			caughtError = error
		}
		#expect(received == [42])
		#expect(caughtError as? TestError == TestError.intentional)
	}

	@Test func failClearsSubscriberCount() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
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
		#expect(countBefore == 2)

		await stream.finish(throwing: TestError.intentional)

		let countAfter = await stream.subscriberCount
		#expect(countAfter == 0)

		await drain1
		await drain2
	}
}

// MARK: - Multiple Subscriber Tests

@Suite struct CurrentAsyncThrowingBroadcastMultipleSubscriberTests {
	@Test func twoSubscribersReceiveSameValues() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let sub1 = stream.subscribe()
		let sub2 = stream.subscribe()

		await stream.yield(10)
		await stream.yield(20)
		await stream.finish()

		var received1: [Int] = []
		do { for try await value in sub1 { received1.append(value) } } catch {}

		var received2: [Int] = []
		do { for try await value in sub2 { received2.append(value) } } catch {}

		#expect(received1 == [42, 10, 20])
		#expect(received2 == [42, 10, 20])
	}

	@Test func concurrentSubscribersReceiveAllValues() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
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
			await stream.yield(value)
		}
		await stream.finish()

		let result1 = await collected1
		let result2 = await collected2

		#expect(result1 == [42] + values)
		#expect(result2 == [42] + values)
	}
}

// MARK: - Cancellation Tests

@Suite struct CurrentAsyncThrowingBroadcastCancellationTests {
	@Test func cancelledSubscriberDoesNotAffectOthers() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
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

// MARK: - Sink Tests

@Suite struct CurrentAsyncThrowingBroadcastSinkTests {
	@Test func sinkReceivesAllValues() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let received = Mutex<[Int]>([])

		let token = stream.sink(onValue: { value in
			received.withLock { $0.append(value) }
		})

		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(1)
		await stream.yield(2)
		await stream.yield(3)
		await stream.finish()

		await token.value

		let result = received.withLock { $0 }
		#expect(result == [42, 1, 2, 3])
	}

	@Test func sinkReceivesError() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
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

		await stream.yield(1)
		await stream.finish(throwing: TestError.intentional)

		await token.value

		let result = received.withLock { $0 }
		let error = caughtError.withLock { $0 }
		#expect(result == [42, 1])
		#expect(error as? TestError == TestError.intentional)
	}

	@Test func sinkCancellation() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let received = Mutex<[Int]>([])

		let token = stream.sink(onValue: { value in
			received.withLock { $0.append(value) }
		})

		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(1)
		token.cancel()
		try? await Task.sleep(for: .milliseconds(50))

		await stream.yield(2)
		await stream.finish()

		let result = received.withLock { $0 }
		#expect(result == [42, 1])
	}
}

// MARK: - Finish Behavior Tests

@Suite struct CurrentAsyncThrowingBroadcastFinishBehaviorTests {
	@Test func finishClearsSubscriberCount() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
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
		#expect(countBefore == 2)

		await stream.finish()

		let countAfter = await stream.subscriberCount
		#expect(countAfter == 0)

		await drain1
		await drain2
	}

	@Test func multipleFinishCallsAreSafe() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let sub = stream.subscribe()

		await stream.yield(1)
		await stream.finish()
		await stream.finish()
		await stream.finish()

		var received: [Int] = []
		do { for try await value in sub { received.append(value) } } catch {}
		#expect(received == [42, 1])
	}

	@Test func multipleFailCallsAreSafe() async {
		let stream = CurrentAsyncThrowingBroadcast<Int, Error>(initialValue: 42)
		let sub = stream.subscribe()

		await stream.yield(1)
		await stream.finish(throwing: TestError.intentional)
		await stream.finish(throwing: TestError.other)

		var received: [Int] = []
		var caughtError: (any Error)?
		do {
			for try await value in sub { received.append(value) }
		} catch {
			caughtError = error
		}
		#expect(received == [42, 1])
		#expect(caughtError as? TestError == TestError.intentional)
	}
}
