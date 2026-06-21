//  Copyright © 2026 AustinSoft.com. All rights reserved worldwide.
//  Created by Glenn L. Austin on 6/20/26

import Foundation
import AsyncAlgorithms
import Synchronization

/// Lock-protected dictionary for throwing channel registration.
/// Mirrors `ChannelStorage` but wraps `AsyncThrowingChannel` instead.
final class ThrowingChannelStorage<Element: Sendable>: Sendable {
	private let channels: Mutex<[UUID: AsyncThrowingChannel<Element, any Error>]> = .init([:])

	func insert(_ channel: AsyncThrowingChannel<Element, any Error>, id: UUID) {
		channels.withLock {
			$0[id] = channel
		}
	}

	func remove(_ id: UUID) {
		let channel = channels.withLock {
			$0.removeValue(forKey: id)
		}
		channel?.finish()
	}

	func allChannels() -> [AsyncThrowingChannel<Element, any Error>] {
		channels.withLock {
			Array($0.values)
		}
	}

	func removeAll() {
		channels.withLock {
			$0.removeAll()
		}
	}

	var count: Int {
		channels.withLock {
			$0.count
		}
	}
}
