//  Copyright © 2026 AustinSoft.com. All rights reserved worldwide.
//  Created by Glenn L. Austin on 6/20/26

import AsyncAlgorithms
import Foundation
import Synchronization

/// Lock-protected dictionary for channel registration.
/// Allows `subscribe()` to register channels synchronously from any thread
/// while `broadcast()` reads the channel list from the actor.
final class ChannelStorage<Element: Sendable>: Sendable {
	private let channels: Mutex<[UUID: AsyncChannel<Element>]> = .init([:])

	func insert(_ channel: AsyncChannel<Element>, id: UUID) {
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

	func allChannels() -> [AsyncChannel<Element>] {
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
