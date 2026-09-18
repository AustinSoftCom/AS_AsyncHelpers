# Contributing to AS_AsyncHelpers

Thanks for your interest in contributing! AS_AsyncHelpers is a small, focused package, and contributions of all kinds are welcome — bug reports, documentation improvements, tests, and code.

## Reporting issues

Please open an issue at
[https://github.com/AustinSoftCom/AS_AsyncHelpers/issues](https://github.com/AustinSoftCom/AS_AsyncHelpers/issues) and include:

- What you expected to happen and what actually happened.
- A minimal code sample that reproduces the problem, if possible.
- The Swift toolchain version and platform (macOS/iOS/tvOS/watchOS/visionOS and OS version).

For questions about usage rather than bugs, an issue is fine too — questions often reveal gaps in the documentation.

## Submitting changes

1. Fork the repository and create a branch from `develop` (this project follows a git-flow-style layout: `develop` for ongoing work, `main` for releases).
2. Make your changes.
3. Add tests demonstrating your fixes.
4. Run the tests (`swift test`) and make sure they all pass.
5. Open a pull request against `develop` with a clear description of what the change does and why.

Please keep pull requests focused — one bug fix or feature per PR is much easier to review than a batch of unrelated changes.

## Code guidelines

- **Swift 6 strict concurrency:** the package builds in Swift 6 language mode with no concurrency warnings. Changes must preserve that — public types are actors or `Sendable`, and shared state is protected (the package uses `Mutex` from the `Synchronization` module internally).
- **No new dependencies** without prior discussion in an issue. The only runtime dependency is [swift-log](https://github.com/apple/swift-log).
- **Public API must be documented** with SwiftDoc (`///`) comments, including parameters and return values. Follow the style of the existing sources.
- **Formatting:** match the existing code — tabs for indentation in Swift sources, PascalCase types, camelCase members.
- **Behavior guarantees matter:** the README documents specific semantics (synchronous subscription registration, per-consumer buffering, current-value replay, late-subscriber termination). A change that alters any of these needs a very good reason, discussion in an issue first, and updated documentation.

## Tests

- Tests use the [Swift Testing](https://developer.apple.com/documentation/testing/) framework (`@Suite`, `@Test`, `#expect`), not XCTest, so the suite runs with `swift test` on any platform — no Xcode required.
- New features and bug fixes should come with tests. For bug fixes, a test that fails before the fix and passes after it is ideal.
- Concurrency tests should be deterministic — avoid sleeps and timing assumptions where possible; prefer confirmation-style patterns and explicit synchronization.

## License

By contributing, you agree that your contributions will be licensed under the same [MIT License](LICENSE.txt) that covers the project.
