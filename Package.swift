// swift-tools-version: 6.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "AS_AsyncHelpers",
	platforms: [
		.macOS(.v15),
		.iOS(.v18),
		.tvOS(.v18),
		.watchOS(.v11),
		.visionOS(.v2),
	],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "AS_AsyncHelpers",
            targets: ["AS_AsyncHelpers"]
        ),
    ],
	dependencies: [
		.package(url: "https://github.com/apple/swift-async-algorithms.git", from: "1.0.4")
	],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "AS_AsyncHelpers",
			dependencies: [
				.product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
			]
        ),
        .testTarget(
            name: "AS_AsyncHelpersTests",
            dependencies: ["AS_AsyncHelpers"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
