// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "PulsepondDogfood",
    platforms: [.iOS(.v13)],
    products: [],
    targets: [
        .binaryTarget(
            name: "Pulsepond",
            path: "Pulsepond.xcframework"
        ),
        .testTarget(
            name: "PulsepondDogfoodTests",
            dependencies: ["Pulsepond"],
            resources: [.process("Fixtures")]
        ),
    ]
)
