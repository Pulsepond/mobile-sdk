import Foundation
import Pulsepond
import XCTest

private struct DogfoodConfiguration: Decodable {
    let endpoint: String
    let writeKey: String
    let deploymentId: String
    let projectId: String
    let sourceId: String
    let eventName: String
    let artifactVersion: String
}

private final class DiagnosticSink: NSObject, PulsepondDiagnosticListener {
    private let lock = NSLock()
    private var diagnostics: [PulsepondDiagnostic] = []

    func onDiagnostic(diagnostic: PulsepondDiagnostic) {
        lock.lock()
        diagnostics.append(diagnostic)
        lock.unlock()
    }

    func wireNames() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return diagnostics.map(\.code.wireName)
    }
}

final class PulsepondDogfoodTests: XCTestCase {
    func testPublishedSDKDelivers() async throws {
        let configURL = try XCTUnwrap(Bundle.module.url(forResource: "config", withExtension: "json"))
        let config = try JSONDecoder().decode(
            DogfoodConfiguration.self,
            from: Data(contentsOf: configURL)
        )
        let diagnostics = DiagnosticSink()
        let client = try await PulsepondApple.shared.create(
            configuration: try PulsepondConfiguration(
                endpoint: config.endpoint,
                writeKey: config.writeKey,
                deploymentId: config.deploymentId,
                projectId: config.projectId,
                sourceId: config.sourceId,
                environment: "production",
                appVersion: "\(config.artifactVersion)-dogfood",
                release: "mobile-sdk@\(config.artifactVersion)",
                batchSize: 20,
                flushIntervalMilliseconds: 5_000,
                maxQueueSize: 1_000,
                eventTtlMilliseconds: 82_800_000,
                diagnosticListener: diagnostics
            )
        )
        let eventId = try XCTUnwrap(
            try client.track(
                eventName: config.eventName,
                properties: try PulsepondProperties()
                    .setString(key: "runtime", value: "ios")
                    .setString(key: "artifact_version", value: config.artifactVersion)
            )
        )

        try await client.flush()
        try await client.shutdown()

        XCTAssertEqual(diagnostics.wireNames(), [])
        print("PULSEPOND_IOS_DOGFOOD_EVENT_ID=\(eventId)")
    }
}
