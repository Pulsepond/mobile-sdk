import Foundation
import Pulsepond

private final class DiagnosticSink: NSObject, PulsepondDiagnosticListener {
    func onDiagnostic(diagnostic: PulsepondDiagnostic) {
        _ = diagnostic.code.wireName
    }
}

@available(iOS 13.0, *)
func makePulsepondClient() async throws -> Pulsepond {
    let configuration = try PulsepondConfiguration(
        endpoint: "https://events.example.com/v1/batch",
        writeKey: "ppw_v1_0123456789abcdef0123456789abcdef_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        deploymentId: "01234567-89ab-4def-8abc-0123456789ab",
        projectId: "project_foundation",
        sourceId: "source_ios",
        environment: "production",
        appVersion: "1.0.0",
        release: "ios@1.0.0",
        batchSize: 20,
        flushIntervalMilliseconds: 5_000,
        maxQueueSize: 1_000,
        eventTtlMilliseconds: 82_800_000,
        diagnosticListener: DiagnosticSink()
    )
    let properties = try PulsepondProperties()
        .setString(key: "work_id", value: "work_123")
        .setBoolean(key: "completed", value: false)
    let client = try await PulsepondApple.shared.create(configuration: configuration)
    _ = try client.track(eventName: "view_work", properties: properties)
    return client
}

@available(iOS 13.0, *)
func flushAndShutdown(_ client: Pulsepond) async throws {
    try await client.flush()
    try await client.shutdown()
}
