// Message text crosses only a private stdin/stdout pipe, never arguments or logs.
import AppKit
import Foundation
import NaturalLanguage
import SwiftUI
import Translation

struct Request: Decodable {
    let operation: String
    let text: String?
    let source: String?
    let target: String?
}

struct Language: Encodable {
    let code: String
    let name: String
}

struct Response: Encodable {
    var status: String
    var text: String? = nil
    var source: String? = nil
    var languages: [Language]? = nil
}

func respond(_ response: Response) {
    if let data = try? JSONEncoder().encode(response) {
        FileHandle.standardOutput.write(data)
        FileHandle.standardOutput.write(Data([10]))
    }
}

@available(macOS 26.0, *)
struct PreparationView: View {
    let source: Locale.Language
    let target: Locale.Language

    var body: some View {
        VStack(spacing: 16) {
            Text("On-device chat translation").font(.title2)
            Text("Download the language packs to translate chats offline. Message text stays on this Mac.")
                .multilineTextAlignment(.center)
            ProgressView()
        }
        .padding(32)
        .frame(width: 400, height: 180)
        .translationTask(source: source, target: target) { session in
            do {
                try await session.prepareTranslation()
                respond(Response(status: "prepared"))
            } catch {
                respond(Response(status: "failed"))
            }
            exit(0)
        }
    }
}

@MainActor
@available(macOS 26.0, *)
func prepare(source: Locale.Language, target: Locale.Language) {
    let app = NSApplication.shared
    app.setActivationPolicy(.regular)
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 464, height: 244),
        styleMask: [.titled, .closable], backing: .buffered, defer: false)
    window.title = "ZapFast Translation"
    window.contentView = NSHostingView(rootView: PreparationView(source: source, target: target))
    window.center()
    window.makeKeyAndOrderFront(nil)
    app.activate(ignoringOtherApps: true)
    // Closing the preparation window also ends the helper.
    let observer = NotificationCenter.default.addObserver(
        forName: NSWindow.willCloseNotification, object: window, queue: .main
    ) { _ in
        respond(Response(status: "failed"))
        exit(0)
    }
    withExtendedLifetime(observer) { app.run() }
}

@main
struct TranslationHelper {
    @MainActor
    static func main() async {
        guard #available(macOS 26.0, *) else {
            respond(Response(status: "unavailable"))
            return
        }
        guard let line = readLine(), let data = line.data(using: .utf8),
              let request = try? JSONDecoder().decode(Request.self, from: data) else {
            respond(Response(status: "failed"))
            return
        }
        let availability = LanguageAvailability()
        let languages = await availability.supportedLanguages.sorted {
            $0.minimalIdentifier.count < $1.minimalIdentifier.count
        }
        if request.operation == "languages" {
            respond(Response(status: "languages", languages: languages.map {
                Language(code: $0.minimalIdentifier,
                         name: Locale.current.localizedString(forLanguageCode: $0.minimalIdentifier)
                            ?? $0.minimalIdentifier)
            }.sorted { $0.name < $1.name }))
            return
        }
        guard let targetCode = request.target,
              let target = languages.first(where: { $0.minimalIdentifier == targetCode }) else {
            respond(Response(status: "unsupported"))
            return
        }
        if request.operation == "prepare", let sourceCode = request.source,
           let source = languages.first(where: { $0.minimalIdentifier == sourceCode }) {
            if await availability.status(from: source, to: target) == .installed {
                respond(Response(status: "prepared"))
                return
            }
            prepare(source: source, target: target)
            return
        }
        guard request.operation == "translate", let text = request.text else {
            respond(Response(status: "failed"))
            return
        }
        let recognizer = NLLanguageRecognizer()
        recognizer.processString(text)
        guard let detected = recognizer.dominantLanguage else {
            respond(Response(status: "original"))
            return
        }
        let detectedLanguage = Locale.Language(identifier: detected.rawValue)
        guard let source = languages.first(where: {
            $0.languageCode == detectedLanguage.languageCode &&
            ($0.languageCode?.identifier != "zh" || $0.script == detectedLanguage.script)
        }) else {
            respond(Response(status: "unsupported"))
            return
        }
        if source.languageCode == target.languageCode && source.script == target.script {
            respond(Response(status: "original"))
            return
        }
        switch await availability.status(from: source, to: target) {
        case .installed: break
        case .supported:
            respond(Response(status: "download", source: source.minimalIdentifier))
            return
        default:
            respond(Response(status: "unsupported"))
            return
        }
        do {
            let session = TranslationSession(installedSource: source, target: target)
            let response = try await session.translate(text)
            respond(Response(status: "translated", text: response.targetText,
                             source: source.minimalIdentifier))
        } catch {
            respond(Response(status: "failed"))
        }
    }
}
