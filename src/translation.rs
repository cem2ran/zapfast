//! Bounded, in-memory chat translations on a dedicated worker. The bundled
//! Apple helper uses local models; there is no HTTP client or cloud fallback.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::backend::Waker;

const HELPER: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/zapfast-translate"));
const CAPACITY: usize = 256;
const QUEUE: usize = 8;
const MAX_TEXT: usize = 16_384;

/// A result is valid only for this exact message revision and target.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    pub chat: String,
    pub message: String,
    pub text: String,
    pub target: String,
}

impl Key {
    pub fn new(chat: &str, message: &str, text: &str, target: &str) -> Self {
        Self {
            chat: chat.into(),
            message: message.into(),
            text: text.into(),
            target: target.into(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum State {
    Pending,
    Translated(String),
    Original,
    Download(String),
    Unsupported,
    Failed,
}

pub struct Entry {
    pub state: State,
    pub show_original: bool,
}

#[derive(Clone, Deserialize)]
pub struct Language {
    pub code: String,
    pub name: String,
}

#[derive(Serialize)]
struct Request<'a> {
    operation: &'a str,
    text: Option<&'a str>,
    source: Option<&'a str>,
    target: Option<&'a str>,
}

#[derive(Deserialize)]
struct Response {
    status: String,
    text: Option<String>,
    source: Option<String>,
    languages: Option<Vec<Language>>,
}

impl Response {
    fn state(self) -> State {
        match self.status.as_str() {
            "translated" => self
                .text
                .filter(|text| !text.is_empty())
                .map_or(State::Failed, State::Translated),
            "original" => State::Original,
            "download" => self.source.map_or(State::Failed, State::Download),
            "unsupported" => State::Unsupported,
            _ => State::Failed,
        }
    }
}

enum Work {
    Languages,
    Translate(Key, u64),
    Prepare(String, String),
}

enum Result {
    Languages(Option<Vec<Language>>),
    Translated(Key, u64, State),
    Prepared(bool),
}

/// UI-side controller. No message text or derived translation is persisted.
#[derive(Default)]
pub struct Translations {
    entries: HashMap<Key, Entry>,
    order: VecDeque<Key>,
    worker: Option<mpsc::SyncSender<Work>>,
    results: Option<mpsc::Receiver<Result>>,
    generation: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    pub languages: Vec<Language>,
    pub unavailable: bool,
    pub preparing: bool,
    pub preparation_failed: bool,
}

impl Translations {
    pub fn supported_build() -> bool {
        cfg!(target_os = "macos") && !HELPER.is_empty()
    }

    pub fn load_languages(&mut self, waker: &Waker) {
        if self.worker.is_some() || self.unavailable {
            return;
        }
        if !Self::supported_build() {
            self.unavailable = true;
            return;
        }
        let (sender, receiver) = mpsc::sync_channel(QUEUE);
        let (results, incoming) = mpsc::channel();
        let generation = self.generation.clone();
        let stopped = self.stopped.clone();
        let waker = waker.clone();
        let started = std::thread::Builder::new()
            .name("chat-translation".into())
            .spawn(move || {
                // tempfile creates a private directory, and removes the executable
                // once the worker exits. Message text is sent only through pipes.
                let directory = tempfile::tempdir().ok();
                let helper = directory
                    .as_ref()
                    .and_then(|directory| install_helper(directory.path()).ok());
                while let Ok(work) = receiver.recv() {
                    let result = match work {
                        Work::Languages => {
                            let response = helper.as_deref().and_then(|path| {
                                run(
                                    path,
                                    &Request {
                                        operation: "languages",
                                        text: None,
                                        source: None,
                                        target: None,
                                    },
                                    Duration::from_secs(30),
                                    || stopped.load(Ordering::Relaxed),
                                )
                            });
                            Result::Languages(response.and_then(|response| response.languages))
                        }
                        Work::Translate(key, epoch) => {
                            if generation.load(Ordering::Relaxed) != epoch {
                                continue;
                            }
                            let response = helper.as_deref().and_then(|path| {
                                run(
                                    path,
                                    &Request {
                                        operation: "translate",
                                        text: Some(&key.text),
                                        source: None,
                                        target: Some(&key.target),
                                    },
                                    Duration::from_secs(30),
                                    || {
                                        stopped.load(Ordering::Relaxed)
                                            || generation.load(Ordering::Relaxed) != epoch
                                    },
                                )
                            });
                            Result::Translated(
                                key,
                                epoch,
                                response.map_or(State::Failed, Response::state),
                            )
                        }
                        Work::Prepare(source, target) => {
                            let response = helper.as_deref().and_then(|path| {
                                run(
                                    path,
                                    &Request {
                                        operation: "prepare",
                                        text: None,
                                        source: Some(&source),
                                        target: Some(&target),
                                    },
                                    Duration::from_secs(600),
                                    || stopped.load(Ordering::Relaxed),
                                )
                            });
                            Result::Prepared(
                                response.is_some_and(|response| response.status == "prepared"),
                            )
                        }
                    };
                    if results.send(result).is_err() {
                        break;
                    }
                    waker.wake();
                }
            });
        if started.is_err() {
            self.unavailable = true;
            return;
        }
        self.results = Some(incoming);
        self.worker = Some(sender);
        let _ = self.worker.as_ref().unwrap().try_send(Work::Languages);
    }

    pub fn get(&self, key: &Key) -> Option<&Entry> {
        self.entries.get(key)
    }

    pub fn request(&mut self, key: Key, waker: &Waker) {
        if self.entries.contains_key(&key) || !eligible(&key.text) {
            return;
        }
        self.load_languages(waker);
        if self.languages.is_empty()
            || self.preparing
            || (self.entries.len() >= CAPACITY
                && self
                    .entries
                    .values()
                    .all(|entry| entry.state == State::Pending))
        {
            return;
        }
        let work = Work::Translate(key.clone(), self.generation.load(Ordering::Relaxed));
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.try_send(work).is_ok())
        {
            self.insert(key, State::Pending);
        }
    }

    fn insert(&mut self, key: Key, state: State) {
        // Evict only completed entries: a pending request must stay deduplicated.
        if self.entries.len() >= CAPACITY {
            let victim = self.order.iter().position(|key| {
                self.entries
                    .get(key)
                    .is_some_and(|entry| entry.state != State::Pending)
            });
            let Some(index) = victim else {
                return;
            };
            let key = self.order.remove(index).unwrap();
            self.entries.remove(&key);
        }
        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            Entry {
                state,
                show_original: false,
            },
        );
    }

    /// Returns the rows whose measured heights need invalidation.
    pub fn poll(&mut self) -> Vec<(String, String)> {
        let mut changed = Vec::new();
        let results: Vec<_> = self
            .results
            .iter()
            .flat_map(|results| results.try_iter())
            .collect();
        for result in results {
            match result {
                Result::Languages(languages) => {
                    self.unavailable = languages.is_none();
                    self.languages = languages.unwrap_or_default();
                }
                Result::Translated(key, epoch, state) => {
                    if epoch == self.generation.load(Ordering::Relaxed)
                        && let Some(entry) = self.entries.get_mut(&key)
                    {
                        entry.state = state;
                        changed.push((key.chat, key.message));
                    }
                }
                Result::Prepared(success) => {
                    self.preparing = false;
                    self.preparation_failed = !success;
                    if success {
                        self.clear();
                    }
                }
            }
        }
        changed
    }

    pub fn toggle_original(&mut self, key: &Key) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.show_original = !entry.show_original;
        }
    }

    pub fn retry(&mut self, key: &Key) {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| entry.state != State::Pending)
        {
            self.entries.remove(key);
            self.order.retain(|old| old != key);
        }
    }

    pub fn prepare(&mut self, source: String, target: String) {
        if self.preparing {
            return;
        }
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.try_send(Work::Prepare(source, target)).is_ok())
        {
            self.preparing = true;
            self.preparation_failed = false;
        }
    }

    pub fn clear(&mut self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.entries.clear();
        self.order.clear();
    }

    /// Synthetic translated sample for layout tests and demo captures.
    #[cfg(any(test, feature = "demo"))]
    pub fn sample(&mut self, key: Key, text: &str) {
        self.insert(key, State::Translated(text.into()));
    }
}

impl Drop for Translations {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
    }
}

/// Limit both memory and pipe size, and leave non-language content alone.
pub fn eligible(text: &str) -> bool {
    text.len() <= MAX_TEXT
        && text
            .chars()
            .filter(|character| character.is_alphabetic())
            .take(2)
            .count()
            == 2
        && !text
            .split_whitespace()
            .all(|word| word.starts_with("https://") || word.starts_with("http://"))
}

/// Only incoming text and media captions take part in automatic translation.
pub fn message_text(message: &crate::model::Message) -> Option<&str> {
    use crate::model::Content;
    if message.from_me {
        return None;
    }
    match &message.content {
        Content::Text { text, .. } => Some(text),
        Content::Image { caption, .. }
        | Content::Video { caption, .. }
        | Content::Document { caption, .. } => caption.as_deref(),
        _ => None,
    }
}

fn install_helper(directory: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
    // The bundle gives SwiftUI's system download sheet a stable app identity.
    let contents = directory.join("ZapFast Translation.app/Contents");
    let executables = contents.join("MacOS");
    std::fs::create_dir_all(&executables)?;
    std::fs::write(
        contents.join("Info.plist"),
        br#"<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.cem2ran.zapfast.translation</string>
<key>CFBundleName</key><string>ZapFast Translation</string>
<key>CFBundleExecutable</key><string>zapfast-translate</string>
<key>CFBundlePackageType</key><string>APPL</string>
</dict></plist>"#,
    )?;
    let path = executables.join("zapfast-translate");
    std::fs::write(&path, HELPER)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(path)
}

// A stalled native service cannot stall the app or retain an unbounded queue.
fn run(
    path: &std::path::Path,
    request: &Request<'_>,
    timeout: Duration,
    cancelled: impl Fn() -> bool,
) -> Option<Response> {
    use std::process::{Command, Stdio};
    let mut child = Command::new(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let output = child.stdout.take()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = output
            .take(65_536)
            .read_to_end(&mut bytes)
            .ok()
            .map(|_| bytes);
        let _ = sender.send(result);
    });
    let written = child.stdin.take().is_some_and(|mut input| {
        serde_json::to_writer(&mut input, request).is_ok() && input.write_all(b"\n").is_ok()
    });
    let deadline = std::time::Instant::now() + timeout;
    let bytes = loop {
        if !written || cancelled() || std::time::Instant::now() >= deadline {
            break None;
        }
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(bytes) => break bytes,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break None,
        }
    };
    // Also reap helpers that failed or were closed during language preparation.
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();
    bytes.and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(text: &str, target: &str) -> Key {
        Key::new("synthetic-chat", "message", text, target)
    }

    #[test]
    fn revisions_targets_and_chats_cannot_reuse_another_translation() {
        let mut translations = Translations::default();
        translations.sample(key("Hola mundo", "en"), "Hello world");
        assert!(translations.get(&key("Hola mundo editado", "en")).is_none());
        assert!(translations.get(&key("Hola mundo", "de")).is_none());
        assert!(
            translations
                .get(&Key::new("other", "message", "Hola mundo", "en"))
                .is_none()
        );
        translations.toggle_original(&key("Hola mundo", "en"));
        assert!(
            translations
                .get(&key("Hola mundo", "en"))
                .unwrap()
                .show_original
        );
        translations.clear();
        assert!(translations.get(&key("Hola mundo", "en")).is_none());
    }

    #[test]
    fn stale_results_are_discarded_after_clearing() {
        let mut translations = Translations::default();
        let (sender, receiver) = mpsc::channel();
        translations.results = Some(receiver);
        translations.insert(key("Hola mundo", "en"), State::Pending);
        translations.clear();
        translations.insert(key("Hola mundo", "en"), State::Pending);
        sender
            .send(Result::Translated(
                key("Hola mundo", "en"),
                0,
                State::Translated("stale".into()),
            ))
            .unwrap();
        assert!(translations.poll().is_empty());
        assert!(matches!(
            translations.get(&key("Hola mundo", "en")).unwrap().state,
            State::Pending
        ));
    }

    #[test]
    fn cache_is_bounded_and_pending_requests_stay_deduplicated() {
        let mut translations = Translations::default();
        translations.insert(key("Pending text", "en"), State::Pending);
        for i in 0..CAPACITY * 2 {
            translations.sample(key(&format!("Synthetic text {i}"), "en"), "Translated");
        }
        assert_eq!(translations.entries.len(), CAPACITY);
        assert_eq!(translations.order.len(), CAPACITY);
        assert!(translations.get(&key("Pending text", "en")).is_some());
    }

    #[test]
    fn an_entirely_pending_cache_never_exceeds_the_limit() {
        let mut translations = Translations::default();
        for i in 0..CAPACITY + 1 {
            translations.insert(key(&format!("Pending {i}"), "en"), State::Pending);
        }
        assert_eq!(translations.entries.len(), CAPACITY);
        assert_eq!(translations.order.len(), CAPACITY);
    }

    #[test]
    fn visible_requests_are_deduplicated_and_the_queue_has_backpressure() {
        let mut translations = Translations::default();
        let (sender, receiver) = mpsc::sync_channel(1);
        translations.worker = Some(sender);
        translations.languages = vec![Language {
            code: "en".into(),
            name: "English".into(),
        }];
        translations.request(key("Hola mundo", "en"), &Waker::default());
        translations.request(key("Hola mundo", "en"), &Waker::default());
        translations.request(key("Bonjour le monde", "en"), &Waker::default());
        assert_eq!(translations.entries.len(), 1);
        assert_eq!(receiver.try_iter().count(), 1);
        translations.request(key("Bonjour le monde", "en"), &Waker::default());
        assert_eq!(receiver.try_iter().count(), 1);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "requires macOS 26 and installed Spanish/English language packs"]
    fn native_worker_translates_a_synthetic_chat() {
        let mut translations = Translations::default();
        translations.load_languages(&Waker::default());
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while translations.languages.is_empty() && !translations.unavailable {
            translations.poll();
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!translations.unavailable);
        let key = key("Hola, ¿cómo estás?", "en");
        translations.request(key.clone(), &Waker::default());
        loop {
            translations.poll();
            if let Some(Entry {
                state: State::Translated(text),
                ..
            }) = translations.get(&key)
            {
                assert_eq!(text, "Hello, how are you?");
                break;
            }
            assert!(matches!(
                translations.get(&key).map(|entry| &entry.state),
                Some(State::Pending)
            ));
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(50));
        }
        let same = Key::new("synthetic-chat", "english", "Hello, how are you?", "en");
        translations.request(same.clone(), &Waker::default());
        loop {
            translations.poll();
            if matches!(
                translations.get(&same).map(|entry| &entry.state),
                Some(State::Original)
            ) {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(50));
        }
        translations.prepare("es".into(), "en".into());
        assert!(translations.preparing);
        while translations.preparing {
            translations.poll();
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!translations.preparation_failed);
        assert!(translations.get(&key).is_none());
    }

    #[test]
    fn non_text_and_oversized_messages_are_skipped() {
        for text in [
            "",
            "🙂👍",
            "1234",
            "https://example.com",
            "http://example.com https://example.org",
        ] {
            assert!(!eligible(text));
        }
        assert!(!eligible(&"a".repeat(MAX_TEXT + 1)));
        assert!(eligible("Привет, мир!"));
        assert!(eligible("你好世界"));
    }

    #[test]
    fn native_failures_do_not_claim_success() {
        for status in ["failed", "unavailable", "translated", "download"] {
            let response = Response {
                status: status.into(),
                text: None,
                source: None,
                languages: None,
            };
            assert!(matches!(response.state(), State::Failed));
        }
    }
}
