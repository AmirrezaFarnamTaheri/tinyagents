use super::*;
use std::sync::Mutex as StdMutex;

#[derive(Default)]
struct Events(StdMutex<Vec<(u8, String, RunMode)>>);
impl ModeChangeNotifier<u8> for Events {
    fn changed(&self, scope: &u8, thread: &str, mode: RunMode) {
        self.0
            .lock()
            .unwrap()
            .push((*scope, thread.to_owned(), mode));
    }
}

#[test]
fn modes_are_scoped_reused_and_notified() {
    let events = Arc::new(Events::default());
    let store = InMemoryRunModeStore::new(events.clone());
    let first = store.handle_for_thread(&1, "same");
    let second = store.handle_for_thread(&1, "same");
    assert_eq!(first.get(), RunMode::Build);
    store.set_mode(&1, "same", RunMode::Plan);
    assert_eq!(second.get(), RunMode::Plan);
    assert_eq!(store.get_mode(&2, "same"), RunMode::Build);
    assert_eq!(
        *events.0.lock().unwrap(),
        vec![(1, "same".into(), RunMode::Plan)]
    );
}

#[test]
fn run_mode_labels_round_trip() {
    assert_eq!(mode_label(RunMode::Plan), "plan");
    assert_eq!(mode_label(RunMode::Build), "build");
    assert_eq!(parse_mode_label("plan"), Some(RunMode::Plan));
    assert_eq!(parse_mode_label("build"), Some(RunMode::Build));
    assert_eq!(parse_mode_label("other"), None);
}

#[test]
fn unchanged_mode_does_not_notify_and_unknown_reads_do_not_create_handles() {
    let events = Arc::new(Events::default());
    let store = InMemoryRunModeStore::new(events.clone());

    assert_eq!(store.get_mode(&1, "unseen"), RunMode::Build);
    assert!(store.handles.lock().is_empty());
    store.set_mode(&1, "unseen", RunMode::Build);
    assert!(events.0.lock().unwrap().is_empty());
    assert!(store.handles.lock().is_empty());
}

struct ReentrantNotifier {
    store: std::sync::OnceLock<std::sync::Weak<InMemoryRunModeStore<u8>>>,
    calls: std::sync::atomic::AtomicUsize,
}

impl ModeChangeNotifier<u8> for ReentrantNotifier {
    fn changed(&self, scope: &u8, thread: &str, mode: RunMode) {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.store
                .get()
                .and_then(std::sync::Weak::upgrade)
                .unwrap()
                .set_mode(scope, thread, mode);
        }
    }
}

#[test]
fn reentrant_same_mode_change_notifies_once() {
    let notifier = Arc::new(ReentrantNotifier {
        store: std::sync::OnceLock::new(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let store = Arc::new(InMemoryRunModeStore::new(notifier.clone()));
    notifier.store.set(Arc::downgrade(&store)).unwrap();

    store.set_mode(&1, "thread", RunMode::Plan);
    assert_eq!(notifier.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
