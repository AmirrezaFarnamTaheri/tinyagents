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
