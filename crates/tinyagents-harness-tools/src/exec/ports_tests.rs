use super::ports::{ExecBackend, ExecGate};
use std::sync::Arc;

#[test]
fn host_ports_are_object_safe_and_thread_safe() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_gate_object(_: Option<Arc<dyn ExecGate>>) {}
    fn assert_backend_object(_: Option<Arc<dyn ExecBackend>>) {}

    assert_send_sync::<Arc<dyn ExecGate>>();
    assert_send_sync::<Arc<dyn ExecBackend>>();
    assert_gate_object(None);
    assert_backend_object(None);
}
