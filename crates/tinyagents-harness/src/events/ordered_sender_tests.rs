use super::*;

use tokio::sync::mpsc;

#[test]
fn detached_sender_ignores_items() {
    let sender = OrderedSender::<u32>::new(None);
    assert!(!sender.is_attached());
    sender.send(1).unwrap();
}

#[tokio::test]
async fn items_arrive_in_order_when_the_channel_has_room() {
    let (tx, mut rx) = mpsc::channel(8);
    let sender = OrderedSender::new(Some(tx));
    for i in 0..5u32 {
        sender.send(i).unwrap();
    }
    for i in 0..5u32 {
        assert_eq!(rx.recv().await, Some(i));
    }
}

#[tokio::test]
async fn backpressure_spills_without_dropping_or_reordering() {
    let (tx, mut rx) = mpsc::channel(1);
    let sender = OrderedSender::new(Some(tx));
    for i in 0..50u32 {
        sender.send(i).unwrap();
    }
    for i in 0..50u32 {
        assert_eq!(rx.recv().await, Some(i), "item {i} lost or reordered");
    }
}

#[tokio::test]
async fn items_sent_after_a_spill_never_overtake_it() {
    let (tx, mut rx) = mpsc::channel(1);
    let sender = OrderedSender::new(Some(tx));
    sender.send(0u32).unwrap();
    sender.send(1).unwrap(); // full: spills
    assert_eq!(rx.recv().await, Some(0));
    // Channel has room again, but the spill is still draining.
    sender.send(2).unwrap();
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
}

#[tokio::test]
async fn closed_receiver_discards_quietly() {
    let (tx, rx) = mpsc::channel(1);
    let sender = OrderedSender::new(Some(tx));
    drop(rx);
    for i in 0..4u32 {
        assert!(matches!(sender.send(i), Err(SendFailure::Closed(_)) | Ok(())));
    }
}

#[test]
fn full_channel_without_a_runtime_reports_backpressure_with_the_item() {
    let (tx, _rx) = mpsc::channel(1);
    let sender = OrderedSender::new(Some(tx));
    sender.send(0u32).unwrap();
    assert_eq!(sender.send(1), Err(SendFailure::Backpressure(1)));
}

#[tokio::test]
async fn a_stalled_receiver_cannot_grow_the_overflow_queue_without_bound() {
    let (tx, _rx) = mpsc::channel(1);
    let sender = OrderedSender::new(Some(tx)).with_max_overflow(2);
    sender.send(0u32).unwrap();
    sender.send(1).unwrap(); // spills (queued 1, drain task blocked)
    sender.send(2).unwrap();
    assert_eq!(sender.send(3), Err(SendFailure::Backpressure(3)));
}
