use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use super::*;
use crate::steering::SteeringPolicy;

/// 900s backstop: wind-down at 780s, hard stop at 840s.
fn point_started(ago: Duration, now: Instant) -> TurnDeadline {
    TurnDeadline::new(now - ago, Duration::from_secs(900))
}

fn handle() -> SteeringHandle {
    SteeringHandle::new(SteeringPolicy::allow_all())
}

#[test]
fn does_not_pause_before_the_wind_down_point() {
    let now = Instant::now();
    let h = handle();
    let mw = WindDownMiddleware::new(h.clone(), point_started(Duration::from_secs(100), now));
    assert!(!mw.maybe_pause("after_model", now));
    assert!(!mw.fired());
    assert_eq!(h.pending(), 0);
}

#[test]
fn pauses_once_after_the_wind_down_point() {
    let now = Instant::now();
    let h = handle();
    let mw = WindDownMiddleware::new(h.clone(), point_started(Duration::from_secs(781), now));
    assert!(mw.maybe_pause("after_tool", now));
    assert!(mw.fired());
    assert!(!mw.maybe_pause("after_model", now), "pause is latched");
    assert_eq!(h.drain(), vec![SteeringCommand::Pause]);
}

#[test]
fn the_pause_callback_runs_exactly_once() {
    let now = Instant::now();
    let mw = WindDownMiddleware::new(handle(), point_started(Duration::from_secs(781), now));
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    mw.set_on_pause(Arc::new(move || {
        seen.fetch_add(1, Ordering::SeqCst);
    }));
    assert!(mw.maybe_pause("after_tool", now));
    assert!(!mw.maybe_pause("after_tool", now));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn clamp_ends_at_the_hard_stop_and_never_later() {
    let now = Instant::now();
    let point = point_started(Duration::ZERO, now);
    let clamped = point.clamp_wall_clock_ms(Some(3_600_000), now).unwrap();
    assert!(clamped <= 840_000 && clamped > 830_000, "got {clamped}");
    assert_eq!(point.clamp_wall_clock_ms(Some(1_000), now), Some(1_000));
    assert!(point.clamp_wall_clock_ms(None, now).is_some());
    let late = point_started(Duration::from_secs(900), now);
    assert_eq!(late.clamp_wall_clock_ms(None, now), Some(1));
}

#[test]
fn install_clamps_the_policy_and_adds_the_middleware_for_a_root_turn() {
    let mut harness: AgentHarness<(), ()> = AgentHarness::new();
    let now = Instant::now();
    let h = handle();
    let mw = install(
        &mut harness,
        Some(&h),
        Some(point_started(Duration::ZERO, now)),
        false,
    );
    assert!(mw.is_some());
    let clamped = harness.policy().limits.max_wall_clock_ms.expect("bounded");
    assert!(clamped <= 840_000 && clamped > 830_000, "got {clamped}");
}

#[test]
fn install_leaves_children_and_unbounded_turns_alone() {
    let h = handle();
    let now = Instant::now();
    let mut child: AgentHarness<(), ()> = AgentHarness::new();
    let before = child.policy().limits.max_wall_clock_ms;
    assert!(
        install(
            &mut child,
            Some(&h),
            Some(point_started(Duration::ZERO, now)),
            true
        )
        .is_none()
    );
    assert_eq!(child.policy().limits.max_wall_clock_ms, before);

    let mut plain: AgentHarness<(), ()> = AgentHarness::new();
    let before = plain.policy().limits.max_wall_clock_ms;
    assert!(install(&mut plain, Some(&h), None, false).is_none());
    assert_eq!(plain.policy().limits.max_wall_clock_ms, before);
}

#[test]
fn install_without_a_handle_still_clamps() {
    let mut harness: AgentHarness<(), ()> = AgentHarness::new();
    let now = Instant::now();
    assert!(
        install(
            &mut harness,
            None,
            Some(point_started(Duration::ZERO, now)),
            false
        )
        .is_none()
    );
    assert!(harness.policy().limits.max_wall_clock_ms.is_some());
}

#[test]
fn install_skips_the_middleware_when_the_handle_cannot_pause() {
    let mut harness = crate::runtime::AgentHarness::<(), ()>::new_for_test();
    let refuses = SteeringHandle::new(SteeringPolicy::new());
    let deadline = Some(point_started(Duration::from_secs(1), Instant::now()));
    assert!(install_wind_down(&mut harness, deadline, Some(&refuses), false).is_none());
}
