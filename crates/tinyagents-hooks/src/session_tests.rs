use super::*;
use std::sync::Mutex;

struct Types;
impl SessionHookTypes for Types {
    type Context = ();
    type Request = String;
    type Options = usize;
    type StateView<'a> = &'a str;
    type ResumePreparation = &'static str;
    type TurnPreparation = &'static str;
    type Outcome = String;
    type TranscriptOptions = String;
    type CommitReceipt = String;
    type Terminal = String;
    type Error = &'static str;
}

#[tokio::test]
async fn callbacks_mutate_and_observe_their_typed_inputs() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let before_commit_seen = Arc::clone(&observations);
    let after_commit_seen = Arc::clone(&observations);
    let terminal_seen = Arc::clone(&observations);
    let hooks = FnSessionHooks::<Types>::new(
        |request, options, state| Box::pin(async move {
            *request = format!("{request}:{state}");
            *options += 1;
            Ok("resume-prepared")
        }),
        |request, options, state| Box::pin(async move {
            *request = format!("{request}:{state}");
            *options += 2;
            Ok("turn-prepared")
        }),
        move |outcome, options| {
            let seen = Arc::clone(&before_commit_seen);
            Box::pin(async move {
                seen.lock().unwrap().push(format!("before:{outcome}:{options}"));
                Ok(())
            })
        },
        move |receipt| {
            let seen = Arc::clone(&after_commit_seen);
            Box::pin(async move {
                seen.lock().unwrap().push(format!("commit:{receipt}"));
                Ok(())
            })
        },
        move |terminal| {
            let seen = Arc::clone(&terminal_seen);
            Box::pin(async move {
                seen.lock().unwrap().push(format!("terminal:{terminal}"));
                Ok(())
            })
        },
    );

    let mut request = "request".to_owned();
    let mut options = 0;
    assert_eq!(hooks.before_resume(&mut request, &mut options, "resume").await, Ok("resume-prepared"));
    assert_eq!(hooks.before_turn(&mut request, &mut options, "turn").await, Ok("turn-prepared"));
    assert_eq!((&request[..], options), ("request:resume:turn", 3));
    hooks.before_commit(&"candidate".to_owned(), &"transcript-options".to_owned()).await.unwrap();
    hooks.after_commit("receipt".to_owned()).await.unwrap();
    hooks.on_terminal("completed".to_owned()).await.unwrap();
    assert_eq!(
        *observations.lock().unwrap(),
        ["before:candidate:transcript-options", "commit:receipt", "terminal:completed"]
    );
}

#[tokio::test]
async fn callback_errors_are_returned_to_the_runtime_adapter() {
    let hooks = FnSessionHooks::<Types>::new(
        |_, _, _| Box::pin(async { Ok("resume") }),
        |_, _, _| Box::pin(async { Err("before turn failed") }),
        |_, _| Box::pin(async { Ok(()) }),
        |_| Box::pin(async { Err("commit failed") }),
        |_| Box::pin(async { Ok(()) }),
    );
    let mut request = String::new();
    let mut options = 0;
    assert_eq!(hooks.before_turn(&mut request, &mut options, "state").await, Err("before turn failed"));
    assert_eq!(hooks.after_commit(String::new()).await, Err("commit failed"));
}
