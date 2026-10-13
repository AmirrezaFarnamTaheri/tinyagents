//! Message-log rewrites (edit, truncate) and the purge.
//!
//! Committed messages live in an immutable per-incarnation stream, so a rewrite
//! never edits it in place. It writes the replacement log to a stream of a
//! fresh incarnation (invisible until published), then swaps the metadata
//! document onto it with a compare-and-swap on the version it read. A
//! concurrent writer therefore either lands before the swap (and the rewrite
//! retries on the new version) or fails with a conflict afterwards, because
//! its incarnation is retired. The retired stream and any append intent that
//! belonged to it are removed after the swap, best effort.
use super::pending::APPENDS;
use super::*;

/// Messages written per stream append while copying a log.
const COPY_BATCH: usize = 128;

impl DriverConversationStore {
    /// Replace `thread_id`'s log with what `edit` returns for the current one.
    ///
    /// `edit` returns `None` to leave the log untouched, or the new messages
    /// plus a value to hand back to the caller.
    async fn rewrite<R>(
        &self,
        thread_id: &str,
        edit: impl Fn(Vec<ThreadMessage>) -> ConversationResult<Option<(Vec<ThreadMessage>, R)>>,
    ) -> ConversationResult<Option<R>> {
        let key = Self::key(thread_id)?;
        for _ in 0..RETRIES {
            let Some(row) = self.load(thread_id).await?.filter(|row| !row.doc.deleted) else {
                return Err(StorageError::not_found("conversation thread not found").into());
            };
            let current = self.messages(thread_id).await?;
            let Some((next, result)) = edit(current)? else {
                return Ok(None);
            };
            let incarnation = uuid::Uuid::new_v4().to_string();
            let stream = Self::stream(&key, &incarnation);
            if let Err(error) = self.write_log(&stream, &incarnation, &next).await {
                let _ = self.streams.delete_stream(&stream).await;
                return Err(error);
            }
            let mut doc = row.doc.clone();
            doc.incarnation = incarnation;
            doc.visible_end = next.len() as u64;
            doc.thread.message_count = next.len();
            doc.thread.last_message_at = next
                .last()
                .map(|message| message.created_at.clone())
                .unwrap_or_else(|| doc.thread.created_at.clone());
            let body = serde_json::to_value(&doc).map_err(|e| {
                StorageError::serialization("encode rewritten conversation metadata")
                    .with_source(e)
            })?;
            match self
                .docs
                .put(COLLECTION, &key, body, row.unchanged())
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        target: "tinyagents_session::threads",
                        messages = next.len(),
                        "conversation log rewritten onto a new incarnation"
                    );
                    self.retire(&key, &row.doc.incarnation).await;
                    return Ok(Some(result));
                }
                Err(error) => {
                    let _ = self.streams.delete_stream(&stream).await;
                    if error.kind() == ErrorKind::Conflict {
                        continue;
                    }
                    return Err(error.into());
                }
            }
        }
        Err(StorageError::conflict("conversation rewrite retry bound exhausted").into())
    }

    async fn write_log(
        &self,
        stream: &str,
        incarnation: &str,
        messages: &[ThreadMessage],
    ) -> ConversationResult<()> {
        for chunk in messages.chunks(COPY_BATCH) {
            let values = chunk
                .iter()
                .map(|message| {
                    serde_json::to_value(MessageRecord {
                        schema: 1,
                        incarnation: incarnation.to_owned(),
                        message: message.clone(),
                    })
                    .map_err(|e| {
                        StorageError::serialization("encode conversation record").with_source(e)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.streams.append_batch(stream, values).await?;
        }
        Ok(())
    }

    /// Drop a retired incarnation's stream and append intent. Failures leave
    /// unreachable garbage only, so they are logged and not surfaced.
    async fn retire(&self, key: &str, incarnation: &str) {
        let stream = Self::stream(key, incarnation);
        if let Err(error) = self.streams.delete_stream(&stream).await {
            tracing::debug!(target: "tinyagents_session::threads", %error, "retired stream not removed");
        }
        let intent = pending::AppendIntent::key(key, incarnation);
        if let Err(error) = self.docs.delete(APPENDS, &intent, Precondition::None).await {
            tracing::debug!(target: "tinyagents_session::threads", %error, "retired append intent not removed");
        }
    }

    pub(super) async fn update_message_native(
        &self,
        thread_id: &str,
        message_id: &str,
        patch: ThreadMessagePatch,
    ) -> ConversationResult<ThreadMessage> {
        let updated = self
            .rewrite(thread_id, |mut messages| {
                let Some(message) = messages.iter_mut().find(|m| m.id == message_id) else {
                    return Err(StorageError::not_found("conversation message not found").into());
                };
                if let Some(extra) = patch.extra_metadata.clone() {
                    message.extra_metadata = extra;
                }
                let updated = message.clone();
                Ok(Some((messages, updated)))
            })
            .await?;
        updated.ok_or_else(|| StorageError::not_found("conversation message not found").into())
    }

    pub(super) async fn delete_messages_from_native(
        &self,
        thread_id: &str,
        message_id: &str,
    ) -> ConversationResult<Option<usize>> {
        self.rewrite(thread_id, |mut messages| {
            let Some(cut) = messages.iter().position(|m| m.id == message_id) else {
                return Ok(None);
            };
            let removed = messages.len() - cut;
            messages.truncate(cut);
            Ok(Some((messages, removed)))
        })
        .await
        .or_else(|error| match error {
            // A cut point in a thread that does not exist is "nothing to truncate".
            ConversationError::Driver(ref e) if e.kind() == ErrorKind::NotFound => Ok(None),
            other => Err(other),
        })
    }

    pub(super) async fn purge_native(&self) -> ConversationResult<ConversationPurgeStats> {
        let mut stats = ConversationPurgeStats::default();
        let mut query = Query::filter(Filter::All).limit(PAGE_SIZE);
        loop {
            let page = self.docs.query(COLLECTION, &query).await?;
            for item in page.items {
                let row = Self::decode(item)?;
                self.retire(&row.id, &row.doc.incarnation).await;
                if !row.doc.deleted {
                    stats.thread_count += 1;
                    stats.message_count += row.doc.thread.message_count;
                }
                self.docs.delete(COLLECTION, &row.id, Precondition::None).await?;
            }
            // Deleting shifts later rows into the first page, so restart from
            // the head until the collection is empty.
            match page.next {
                Some(_) => query = Query::filter(Filter::All).limit(PAGE_SIZE),
                None => return Ok(stats),
            }
        }
    }
}
