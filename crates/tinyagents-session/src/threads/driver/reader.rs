//! Exhaustive bounded backend windows; metadata revalidation excludes retired incarnations.
use super::*;
impl DriverConversationStore {
    pub(super) async fn live_rows(&self) -> ConversationResult<Vec<Versioned<ThreadDoc>>> {
        self.admit().await?;
        let mut query = Query::filter(Filter::eq("deleted", false)).limit(PAGE_SIZE);
        let mut rows = Vec::new();
        loop {
            let page = self.docs.query(COLLECTION, &query).await?;
            for item in page.items {
                let row = Self::decode(item)?;
                if !row.doc.deleted {
                    rows.push(row);
                }
            }
            let Some(next) = page.next else {
                return Ok(rows);
            };
            if query.cursor.as_ref() == Some(&next) {
                return Err(StorageError::backend("nonadvancing conversation page cursor").into());
            }
            query.cursor = Some(next);
        }
    }
    pub(super) async fn messages(&self, id: &str) -> ConversationResult<Vec<ThreadMessage>> {
        for _ in 0..RETRIES {
            let Some(row) = self.load(id).await?.filter(|row| !row.doc.deleted) else {
                return Ok(Vec::new());
            };
            let mut messages = Vec::new();
            let mut offset = 0;
            let stream = Self::stream(&row.id, &row.doc.incarnation);
            while offset < row.doc.visible_end {
                let limit = usize::try_from((row.doc.visible_end - offset).min(PAGE_SIZE as u64))
                    .expect("bounded page");
                let entries = self.streams.read_window(&stream, offset, limit).await?;
                if entries.is_empty() {
                    return Err(StorageError::serialization(
                        "missing committed conversation prefix",
                    )
                    .into());
                }
                for entry in entries {
                    if entry.offset != offset || entry.offset >= row.doc.visible_end {
                        return Err(StorageError::serialization(
                            "noncontiguous conversation stream",
                        )
                        .into());
                    }
                    let record: MessageRecord =
                        serde_json::from_value(entry.value).map_err(|e| {
                            StorageError::serialization("invalid conversation stream record")
                                .with_source(e)
                        })?;
                    if record.schema != 1 || record.incarnation != row.doc.incarnation {
                        return Err(StorageError::serialization(
                            "conversation stream incarnation/schema mismatch",
                        )
                        .into());
                    }
                    messages.push(record.message);
                    offset += 1;
                }
            }
            let current = self.load(id).await?;
            if current
                .as_ref()
                .is_some_and(|current| current.version == row.version)
            {
                return Ok(messages);
            }
        }
        Err(StorageError::conflict("conversation read retry bound exhausted").into())
    }
}
