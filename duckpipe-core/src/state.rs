use std::fmt;

/// Table sync state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    Pending,
    Snapshot,
    Catchup,
    Streaming,
    Errored,
}

impl SyncState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncState::Pending => "PENDING",
            SyncState::Snapshot => "SNAPSHOT",
            SyncState::Catchup => "CATCHUP",
            SyncState::Streaming => "STREAMING",
            SyncState::Errored => "ERRORED",
        }
    }

    pub fn from_str(s: &str) -> Result<Self, crate::error::DuckPipeError> {
        match s {
            "PENDING" => Ok(SyncState::Pending),
            "SNAPSHOT" => Ok(SyncState::Snapshot),
            "CATCHUP" => Ok(SyncState::Catchup),
            "STREAMING" => Ok(SyncState::Streaming),
            "ERRORED" => Ok(SyncState::Errored),
            _ => Err(crate::error::DuckPipeError::Internal(format!(
                "unknown sync state: {}",
                s
            ))),
        }
    }

    /// Validate a state transition
    pub fn can_transition_to(&self, target: SyncState) -> bool {
        matches!(
            (self, target),
            (SyncState::Pending, SyncState::Snapshot)
                | (SyncState::Pending, SyncState::Streaming)
                | (SyncState::Snapshot, SyncState::Catchup)
                | (SyncState::Catchup, SyncState::Streaming)
                | (SyncState::Streaming, SyncState::Snapshot) // resync
                | (SyncState::Streaming, SyncState::Errored)
                | (SyncState::Snapshot, SyncState::Errored)
                | (SyncState::Catchup, SyncState::Errored)
                | (SyncState::Errored, SyncState::Snapshot)  // resync from error
                | (SyncState::Errored, SyncState::Streaming) // auto-retry
        )
    }
}

impl fmt::Display for SyncState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
