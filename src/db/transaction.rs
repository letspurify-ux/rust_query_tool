use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionIsolation {
    #[default]
    Default,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionAccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionMode {
    pub isolation: TransactionIsolation,
    pub access_mode: TransactionAccessMode,
}

impl TransactionIsolation {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "Default",
            Self::ReadUncommitted => "Read uncommitted",
            Self::ReadCommitted => "Read committed",
            Self::RepeatableRead => "Repeatable read",
            Self::Serializable => "Serializable",
        }
    }

    pub(crate) fn sql_level(self) -> Option<&'static str> {
        match self {
            Self::Default => None,
            Self::ReadUncommitted => Some("READ UNCOMMITTED"),
            Self::ReadCommitted => Some("READ COMMITTED"),
            Self::RepeatableRead => Some("REPEATABLE READ"),
            Self::Serializable => Some("SERIALIZABLE"),
        }
    }

    pub(crate) fn from_sql_level(value: &str) -> Option<Self> {
        let normalized = value
            .trim()
            .replace(['-', '_'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();

        match normalized.as_str() {
            "READ UNCOMMITTED" => Some(Self::ReadUncommitted),
            "READ COMMITED" => Some(Self::ReadCommitted),
            "READ COMMITTED" => Some(Self::ReadCommitted),
            "REPEATABLE READ" => Some(Self::RepeatableRead),
            "SERIALIZABLE" => Some(Self::Serializable),
            _ => None,
        }
    }
}

impl TransactionAccessMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::ReadWrite => "Read write",
            Self::ReadOnly => "Read only",
        }
    }

    pub(crate) fn sql_clause(self) -> &'static str {
        match self {
            Self::ReadWrite => "READ WRITE",
            Self::ReadOnly => "READ ONLY",
        }
    }
}

impl TransactionMode {
    pub fn new(isolation: TransactionIsolation, access_mode: TransactionAccessMode) -> Self {
        Self {
            isolation,
            access_mode,
        }
    }

    pub fn is_default(self) -> bool {
        self == Self::default()
    }

    pub fn label(self) -> String {
        format!("{}, {}", self.isolation.label(), self.access_mode.label())
    }
}
