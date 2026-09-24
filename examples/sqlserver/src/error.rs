use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractError {
    MissingField {
        field: &'static str,
    },
    InvalidLength {
        field: &'static str,
        max: usize,
    },
    InvalidCharacter {
        field: &'static str,
    },
    InvalidGuid {
        field: &'static str,
    },
    NilGuid {
        field: &'static str,
    },
    MissingNativeIdentity {
        field: &'static str,
    },
    UnexpectedNativeIdentity {
        field: &'static str,
    },
    InvalidProgress,
    InvalidEndpointHost,
    InvalidPort,
    InvalidImageDigest,
    InvalidSecretReference {
        field: &'static str,
    },
    UnsupportedProfile {
        field: &'static str,
        expected: &'static str,
        actual: String,
    },
    DuplicateValue {
        field: &'static str,
        value: String,
    },
    EpochRegression {
        source: u64,
        target: u64,
    },
    EpochNotAdvanced {
        source: u64,
        target: u64,
    },
    MissingDestructiveApproval,
    UnexpectedDestructiveApproval,
    ApprovalOperationMismatch,
    ApprovalInputMismatch,
    MissingFence,
    UnexpectedFence,
    FenceOperationMismatch,
    FenceInputMismatch,
    FenceTargetMismatch,
    OperationIdReuse,
}

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(f, "{field} must not be empty"),
            Self::InvalidLength { field, max } => {
                write!(f, "{field} exceeds the maximum length of {max}")
            }
            Self::InvalidCharacter { field } => {
                write!(f, "{field} contains an unsupported character")
            }
            Self::InvalidGuid { field } => write!(f, "{field} is not a canonical GUID"),
            Self::NilGuid { field } => write!(f, "{field} must not be the nil GUID"),
            Self::MissingNativeIdentity { field } => {
                write!(f, "{field} requires an observed native replica GUID")
            }
            Self::UnexpectedNativeIdentity { field } => {
                write!(
                    f,
                    "{field} must not depend on a generated native replica GUID"
                )
            }
            Self::InvalidProgress => {
                write!(
                    f,
                    "SQL Server progress must be a 1-25 digit unsigned decimal"
                )
            }
            Self::InvalidEndpointHost => write!(f, "endpoint host must be a valid DNS name"),
            Self::InvalidPort => write!(f, "endpoint port must be nonzero"),
            Self::InvalidImageDigest => {
                write!(f, "SQL Server image must be pinned with a sha256 digest")
            }
            Self::InvalidSecretReference { field } => {
                write!(f, "{field} is not a valid Kubernetes Secret reference")
            }
            Self::UnsupportedProfile {
                field,
                expected,
                actual,
            } => write!(
                f,
                "unsupported {field}: expected {expected}, observed {actual}"
            ),
            Self::DuplicateValue { field, value } => {
                write!(f, "duplicate {field}: {value}")
            }
            Self::EpochRegression { source, target } => write!(
                f,
                "target epoch {target} is older than source epoch {source}"
            ),
            Self::EpochNotAdvanced { source, target } => write!(
                f,
                "authority-changing operation must advance epoch beyond {source}, got {target}"
            ),
            Self::MissingDestructiveApproval => {
                write!(f, "operation requires explicit destructive approval")
            }
            Self::UnexpectedDestructiveApproval => {
                write!(
                    f,
                    "non-destructive operation must not carry destructive approval"
                )
            }
            Self::ApprovalOperationMismatch => {
                write!(f, "destructive approval is bound to a different operation")
            }
            Self::ApprovalInputMismatch => {
                write!(
                    f,
                    "destructive approval is bound to different canonical input"
                )
            }
            Self::MissingFence => write!(f, "operation requires a verified fence reference"),
            Self::UnexpectedFence => {
                write!(f, "operation must not carry an unrelated fence reference")
            }
            Self::FenceOperationMismatch => {
                write!(f, "fence reference is bound to a different operation")
            }
            Self::FenceInputMismatch => {
                write!(f, "fence reference is bound to different canonical input")
            }
            Self::FenceTargetMismatch => {
                write!(f, "fence reference does not identify the required replica")
            }
            Self::OperationIdReuse => {
                write!(f, "operation ID was reused with different canonical input")
            }
        }
    }
}

impl Error for ContractError {}
