use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
use crate::protocol::types::ConfigurationDescriptor;
use crate::protocol::types::{OperationId, ReplicaIdentity};
use crate::transport::CopyItem;
use futures::Stream;
use tokio::sync::{mpsc, watch};

use crate::Result;
use crate::application::OperationDataStream;
#[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
use crate::authority::BuildAuthority;
use crate::authority::DurableBuildProgress;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BuildConfiguration {
    Current,
    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    Bootstrap(ConfigurationDescriptor),
}

pub(crate) struct PrepareCopyRequest {
    pub(crate) build_id: OperationId,
    pub(crate) target: ReplicaIdentity,
    pub(crate) configuration: BuildConfiguration,
    pub(crate) copy_context: OperationDataStream,
}

pub(crate) struct PreparedCopy {
    #[cfg(any(all(test, kuberic_workspace_tests), feature = "testing"))]
    pub(crate) authority: BuildAuthority,
    pub(crate) items: Pin<Box<dyn Stream<Item = Result<CopyItem>> + Send>>,
}

pub(crate) struct CopyItemStream {
    receiver: mpsc::Receiver<Result<CopyItem>>,
    cancellation: watch::Sender<bool>,
}

impl CopyItemStream {
    pub(crate) fn new(
        receiver: mpsc::Receiver<Result<CopyItem>>,
        cancellation: watch::Sender<bool>,
    ) -> Self {
        Self {
            receiver,
            cancellation,
        }
    }
}

impl Stream for CopyItemStream {
    type Item = Result<CopyItem>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(context)
    }
}

impl Drop for CopyItemStream {
    fn drop(&mut self) {
        self.cancellation.send_replace(true);
    }
}

pub(crate) type BuildProgress = DurableBuildProgress;
