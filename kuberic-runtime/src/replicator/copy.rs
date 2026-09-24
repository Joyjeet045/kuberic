use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use kuberic_protocol::types::{ConfigurationDescriptor, OperationId, ReplicaIdentity};
use kuberic_runtime_internal::transport::CopyItem;
use tokio::sync::{mpsc, watch};

use crate::Result;
use crate::application::OperationDataStream;
use crate::authority::{BuildAuthority, DurableBuildProgress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildConfiguration {
    Current,
    Bootstrap(ConfigurationDescriptor),
}

pub struct PrepareCopyRequest {
    pub build_id: OperationId,
    pub target: ReplicaIdentity,
    pub configuration: BuildConfiguration,
    pub copy_context: OperationDataStream,
}

pub struct PreparedCopy {
    pub authority: BuildAuthority,
    pub items: Pin<Box<dyn Stream<Item = Result<CopyItem>> + Send>>,
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

pub type BuildProgress = DurableBuildProgress;
