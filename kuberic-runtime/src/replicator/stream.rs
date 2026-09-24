use bytes::Bytes;
use kuberic_protocol::types::OperationId;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::application::{DurableApplicationProgress, Lsn, Operation};
use crate::{Result, RuntimeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationMetadata {
    Replication {
        lsn: Lsn,
        committed_lsn: Lsn,
    },
    Copy {
        build_id: OperationId,
        sequence: u64,
    },
    CopyComplete {
        build_id: OperationId,
        up_to_lsn: Lsn,
        committed_lsn: Lsn,
    },
}

/// Acknowledge only after the operation and its progress are durably accepted.
/// Dropping an operation is not an ACK and fails the waiting data-plane request.
pub struct StreamOperation {
    pub metadata: OperationMetadata,
    pub data: Bytes,
    completion: oneshot::Sender<Result<DurableApplicationProgress>>,
}

pub(crate) struct OperationCompletion {
    receiver: oneshot::Receiver<Result<DurableApplicationProgress>>,
    closed: watch::Receiver<bool>,
}

impl OperationCompletion {
    pub(crate) async fn completed(mut self) -> Result<DurableApplicationProgress> {
        tokio::select! {
            biased;
            _ = self.closed.changed() => Err(RuntimeError::Closed),
            result = self.receiver => result.map_err(|_| RuntimeError::WriteCompletionClosed)?,
        }
    }
}

impl StreamOperation {
    pub fn acknowledge(self, progress: DurableApplicationProgress) -> Result<()> {
        self.completion
            .send(Ok(progress))
            .map_err(|_| RuntimeError::Closed)
    }

    pub fn reject(self, error: RuntimeError) -> Result<()> {
        self.completion
            .send(Err(error))
            .map_err(|_| RuntimeError::Closed)
    }
}

pub struct OperationStream {
    receiver: mpsc::Receiver<StreamOperation>,
    closed: watch::Receiver<bool>,
}

impl OperationStream {
    pub fn channel(capacity: usize) -> (OperationSender, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        let (closed, closed_rx) = watch::channel(false);
        (
            OperationSender { sender, closed },
            Self {
                receiver,
                closed: closed_rx,
            },
        )
    }

    pub async fn get_operation(&mut self) -> Result<Option<StreamOperation>> {
        if *self.closed.borrow() {
            return Ok(None);
        }
        tokio::select! {
            biased;
            _ = self.closed.changed() => Ok(None),
            operation = self.receiver.recv() => Ok(operation),
        }
    }
}

/// Non-COM producer side for replication engines implementing service delivery.
#[derive(Clone)]
pub struct OperationSender {
    sender: mpsc::Sender<StreamOperation>,
    closed: watch::Sender<bool>,
}

impl OperationSender {
    pub fn close(&self) {
        self.closed.send_replace(true);
    }

    pub(crate) async fn enqueue(
        &self,
        metadata: OperationMetadata,
        data: Bytes,
    ) -> Result<OperationCompletion> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow() {
            return Err(RuntimeError::Closed);
        }
        let (completion, receiver) = oneshot::channel();
        tokio::select! {
            biased;
            _ = closed.changed() => Err(RuntimeError::Closed),
            result = self.sender.send(StreamOperation { metadata, data, completion }) => {
                result.map_err(|_| RuntimeError::Closed)?;
                Ok(OperationCompletion { receiver, closed })
            },
        }
    }

    pub async fn send(
        &self,
        metadata: OperationMetadata,
        data: Bytes,
    ) -> Result<DurableApplicationProgress> {
        self.enqueue(metadata, data).await?.completed().await
    }
}

pub(crate) struct ServiceStreams {
    replication_tx: OperationSender,
    copy_tx: OperationSender,
    replication: Mutex<Option<OperationStream>>,
    copy: Mutex<Option<OperationStream>>,
}

impl ServiceStreams {
    pub(crate) fn new() -> Self {
        let (replication_tx, replication) = OperationStream::channel(64);
        let (copy_tx, copy) = OperationStream::channel(64);
        Self {
            replication_tx,
            copy_tx,
            replication: Mutex::new(Some(replication)),
            copy: Mutex::new(Some(copy)),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.replication_tx.close();
        self.copy_tx.close();
    }

    pub(crate) async fn take_replication(&self) -> Result<OperationStream> {
        self.replication
            .lock()
            .await
            .take()
            .ok_or_else(|| RuntimeError::Application("replication stream already taken".into()))
    }

    pub(crate) async fn take_copy(&self) -> Result<OperationStream> {
        self.copy
            .lock()
            .await
            .take()
            .ok_or_else(|| RuntimeError::Application("copy stream already taken".into()))
    }

    pub(crate) async fn replication(
        &self,
        operation: Operation,
    ) -> Result<DurableApplicationProgress> {
        self.replication_tx
            .send(
                OperationMetadata::Replication {
                    lsn: operation.lsn,
                    committed_lsn: operation.committed_lsn,
                },
                operation.data,
            )
            .await
    }

    pub(crate) async fn enqueue_replication(
        &self,
        operation: Operation,
    ) -> Result<OperationCompletion> {
        self.replication_tx
            .enqueue(
                OperationMetadata::Replication {
                    lsn: operation.lsn,
                    committed_lsn: operation.committed_lsn,
                },
                operation.data,
            )
            .await
    }

    pub(crate) async fn copy(
        &self,
        metadata: OperationMetadata,
        data: Bytes,
    ) -> Result<DurableApplicationProgress> {
        self.copy_tx.send(metadata, data).await
    }
}
