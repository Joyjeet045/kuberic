use crate::{
    ActivityRegistry, ActivityRunner, CheckpointStore, DurableHost, ExecutionSpec, HostOutcome,
    TerminalOutcome, Workflow,
};

/// Controller-style result of one mocked Kubernetes reconciliation.
#[derive(Debug, Eq, PartialEq)]
pub enum MockReconcileAction {
    RequeueNow,
    RequeueAt(i64),
    AwaitChange,
    Complete(TerminalOutcome),
    Failed(Box<HostOutcome>),
}

/// Small runtime-neutral harness that maps DEX runner outcomes to the actions
/// a Kubernetes reconciler would return.
pub struct MockReconciler<S> {
    runner: ActivityRunner<S>,
    now_unix_millis: i64,
}

impl<S: CheckpointStore> MockReconciler<S> {
    pub fn new(host: DurableHost<S>, activities: ActivityRegistry, now_unix_millis: i64) -> Self {
        Self {
            runner: ActivityRunner::new(host, activities),
            now_unix_millis,
        }
    }

    pub const fn now_unix_millis(&self) -> i64 {
        self.now_unix_millis
    }

    pub fn advance_to(&mut self, now_unix_millis: i64) {
        self.now_unix_millis = now_unix_millis;
    }

    pub async fn reconcile<W: Workflow + ?Sized>(
        &mut self,
        workflow: &W,
        execution: ExecutionSpec,
    ) -> MockReconcileAction {
        match self
            .runner
            .run_once(workflow, execution, self.now_unix_millis)
            .await
        {
            HostOutcome::ScheduleAccepted { .. }
            | HostOutcome::ObservationAccepted { .. }
            | HostOutcome::ReloadRequired { .. } => MockReconcileAction::RequeueNow,
            HostOutcome::RetryScheduled {
                retry_not_before_unix_millis,
                ..
            } => MockReconcileAction::RequeueAt(retry_not_before_unix_millis),
            HostOutcome::Waiting {
                wake_at_unix_millis,
                ..
            } => MockReconcileAction::RequeueAt(wake_at_unix_millis),
            HostOutcome::Quarantined { .. } => MockReconcileAction::AwaitChange,
            HostOutcome::WorkflowCompleted { outcome, .. } => {
                MockReconcileAction::Complete(outcome)
            }
            failure => MockReconcileAction::Failed(Box::new(failure)),
        }
    }
}
