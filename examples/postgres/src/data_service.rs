use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::build::{MAX_ENVELOPE_BYTES, PgBuildRequest, decode, encode};
use crate::proto::pg_data_service_server::{PgDataService, PgDataServiceServer};
use crate::proto::{NativeBuildRequest, NativeBuildResponse, NativeSourceResponse};
use crate::service::PgService;

pub(crate) struct PgCoordination {
    pub local_endpoint: String,
    pub bearer_token: String,
}

pub struct PgDataServiceImpl {
    service: Arc<PgService>,
    token: String,
}

impl PgDataServiceImpl {
    pub fn new(service: Arc<PgService>, token: String) -> Self {
        Self { service, token }
    }

    pub fn into_server(self) -> PgDataServiceServer<Self> {
        PgDataServiceServer::new(self)
            .max_decoding_message_size(MAX_ENVELOPE_BYTES + 16)
            .max_encoding_message_size(MAX_ENVELOPE_BYTES + 16)
    }

    fn request(&self, request: Request<NativeBuildRequest>) -> Result<PgBuildRequest, Status> {
        if self.token.is_empty()
            || request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                != Some(format!("Bearer {}", self.token).as_str())
        {
            return Err(Status::unauthenticated(
                "invalid native coordination credentials",
            ));
        }
        let request: PgBuildRequest =
            decode(&request.into_inner().envelope_json).map_err(Status::invalid_argument)?;
        request.validate().map_err(Status::invalid_argument)?;
        Ok(request)
    }
}

#[tonic::async_trait]
impl PgDataService for PgDataServiceImpl {
    async fn recover(
        &self,
        request: Request<crate::proto::RecoveryRequest>,
    ) -> Result<Response<crate::proto::RecoveryResponse>, Status> {
        if self.token.is_empty()
            || request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some(format!("Bearer {}", self.token).as_str())
        {
            return Err(Status::unauthenticated(
                "invalid PostgreSQL coordination credentials",
            ));
        }
        let request =
            decode(&request.into_inner().envelope_json).map_err(Status::invalid_argument)?;
        let observation = self
            .service
            .driver()?
            .recovery_request(request)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(crate::proto::RecoveryResponse {
            envelope_json: encode(&observation).map_err(Status::internal)?,
        }))
    }

    async fn build(
        &self,
        request: Request<NativeBuildRequest>,
    ) -> Result<Response<NativeBuildResponse>, Status> {
        let request = self.request(request)?;
        let progress = self
            .service
            .driver()?
            .receive_build(request)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(NativeBuildResponse {
            progress_json: encode(&progress).map_err(Status::internal)?,
        }))
    }

    async fn inspect_source(
        &self,
        request: Request<NativeBuildRequest>,
    ) -> Result<Response<NativeSourceResponse>, Status> {
        let request = self.request(request)?;
        let lineage = self
            .service
            .driver()?
            .inspect_source(&request)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(NativeSourceResponse {
            lineage_json: encode(&lineage).map_err(Status::internal)?,
        }))
    }
}
