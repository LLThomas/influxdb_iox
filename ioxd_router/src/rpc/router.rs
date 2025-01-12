use std::sync::Arc;

use tonic::{Request, Response, Status};

use generated_types::google::PreconditionViolation;
use generated_types::{
    google::{FromOptionalField, NotFound, ResourceType},
    influxdata::iox::router::v1::*,
};
use router::server::RouterServer;

struct RouterService {
    server: Arc<RouterServer>,
    config_immutable: bool,
}

#[tonic::async_trait]
impl router_service_server::RouterService for RouterService {
    async fn get_router(
        &self,
        request: Request<GetRouterRequest>,
    ) -> Result<Response<GetRouterResponse>, Status> {
        let GetRouterRequest { router_name } = request.into_inner();
        let router = self
            .server
            .router(&router_name)
            .ok_or_else(|| NotFound::new(ResourceType::Router, router_name))?;
        Ok(Response::new(GetRouterResponse {
            router: Some(router.config().clone().into()),
        }))
    }

    async fn list_routers(
        &self,
        _: Request<ListRoutersRequest>,
    ) -> Result<Response<ListRoutersResponse>, Status> {
        Ok(Response::new(ListRoutersResponse {
            routers: self
                .server
                .routers()
                .into_iter()
                .map(|router| router.config().clone().into())
                .collect(),
        }))
    }

    async fn update_router(
        &self,
        request: Request<UpdateRouterRequest>,
    ) -> Result<Response<UpdateRouterResponse>, Status> {
        if self.config_immutable {
            return Err(PreconditionViolation::RouterConfigImmutable.into());
        }

        use data_types::router::Router as RouterConfig;

        let UpdateRouterRequest { router } = request.into_inner();
        let cfg: RouterConfig = router.required("router")?;
        self.server.update_router(cfg);
        Ok(Response::new(UpdateRouterResponse {}))
    }

    async fn delete_router(
        &self,
        request: Request<DeleteRouterRequest>,
    ) -> Result<Response<DeleteRouterResponse>, Status> {
        if self.config_immutable {
            return Err(PreconditionViolation::RouterConfigImmutable.into());
        }

        let DeleteRouterRequest { router_name } = request.into_inner();
        self.server.delete_router(&router_name);
        Ok(Response::new(DeleteRouterResponse {}))
    }
}

pub fn make_server(
    server: Arc<RouterServer>,
    config_immutable: bool,
) -> router_service_server::RouterServiceServer<impl router_service_server::RouterService> {
    router_service_server::RouterServiceServer::new(RouterService {
        server,
        config_immutable,
    })
}
