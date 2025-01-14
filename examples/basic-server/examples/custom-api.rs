//! Shows basic setup of a custom api server.
//!
//! This example has a section in the User Guide: https://dev.bonsaidb.io/main/guide/about/access-models/custom-api-server.html

use std::time::Duration;

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        actionable::{Actionable, Dispatcher, Permissions},
        async_trait::async_trait,
        connection::{Authentication, SensitiveString, StorageConnection},
        custom_api::{CustomApi, Infallible},
        permissions::{
            bonsai::{AuthenticationMethod, BonsaiAction, ServerAction},
            Action, ResourceName, Statement,
        },
    },
    local::config::Builder,
    server::{
        Backend, BackendError, ConnectedClient, CustomApiDispatcher, CustomServer,
        ServerConfiguration,
    },
};
use serde::{Deserialize, Serialize};

/// The `Backend` for the BonsaiDb server.
#[derive(Debug)]
pub struct ExampleBackend;

/// The `CustomApi` for this example.
#[derive(Debug)]
pub enum ExampleApi {}

// ANCHOR: api-types
#[derive(Serialize, Deserialize, Actionable, Debug)]
#[actionable(actionable = bonsaidb::core::actionable)]
pub enum Request {
    #[actionable(protection = "none")]
    Ping,
    #[actionable(protection = "simple")]
    DoSomethingSimple { some_argument: u32 },
    #[actionable(protection = "custom")]
    DoSomethingCustom { some_argument: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Pong,
    DidSomething,
}

impl CustomApi for ExampleApi {
    type Request = Request;
    type Response = Response;
    type Error = Infallible;
}
// ANCHOR_END: api-types

// ANCHOR: server-traits
impl Backend for ExampleBackend {
    type CustomApi = ExampleApi;
    type CustomApiDispatcher = ExampleDispatcher;
    type ClientData = ();
}

/// Dispatches Requests and returns Responses.
#[derive(Debug, Dispatcher)]
#[dispatcher(input = Request, actionable = bonsaidb::core::actionable)]
pub struct ExampleDispatcher {
    // While this example doesn't use the server reference, this is how a custom
    // API can gain access to the running server to perform database operations
    // within the handlers. The `ConnectedClient` can also be cloned and stored
    // in the dispatcher if handlers need to interact with clients outside of a
    // simple Request/Response exchange.
    _server: CustomServer<ExampleBackend>,
}

impl CustomApiDispatcher<ExampleBackend> for ExampleDispatcher {
    fn new(
        server: &CustomServer<ExampleBackend>,
        _client: &ConnectedClient<ExampleBackend>,
    ) -> Self {
        Self {
            _server: server.clone(),
        }
    }
}

#[async_trait]
impl RequestDispatcher for ExampleDispatcher {
    type Output = Response;
    type Error = BackendError<Infallible>;
}

/// The Request::Ping variant has `#[actionable(protection = "none")]`, which
/// causes `PingHandler` to be generated with a single method and no implicit
/// permission handling.
#[async_trait]
impl PingHandler for ExampleDispatcher {
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response, BackendError<Infallible>> {
        Ok(Response::Pong)
    }
}
// ANCHOR_END: server-traits

// ANCHOR: permission-handles
/// The permissible actions that can be granted for this example api.
#[derive(Debug, Action)]
#[action(actionable = bonsaidb::core::actionable)]
pub enum ExampleActions {
    DoSomethingSimple,
    DoSomethingCustom,
}

/// With `protection = "simple"`, `actionable` will generate a trait that allows
/// you to return a `ResourceName` and an `Action`, and the handler will
/// automatically confirm that the connected user has been granted the ability
/// to perform `Action` against `ResourceName`.
#[async_trait]
impl DoSomethingSimpleHandler for ExampleDispatcher {
    type Action = ExampleActions;

    async fn resource_name<'a>(
        &'a self,
        _some_argument: &'a u32,
    ) -> Result<ResourceName<'a>, BackendError<Infallible>> {
        Ok(ResourceName::named("example"))
    }

    fn action() -> Self::Action {
        ExampleActions::DoSomethingSimple
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        _some_argument: u32,
    ) -> Result<Response, BackendError<Infallible>> {
        // The permissions have already been checked.
        Ok(Response::DidSomething)
    }
}

/// With `protection = "custom"`, `actionable` will generate a trait with two
/// functions: one to verify the permissions are valid, and one to do the
/// protected action. This is useful if there are multiple actions or resource
/// names that need to be checked, or if permissions change based on the
/// arguments passed.
#[async_trait]
impl DoSomethingCustomHandler for ExampleDispatcher {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        some_argument: &u32,
    ) -> Result<(), BackendError<Infallible>> {
        if *some_argument == 42 {
            Ok(())
        } else {
            permissions.check(
                ResourceName::named("example"),
                &ExampleActions::DoSomethingCustom,
            )?;

            Ok(())
        }
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        _some_argument: u32,
    ) -> Result<Response, BackendError<Infallible>> {
        // `verify_permissions` has already been executed, so no permissions
        // logic needs to live here.
        Ok(Response::DidSomething)
    }
}
// ANCHOR_END: permission-handles

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    // ANCHOR: server-init
    let server = CustomServer::<ExampleBackend>::open(
        ServerConfiguration::new("custom-api.bonsaidb")
            .default_permissions(Permissions::from(
                Statement::for_any()
                    .allowing(&BonsaiAction::Server(ServerAction::Connect))
                    .allowing(&BonsaiAction::Server(ServerAction::Authenticate(
                        AuthenticationMethod::PasswordHash,
                    ))),
            ))
            .authenticated_permissions(Permissions::from(
                Statement::for_any()
                    .allowing(&ExampleActions::DoSomethingSimple)
                    .allowing(&ExampleActions::DoSomethingCustom),
            )),
    )
    .await?;
    // ANCHOR_END: server-init

    // Create a user to allow testing authenticated permissions
    match server.create_user("test-user").await {
        Ok(_) | Err(bonsaidb::core::Error::UniqueKeyViolation { .. }) => {}
        Err(other) => anyhow::bail!(other),
    }

    server
        .set_user_password("test-user", SensitiveString("hunter2".to_string()))
        .await?;

    if server.certificate_chain().await.is_err() {
        server.install_self_signed_certificate(true).await?;
    }
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();

    // If websockets are enabled, we'll also listen for websocket traffic.
    #[cfg(feature = "websockets")]
    {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .listen_for_websockets_on("localhost:8080", false)
                .await
        });
    }

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // To allow this example to run both websockets and QUIC, we're going to gather the clients
    // into a collection and use join_all to wait until they finish.
    let mut tasks = Vec::new();
    #[cfg(feature = "websockets")]
    {
        // To connect over websockets, use the websocket scheme.
        tasks.push(invoke_apis(
            Client::build(Url::parse("ws://localhost:8080")?)
                .with_custom_api()
                .finish()
                .await?,
            "websockets",
        ));
    }

    // To connect over QUIC, use the bonsaidb scheme.
    tasks.push(invoke_apis(
        Client::build(Url::parse("bonsaidb://localhost")?)
            .with_custom_api()
            .with_certificate(certificate)
            .finish()
            .await?,
        "bonsaidb",
    ));

    // Wait for the clients to finish
    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}
async fn invoke_apis(
    client: Client<ExampleApi>,
    client_name: &str,
) -> Result<(), bonsaidb::core::Error> {
    ping_the_server(&client, client_name).await?;

    // Calling DoSomethingSimple and DoSomethingCustom will check permissions, which our client currently doesn't have access to.
    assert!(matches!(
        client
            .send_api_request(Request::DoSomethingSimple { some_argument: 1 })
            .await,
        Err(bonsaidb::client::Error::Core(
            bonsaidb::core::Error::PermissionDenied(_)
        ))
    ));
    assert!(matches!(
        client
            .send_api_request(Request::DoSomethingCustom { some_argument: 1 })
            .await,
        Err(bonsaidb::client::Error::Core(
            bonsaidb::core::Error::PermissionDenied(_)
        ))
    ));
    // However, DoSomethingCustom with the argument `42` will succeed, because that argument has special logic in the handler.
    assert!(matches!(
        client
            .send_api_request(Request::DoSomethingCustom { some_argument: 42 })
            .await,
        Ok(Response::DidSomething)
    ));

    // Now, let's authenticate and try calling the APIs that previously were denied permissions
    client
        .authenticate(
            "test-user",
            Authentication::Password(SensitiveString(String::from("hunter2"))),
        )
        .await
        .unwrap();
    assert!(matches!(
        client
            .send_api_request(Request::DoSomethingSimple { some_argument: 1 })
            .await,
        Ok(Response::DidSomething)
    ));
    assert!(matches!(
        client
            .send_api_request(Request::DoSomethingCustom { some_argument: 1 })
            .await,
        Ok(Response::DidSomething)
    ));

    Ok(())
}

// ANCHOR: api-call
async fn ping_the_server(
    client: &Client<ExampleApi>,
    client_name: &str,
) -> Result<(), bonsaidb::core::Error> {
    match client.send_api_request(Request::Ping).await {
        Ok(Response::Pong) => {
            println!("Received Pong from server on {}", client_name);
        }
        other => println!(
            "Unexpected response from API call on {}: {:?}",
            client_name, other
        ),
    }

    Ok(())
}
// ANCHOR_END: api-call

#[test]
fn runs() {
    main().unwrap()
}
