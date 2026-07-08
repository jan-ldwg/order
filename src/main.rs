use std::{env, fs::File, io::Write, time::Duration};

use async_graphql::{
    extensions::Logger, http::GraphiQLSource, EmptySubscription, SDLExportOptions, Schema,
};

use async_graphql_axum::{GraphQLRequest, GraphQLResponse};

use axum::{
    extract::State,
    http::{header::HeaderMap, StatusCode},
    response::{self, IntoResponse},
    routing::{get, post},
    Router,
};

use once_cell::sync::Lazy;
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_otel_metrics::HttpMetricsLayer;

use opentelemetry::global;
use opentelemetry_sdk::trace as sdktrace;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};

use tracing::{info, instrument};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

use clap::Parser;

use mongodb::{options::ClientOptions, Client, Database};

mod authorization;
use authorization::AuthorizedUserHeader;

mod event;
mod graphql;

use event::{
    http_event_service::{
        list_topic_subscriptions, on_id_creation_event, on_product_variant_update_event,
        on_product_variant_version_creation_event, on_shipment_creation_failed_event,
        on_tax_rate_version_creation_event, on_user_address_archived_event,
        on_user_address_creation_event, HttpEventServiceState,
    },
    order_compensation::OrderCompensation,
};
use graphql::{
    model::{
        foreign_types::{Coupon, ProductVariant, ShipmentMethod, TaxRate},
        order::Order,
        user::User,
    },
    mutation::Mutation,
    query::Query,
};

/// Builds the GraphiQL frontend.
async fn graphiql() -> impl IntoResponse {
    response::Html(GraphiQLSource::build().endpoint("/").finish())
}

/// Establishes database connection and returns the client.
async fn db_connection() -> Client {
    let uri = match env::var_os("MONGODB_URI") {
        Some(uri) => uri.into_string().unwrap(),
        None => panic!("$MONGODB_URI is not set."),
    };

    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse(uri).await.unwrap();

    // Manually set an option.
    client_options.app_name = Some("Order".to_string());
    client_options.max_pool_size = Some(100);
    client_options.max_connecting = Some(3);
    client_options.min_pool_size = Some(1);
    client_options.max_idle_time = Some(Duration::new(90, 0));

    // Get a handle to the deployment.
    Client::with_options(client_options).unwrap()
}

/// Returns Router that establishes connection to Dapr.
///
/// Creates endpoints to define pub/sub interaction with Dapr.
///
/// * `db_client` - MongoDB database client.
async fn build_dapr_router(db_client: Database) -> Router {
    let product_variant_collection: mongodb::Collection<ProductVariant> =
        db_client.collection::<ProductVariant>("product_variants");
    let coupon_collection: mongodb::Collection<Coupon> = db_client.collection::<Coupon>("coupons");
    let tax_rate_collection: mongodb::Collection<TaxRate> =
        db_client.collection::<TaxRate>("tax_rates");
    let shipment_method_collection: mongodb::Collection<ShipmentMethod> =
        db_client.collection::<ShipmentMethod>("shipment_methods");
    let user_collection: mongodb::Collection<User> = db_client.collection::<User>("users");
    let order_collection: mongodb::Collection<Order> = db_client.collection::<Order>("orders");
    let order_compensation_collection: mongodb::Collection<OrderCompensation> =
        db_client.collection::<OrderCompensation>("order_compensations");

    // Define routes.
    let app = Router::new()
        .route("/dapr/subscribe", get(list_topic_subscriptions))
        .route("/on-id-creation-event", post(on_id_creation_event))
        .route(
            "/on-product-variant-version-creation-event",
            post(on_product_variant_version_creation_event),
        )
        .route(
            "/on-product-variant-updated-event",
            post(on_product_variant_update_event),
        )
        .route(
            "/on-tax-rate-version-creation-event",
            post(on_tax_rate_version_creation_event),
        )
        .route(
            "/on-user-address-creation-event",
            post(on_user_address_creation_event),
        )
        .route(
            "/on-user-address-archived-event",
            post(on_user_address_archived_event),
        )
        .route(
            "/on-shipment-creation-failed-event",
            post(on_shipment_creation_failed_event),
        )
        .with_state(HttpEventServiceState {
            product_variant_collection,
            coupon_collection,
            tax_rate_collection,
            shipment_method_collection,
            user_collection,
            order_collection,
            order_compensation_collection,
        });
    app
}

/// Command line argument to toggle schema generation instead of service execution.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Generates GraphQL schema in `./schemas/order.graphql`.
    #[arg(long)]
    generate_schema: bool,
}

/// Activates logger and parses argument for optional schema generation. Otherwise starts gRPC and GraphQL server.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let args = Args::parse();
    if args.generate_schema {
        let schema = Schema::build(Query, Mutation, EmptySubscription).finish();
        let mut file = File::create("./schemas/order.graphql")?;
        let sdl_export_options = SDLExportOptions::new().federation();
        let schema_sdl = schema.sdl_with_options(sdl_export_options);
        file.write_all(schema_sdl.as_bytes())?;
        info!("GraphQL schema: ./schemas/order.graphql was successfully generated!");
    } else {
        start_service().await;
    }
    Ok(())
}

/// Describes the handler for GraphQL requests.
///
/// Parses the "Authenticate-User" header and writes it in the context data of the specfic request.
/// Then executes the GraphQL schema with the request.
///
/// * `schema` - GraphQL schema used by handler.
/// * `headers` - Header map containing headers of request.
/// * `request` - GraphQL request.
#[instrument(skip(schema, headers, req))]
async fn graphql_handler(
    State(schema): State<Schema<Query, Mutation, EmptySubscription>>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut req = req.into_inner();
    if let Ok(authenticate_user_header) = AuthorizedUserHeader::try_from(&headers) {
        req = req.data(authenticate_user_header);
    }
    schema.execute(req).await.into()
}

static RESOURCE: Lazy<Resource> = Lazy::new(|| {
    Resource::builder()
        .with_service_name("order")
        .build()
});

/// Initializes OpenTelemetry metrics exporter and sets the global meter provider.
fn init_otlp() -> HttpMetricsLayer {
    let otlp_url = match env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Some(uri) => uri.into_string().unwrap(),
        None => "http://localhost:4318".to_string(),
    };
    
    let otlp_endpoint = format!("{}/v1/metrics", otlp_url.trim_end_matches('/'));
    
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(otlp_endpoint)
        .with_temporality(Temporality::default())
        .build()
        .unwrap();

    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(5))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(RESOURCE.clone())
        .build();

    global::set_meter_provider(provider.clone());
    // register global provider and build metrics layer using the global provider
    HttpMetricsLayerBuilder::new().build()
}

fn init_tracing() {
    // Do not initialize `LogTracer` here; it can conflict with setting a global tracing subscriber.
    let otlp_url = match env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Some(uri) => uri.into_string().unwrap(),
        None => "http://localhost:4318".to_string(),
    };

    let otlp_endpoint = format!("{}/v1/traces", otlp_url.trim_end_matches('/'));

    let span_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(otlp_endpoint)
        .build()
        .expect("Failed to create OTLP span exporter");

    let tracer_provider = sdktrace::SdkTracerProvider::builder()
        .with_simple_exporter(span_exporter)
        .build();

    global::set_tracer_provider(tracer_provider);

    let tracer = global::tracer("order");

    let telemetry_layer = OpenTelemetryLayer::new(tracer);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_layer)
        .init();
}

/// Starts order service on port 8000.
async fn start_service() {
    let client = db_connection().await;
    let db_client: Database = client.database("order-database");

    let schema = Schema::build(Query, Mutation, EmptySubscription)
        .extension(Logger)
        .data(db_client.clone())
        .enable_federation()
        .finish();

    let graphiql = Router::new()
        .route("/", get(graphiql).post(graphql_handler))
        .route("/health", get(StatusCode::OK))
        .with_state(schema);
    let dapr_router = build_dapr_router(db_client).await;

    let metrics = init_otlp();
    
    let app = Router::new()
        .merge(graphiql)
        .merge(dapr_router)
        .layer(metrics);

    info!("GraphiQL IDE: http://0.0.0.0:8080");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app)
        .await
        .unwrap();
}
