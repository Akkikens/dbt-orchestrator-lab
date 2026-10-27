//! Structured logging via `tracing`. JSON lines by default (`LOG_FORMAT=pretty`
//! for humans). With the `otel` feature and `OTEL_EXPORTER_OTLP_ENDPOINT` set,
//! spans are also exported over OTLP/HTTP, e.g. to an OpenTelemetry Collector or
//! a Datadog Agent with OTLP ingest enabled. The exporter path compiles and is
//! linted in CI, but no collector is exercised in tests.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer, Registry};

pub struct Guard {
    #[cfg(feature = "otel")]
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(p) = self.provider.take() {
            let _ = p.shutdown();
        }
    }
}

pub fn init(service: &str) -> Guard {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,rdkafka=warn"));
    let fmt_layer: Box<dyn Layer<Registry> + Send + Sync> =
        if std::env::var("LOG_FORMAT").as_deref() == Ok("pretty") {
            fmt::layer().boxed()
        } else {
            fmt::layer().json().with_current_span(true).boxed()
        };

    #[cfg(feature = "otel")]
    {
        let provider = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .and_then(|_| otel_provider(service));
        let otel_layer = provider.as_ref().map(|p| {
            use opentelemetry::trace::TracerProvider as _;
            tracing_opentelemetry::layer().with_tracer(p.tracer("orchestrator"))
        });
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel_layer)
            .with(filter)
            .init();
        Guard { provider }
    }
    #[cfg(not(feature = "otel"))]
    {
        let _ = service;
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(filter)
            .init();
        Guard {}
    }
}

#[cfg(feature = "otel")]
fn otel_provider(service: &str) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    // endpoint, headers and timeout come from the standard OTEL_EXPORTER_OTLP_* env vars
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("OTLP exporter disabled: {e}");
            return None;
        }
    };
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service.to_string())
        .build();
    Some(
        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    )
}
