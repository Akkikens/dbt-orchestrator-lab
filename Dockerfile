# Build the orchestrator, then run it next to a dbt-duckdb install.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
COPY migrations migrations
RUN cargo build --release -p orchestrator

FROM python:3.12-slim-bookworm
RUN pip install --no-cache-dir dbt-core==1.12.6 dbt-duckdb==1.11.1 \
 && useradd --create-home --uid 10001 orchestrator
COPY --from=build /src/target/release/orchestrator /usr/local/bin/orchestrator
WORKDIR /app
COPY jobs.toml ./
COPY examples ./examples
RUN chown -R orchestrator /app
USER orchestrator
ENV DBT_BIN=dbt \
    DBT_PROJECT_DIR=/app/examples/jaffle \
    JOBS_FILE=/app/jobs.toml \
    WORK_DIR=/tmp/dbt-orchestrator \
    DBT_DUCKDB_PATH=/tmp/dbt-orchestrator/jaffle.duckdb
EXPOSE 8080
HEALTHCHECK --interval=3s --timeout=3s --retries=40 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz')"
ENTRYPOINT ["orchestrator"]
CMD ["all"]
