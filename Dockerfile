# Multi-stage build for distributed key-value store
FROM rust:1.70 as builder

WORKDIR /app

# Copy manifest files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY examples ./examples
COPY tests ./tests

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -r -s /bin/false appuser

# Create app directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/target/release/distributed-kv-store /app/distributed-kv-store

# Create data directories
RUN mkdir -p /app/wal /app/data && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose ports
EXPOSE 8000 9000

# Default command
CMD ["./distributed-kv-store", "server", "--node-id", "1", "--address", "0.0.0.0", "--port", "8000", "--http-port", "9000"]
