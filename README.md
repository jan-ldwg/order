# Order service for MiSArch

### Quickstart (DevContainer)

1. Open VSCode Development Container
2. `cargo run` starts the GraphQL service + GraphiQL on port `8080`

### Quickstart (Docker Compose)

1. `docker compose -f docker-compose-dev.yaml up --build` in the repository root directory. **IMPORTANT:** MongoDB credentials should be configured for production.

### What it can do

- CRUD orders
- Validates all UUIDs input as strings
- Error prop to GraphQL

### Build

```sh
docker buildx build \
  --platform linux/amd64 \
  -f base-dockerfile \
  -t ghcr.io/jan-ldwg/order:latest \
  --push \
  .
```
