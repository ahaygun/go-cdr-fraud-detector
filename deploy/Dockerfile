# syntax=docker/dockerfile:1
#
# Shared multi-stage build for every service. Pick the service with:
#   docker build --build-arg SERVICE=fraud -f deploy/Dockerfile .
# docker-compose passes SERVICE per service.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependency layer (cached until go.mod changes).
COPY go.mod ./
RUN go mod download

# Source + build a single static binary for the chosen service.
COPY . .
ARG SERVICE
RUN test -n "$SERVICE" || (echo "SERVICE build-arg is required" && exit 1)
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./services/${SERVICE}

# Minimal, non-root runtime.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
