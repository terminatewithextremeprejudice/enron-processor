# syntax=docker/dockerfile:1
#
# One image per microservice binary. The SERVICE build arg selects which
# package under cmd/ to compile, so all three services share this Dockerfile:
#
#   docker build --build-arg SERVICE=producer -t enron-processor/producer .
#   docker build --build-arg SERVICE=parser   -t enron-processor/parser   .
#   docker build --build-arg SERVICE=api      -t enron-processor/api      .

ARG GO_VERSION=1.25

# ---- [1/3] protobuf code generation ------------------------------------
# Generated inside the image build so the pipeline never depends on the
# host's generator toolchain. Output lands in /repo/gen.
FROM golang:${GO_VERSION}-bookworm AS protoc
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /repo
COPY proto/ ./proto/
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 \
 && MOD="github.com/terminatewithextremeprejudice/enron-processor/gen" \
 && mkdir -p gen \
 && for f in proto/enron/v1/*.proto; do \
      protoc --proto_path=proto \
        --go_out=gen      --go_opt=module=$MOD \
        --go-grpc_out=gen --go-grpc_opt=module=$MOD \
        "$f"; \
    done

# ---- [2/3] build the Go service ------------------------------------------
FROM golang:${GO_VERSION}-bookworm AS build
ARG SERVICE=api
WORKDIR /src
# Module metadata first for layer-cached dependency downloads.
COPY go.mod go.sum ./
RUN go mod download
# Whole repo (.dockerignore excludes dataset/binaries/generated code).
COPY . .
# Freshly generated protobuf code wins over any committed copy.
COPY --from=protoc /repo/gen ./gen
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/app ./cmd/${SERVICE}

# ---- [3/3] runtime --------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=nonroot:nonroot /out/app /app
USER nonroot
ENTRYPOINT ["/app"]
