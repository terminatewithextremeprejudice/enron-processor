SHELL := /bin/bash
.DEFAULT_GOAL := help

REPO     := enron-processor
PROTO_GO := github.com/terminatewithextremeprejudice/$(REPO)/gen

help: ## List available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

infra: ## Start infra only (Kafka 4.0 KRaft + Postgres 16)
	docker compose up -d kafka postgres
	docker compose ps

app: ## Build the four Go services and start the full stack
	docker compose --profile app up -d --build

logs: ## Tail all service logs
	docker compose --profile app logs -f

ps: ## Show stack status
	docker compose --profile app ps

psql: ## Open a psql shell against the service database
	docker compose exec postgres psql -U enron -d enron

fetch-dataset: ## Download and unpack the Enron dataset into ./data
	bash scripts/download_dataset.sh

proto: ## Regenerate the Go protobuf code into ./gen (needs protoc + the two plugins)
	@mkdir -p gen \
	&& for f in proto/enron/v1/*.proto; do \
		echo "protoc $$f"; \
		protoc --proto_path=proto \
			--go_out=gen     --go_opt=module=$(PROTO_GO) \
			--go-grpc_out=gen --go-grpc_opt=module=$(PROTO_GO) \
			"$$f"; \
	done \
	&& go build ./gen/...

clean: ## Stop the stack and delete named volumes
	docker compose --profile app down -v
