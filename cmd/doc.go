// Package main — the Enron mail processor microservices.
//
// Each subdirectory becomes one deployable binary sharing the Dockerfile:
//
//	producer  reads the raw maildir -> RawEnvelope protos -> Kafka enron.raw
//	parser    consumes enron.raw -> parses mail -> ParsedMessage -> PostgreSQL
//	api       HTTP: POST /start, GET /status, GET /top-senders
package doc
