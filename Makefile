PWD = $(shell pwd)

GO ?= go

GOPKG=$(GOPATH)/pkg/mod

PROTO_SRC = protos

PROTO_DES = $(PWD)/pb

INTEGRATION_PACKAGES ?= github.com/datbeohbbh/wal/tests

PACKAGES ?= $(filter-out $(INTEGRATION_PACKAGES), $(shell $(GO) list ./...))

# test

test-unit:
	$(GO) test -count=1 -v $(PACKAGES)

test-integration:
	$(GO) test -count=1 -v $(INTEGRATION_PACKAGES)

test-all: test-unit test-integration

# format

go-format:
	gofmt ./...

format-proto:
	clang-format -i $(PROTO_SRC)/*.proto

# gen proto

gen-proto-walpb:
	protoc \
	--proto_path=$(GOPKG)/github.com/gogo/protobuf@v1.3.2:./protos \
	--gofast_out=$(PROTO_DES)/walpb  \
	$(PROTO_SRC)/wal.proto 

gen-proto-logpb:
	protoc \
	--proto_path=$(GOPKG)/github.com/gogo/protobuf@v1.3.2:./protos \
	--gofast_out=$(PROTO_DES)/logpb \
	$(PROTO_SRC)/log.proto 

gen-proto: gen-proto-walpb gen-proto-logpb