GOPKG=$(GOPATH)/pkg/mod

gen-proto-walpb:
	protoc \
	--proto_path=$(GOPKG)/github.com/gogo/protobuf@v1.3.2:./protos \
	--gofast_out=./walpb  \
	wal.proto 

gen-proto-logpb:
	protoc \
	--proto_path=$(GOPKG)/github.com/gogo/protobuf@v1.3.2:./protos \
	--gofast_out=./logpb \
	log.proto 

format-proto:
	clang-format -i ./protos/*.proto

gen-proto: gen-proto-walpb gen-proto-logpb