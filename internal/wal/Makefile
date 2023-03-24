GOPKG=$(GOPATH)/pkg/mod

gen-proto:
	protoc \
	--proto_path=$(GOPKG)/github.com/gogo/protobuf@v1.3.2:./protos \
	--gofast_out=./walpb  \
	wal.proto