# Copyright 2021 Linka Cloud  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MODULE = go.linka.cloud/protodb


PROTO_BASE_PATH = $(PWD)

INCLUDE_PROTO_PATH = -I$(PROTO_BASE_PATH) \
	-I $(shell go list -m -f {{.Dir}} google.golang.org/protobuf) \
	-I $(shell go list -m -f {{.Dir}} go.linka.cloud/protoc-gen-defaults) \
	-I $(shell go list -m -f {{.Dir}} go.linka.cloud/protofilters) \
	-I $(shell go list -m -f {{.Dir}} github.com/envoyproxy/protoc-gen-validate) \
	-I $(shell go list -m -f {{.Dir}} github.com/alta/protopatch) \
	-I $(shell go list -m -f {{.Dir}} github.com/grpc-ecosystem/grpc-gateway/v2)

PROTO_OPTS = paths=source_relative


$(shell mkdir -p .bin)

export GOBIN=$(PWD)/.bin

export PATH := $(GOBIN):$(PATH)

protoc-gen-protodb:
	@protoc -I$(PROTO_BASE_PATH) \
		-I $(shell go list -m -f {{.Dir}} google.golang.org/protobuf) \
		--go_out=$(PROTO_OPTS):. protodb/protodb.proto
	@go install ./cmd/protoc-gen-protodb

bin: protoc-gen-protodb
	@go install github.com/golang/protobuf/protoc-gen-go
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	@go install go.linka.cloud/protoc-gen-defaults
	@go install go.linka.cloud/protoc-gen-go-fields
	@go install go.linka.cloud/protodb/cmd/protoc-gen-protodb
	@go install github.com/envoyproxy/protoc-gen-validate
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2
	@go install github.com/alta/protopatch/cmd/protoc-gen-go-patch
	@go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto

clean:
	@rm -rf .bin
	@find $(PROTO_BASE_PATH) -name '*.pb*.go' -type f -exec rm {} \;

.PHONY: proto
proto: gen-proto lint


.PHONY: gen-proto
gen-proto: bin
	@find $(PROTO_BASE_PATH) -name '*.proto' -type f -exec \
    	protoc $(INCLUDE_PROTO_PATH) \
    		--go-patch_out=plugin=go,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=go-grpc,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=defaults,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=go-fields,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=protodb,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=go-vtproto,features=marshal+unmarshal+size,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=validate,lang=go,$(PROTO_OPTS):. {} \;

.PHONY: lint
lint:
	@goimports -w -local $(MODULE) $(PWD)
	@gofmt -w $(PWD)

.PHONY: tests
tests: proto
	@go test -v ./...

