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

COVERAGE_FLOOR ?= 60
CRITICAL_PACKAGES = ./internal/db ./internal/index
CRITICAL_COVERPKG = ./internal/db,./internal/index
BADGERD_COVERAGE_FLOOR ?= 35
BADGERD_PACKAGES = $(shell go list ./internal/badgerd/... | grep -v '/pb$$')
BADGERD_COVERPKG = $(shell go list ./internal/badgerd/... | grep -v '/pb$$' | paste -sd, -)


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

protoc-gen-protodb: bin
	@protoc -I$(PROTO_BASE_PATH) \
		-I $(shell go list -m -f {{.Dir}} google.golang.org/protobuf) \
		--go_out=$(PROTO_OPTS):. protodb/protodb.proto
	@go install ./cmd/protoc-gen-protodb

bin:
	@go install golang.org/x/tools/cmd/goimports
	@go install google.golang.org/protobuf/cmd/protoc-gen-go
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	@go install go.linka.cloud/protoc-gen-defaults
	@go install go.linka.cloud/protoc-gen-go-fields
	@go install go.linka.cloud/protoc-gen-proxy
	@go install github.com/envoyproxy/protoc-gen-validate
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway
	@go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2
	@go install github.com/alta/protopatch/cmd/protoc-gen-go-patch
	@go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto
	@go install github.com/bufbuild/buf/cmd/buf@v1.45.0

clean:
	@rm -rf .bin
	@find $(PROTO_BASE_PATH) -name '*.pb*.go' -type f -exec rm {} \;

.PHONY: proto
proto: gen-proto lint


.PHONY: gen-proto
gen-proto: protoc-gen-protodb
	@buf generate
	@find $(PROTO_BASE_PATH)/tests -name '*.proto' -type f -exec \
    	protoc $(INCLUDE_PROTO_PATH) \
    		--go-patch_out=plugin=go,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=go-grpc,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=defaults,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=go-fields,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=protodb,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=proxy,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=go-vtproto,features=marshal+unmarshal+size+clone+equal,$(PROTO_OPTS):. \
    		--go-patch_out=plugin=validate,lang=go,$(PROTO_OPTS):. {} \;

.PHONY: lint
lint:
	@gofmt -w $(PWD)

# @goimports -w -local $(MODULE) $(PWD)

.PHONY: tests
tests: proto
	@go test -count=1 -timeout 1h -v -p 1 ./...

.PHONY: ci-test-unit
ci-test-unit:
	@go list ./... | grep -v tests | xargs -n 1 go test -v -count 1 -shuffle=on -p 1

.PHONY: ci-test-race
ci-test-race: ci-test-race-critical ci-test-race-badgerd

.PHONY: ci-test-race-critical
ci-test-race-critical:
	@go test -v -count 1 -shuffle=on -race $(CRITICAL_PACKAGES)

.PHONY: ci-test-race-badgerd
ci-test-race-badgerd:
	@go test -v -count 1 -shuffle=on -race $(BADGERD_PACKAGES)

.PHONY: ci-test-coverage
ci-test-coverage: ci-test-coverage-critical ci-test-coverage-badgerd

.PHONY: ci-test-coverage-critical
ci-test-coverage-critical:
	@go test -v -count 1 -covermode=atomic -coverpkg=$(CRITICAL_COVERPKG) -coverprofile=coverage-db-index.out $(CRITICAL_PACKAGES)

.PHONY: ci-test-coverage-badgerd
ci-test-coverage-badgerd:
	@go test -v -count 1 -covermode=atomic -coverpkg=$(BADGERD_COVERPKG) -coverprofile=coverage-badgerd.out $(BADGERD_PACKAGES)

.PHONY: ci-test-coverage-check
ci-test-coverage-check: ci-test-coverage-critical-check ci-test-coverage-badgerd-check

.PHONY: ci-test-coverage-critical-check
ci-test-coverage-critical-check: ci-test-coverage-critical
	@pct=$$(go tool cover -func=coverage-db-index.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); \
	if [ -z "$$pct" ]; then pct=0; fi; \
	echo "critical coverage: $$pct%"; \
	awk -v pct="$$pct" -v floor="$(COVERAGE_FLOOR)" 'BEGIN { \
		if (pct+0 < floor+0) { \
			printf("coverage floor not met: %.2f%% < %.2f%%\n", pct+0, floor+0); \
			exit 1; \
		} \
		printf("coverage floor satisfied: %.2f%% >= %.2f%%\n", pct+0, floor+0); \
	}'

.PHONY: ci-test-coverage-badgerd-check
ci-test-coverage-badgerd-check: ci-test-coverage-badgerd
	@pct=$$(go tool cover -func=coverage-badgerd.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); \
	if [ -z "$$pct" ]; then pct=0; fi; \
	echo "badgerd coverage: $$pct%"; \
	awk -v pct="$$pct" -v floor="$(BADGERD_COVERAGE_FLOOR)" 'BEGIN { \
		if (pct+0 < floor+0) { \
			printf("badgerd coverage floor not met: %.2f%% < %.2f%%\n", pct+0, floor+0); \
			exit 1; \
		} \
		printf("badgerd coverage floor satisfied: %.2f%% >= %.2f%%\n", pct+0, floor+0); \
	}'

.PHONY: ci-test
ci-test: ci-test-unit ci-test-race ci-test-coverage-check

.PHONY: ci-fuzz-smoke
ci-fuzz-smoke:
	@go test -fuzz=FuzzTokenDecodeNoPanic -fuzztime=5s ./internal/token
	@go test -fuzz=FuzzTokenRoundTrip -fuzztime=5s ./internal/token

.PHONY: ci-integration
ci-integration:
	@set -eu; \
	if [ -z "$(TEST)" ]; then \
		echo "TEST is required, e.g. make ci-integration TEST=TestEmbed"; \
		exit 1; \
	fi; \
	name=""; \
	for p in $$(printf '%s' "$(TEST)" | tr '/' '\n'); do \
		name="$$name^\\Q$$p\\E$$/"; \
	done; \
	go test -v -count 1 -p 1 -timeout 1h -run "$$name" ./tests

check-fmt:
	@[ "$(gofmt -l $(find . -name '*.go') 2>&1)" = "" ]

vet:
	@go list ./...|grep -v scratch|GOOS=linux xargs go vet

build:
	@CGO_ENABLED=0 go build -v ./cmd/...
