module go.linka.cloud/protodb

go 1.16

require (
	github.com/alta/protopatch v0.3.4
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/envoyproxy/protoc-gen-validate v0.6.2
	github.com/fullstorydev/grpchan v1.1.1
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.5.0
	github.com/planetscale/vtprotobuf v0.2.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	go.linka.cloud/grpc v0.3.7-0.20220317142231-884d59e280e3
	go.linka.cloud/protoc-gen-defaults v0.4.0
	go.linka.cloud/protoc-gen-go-fields v0.1.1
	go.linka.cloud/protofilters v0.3.2
	go.uber.org/multierr v1.7.0
	google.golang.org/grpc v1.45.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.27.1
)

require (
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/jhump/protoreflect v1.11.0
	github.com/lyft/protoc-gen-star v0.6.0
	github.com/mennanov/fmutils v0.1.1
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/cobra v1.3.0
)

replace github.com/grpc-ecosystem/go-grpc-prometheus => github.com/linka-cloud/go-grpc-prometheus v1.2.0-lk
