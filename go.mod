module go.linka.cloud/protodb

go 1.19

require (
	github.com/alta/protopatch v0.3.4
	github.com/dgraph-io/badger/v2 v2.2007.4
	github.com/dgraph-io/badger/v3 v3.0.0-20220802162129-2de177900e3c
	github.com/dgraph-io/ristretto v0.1.0
	github.com/envoyproxy/protoc-gen-validate v0.6.2
	github.com/fullstorydev/grpchan v1.1.1
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.5.0
	github.com/lyft/protoc-gen-star v0.6.1-0.20211111032917-1635a6baa3f5
	github.com/mennanov/fmutils v0.1.1
	github.com/planetscale/vtprotobuf v0.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/testify v1.7.1
	go.linka.cloud/grpc v0.4.1-0.20220726055518-8e6cfd2daa71
	go.linka.cloud/protoc-gen-defaults v0.4.0
	go.linka.cloud/protoc-gen-go-fields v0.2.1
	go.linka.cloud/protoc-gen-proxy v0.0.0-20220509093653-fcccfa74eb0f
	go.linka.cloud/protofilters v0.5.0
	go.uber.org/multierr v1.7.0
	golang.org/x/tools v0.3.0
	google.golang.org/grpc v1.46.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0
	google.golang.org/protobuf v1.28.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bombsimon/logrusr/v2 v2.0.1 // indirect
	github.com/caitlinelfring/go-env-default v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/iancoleman/strcase v0.2.0 // indirect
	github.com/improbable-eng/grpc-web v0.14.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jhump/protoreflect v1.11.0 // indirect
	github.com/justinas/alice v1.2.0 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.29.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/net v0.2.0 // indirect
	golang.org/x/sys v0.2.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	google.golang.org/genproto v0.0.0-20211208223120-3a66f561d7aa // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	nhooyr.io/websocket v1.8.6 // indirect
)

replace github.com/grpc-ecosystem/go-grpc-prometheus => github.com/linka-cloud/go-grpc-prometheus v1.2.0-lk
