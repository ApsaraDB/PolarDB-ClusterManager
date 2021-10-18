FROM golang:1.17.0-alpine3.14

RUN go env -w GOPROXY=https://goproxy.cn,direct

RUN go install github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /go/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager

COPY . /go/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/

RUN CGO_ENABLED=0 go build -mod=vendor -ldflags=-compressdwarf=false -gcflags='all=-N -l' -o polardb-cluster-manager cmd/manager/main.go

FROM alpine:3.13 as prod

WORKDIR /root/

COPY --from=0 /go/bin/dlv /bin/dlv

RUN mkdir -p /root/polardb_cluster_manager/conf \
    && mkdir -p /usr/local/polardb_cluster_manager/bin \
    && mkdir -p  /usr/local/polardb_cluster_manager/bin/plugin/manager

COPY --from=0 /go/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/polardb-cluster-manager /usr/local/polardb_cluster_manager/bin/polardb-cluster-manager
COPY --from=0 /go/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/tools/supervisor.py /usr/local/polardb_cluster_manager/bin/supervisor.py
COPY --from=0 /go/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/conf/polardb_cluster_manager.conf /root/polardb_cluster_manager/conf/polardb_cluster_manager.conf

ENTRYPOINT ["/usr/local/polardb_cluster_manager/bin/polardb-cluster-manager", "--work_dir=/root/polardb_cluster_manager"]
