CLUSTER_MANAGER_IMAGE_NAME := polardb/polardb-cluster-manager

all:
	make prepare
	make fmt
	make build

fmt:
	go fmt ./pkg/...
	go fmt ./cmd/...

prepare:
	go mod tidy
	go mod vendor
	./tools/version_hack.sh

build:
	docker build . -t ${CLUSTER_MANAGER_IMAGE_NAME}

clean:
	./tools/version_reset.sh
	rm -rf vendor
	docker image rm -f ${CLUSTER_MANAGER_IMAGE_NAME}

