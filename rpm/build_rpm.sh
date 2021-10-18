#!/usr/bin/env bash
#yum erase -y  t-rds-spython-release && yum install -y -b current t-rds-spython-2.7.4.0
#pip install wheel
set -e
set -x

check_call()
{
    eval $@
    rc=$?
    if [[ ${rc} -ne 0 ]]; then
        echo "[$@] execute fail: $rc"
        exit 1
    fi
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
PROJECT_ROOT=$(dirname ${SCRIPT_DIR})
cd ${PROJECT_ROOT}

# ./3rd dir
GO_BASE=${PROJECT_ROOT}/3rd
GO_SRC_BASE=${PROJECT_ROOT}/gopath
APP_SRC_PATH=${GO_SRC_BASE}/src/gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager
echo ${APP_SRC_PATH}

# prepare
mkdir -p ${GO_SRC_BASE}/bin ${GO_SRC_BASE}/pkg ${GO_SRC_BASE}/src
mkdir -p ${APP_SRC_PATH}


# install_golang
if [[ "Linux"x = `uname`x ]]; then
  if [[ "aarch64"x = `uname -m`x ]];then
    echo arm
    tar zxf ${GO_BASE}/go1.15.linux-arm64.tar.gz -C ${GO_BASE}
  else
    echo x86_64
    tar zxf ${GO_BASE}/go1.15.linux-amd64.tar.gz -C ${GO_BASE}
  fi
fi

git config --global url.git@gitlab.alibaba-inc.com:.insteadOf https://gitlab.alibaba-inc.com

export PATH=${GO_BASE}/go/bin:$PATH
export GOROOT=${GO_BASE}/go
export GOPRIVATE=gitlab.alibaba-inc.com
export GOPROXY=http://gomodule-repository.aone.alibaba-inc.com,https://proxy.golang.org,direct

# export GOPATH=${GO_SRC_BASE}
go version
ls -al
cd ${PROJECT_ROOT}
cp -r `ls -A | grep -v '3rd' | grep -v 'gopath' | grep -v '.git'` ${APP_SRC_PATH}
cd ${APP_SRC_PATH}

make clean
make local
