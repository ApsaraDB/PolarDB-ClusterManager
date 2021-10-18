package action

import (
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/meta"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/status"
	"strconv"
)

type StorageFullActionExecutor struct {
	Lock bool
}

func (e *StorageFullActionExecutor) String() string {
	return "StorageFullActionExecutor: lock=" + strconv.FormatBool(e.Lock)
}

func (e *StorageFullActionExecutor) Execute(metaManager *meta.MetaManager, statusManager *status.StatusManager) error {
	metaManager.ClusterSpec.ReadOnly = e.Lock
	return nil
}
