package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft-boltdb"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/common"
	"gitlab.alibaba-inc.com/RDS_PG/polardb_cluster_manager/pkg/resource"
	"io"
	"k8s.io/client-go/tools/leaderelection"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrOpenTimeout is returned when the RaftConsensusService does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	applyTimeout        = 10 * time.Second
	openTimeout         = 120 * time.Second
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

type ConsistencyLevel int

// Represents the available consistency levels.
const (
	Default ConsistencyLevel = iota
	Stale
	Consistent
)

type RaftConsensusService struct {
	RaftDir         string
	RaftBind        string
	SingleBootstrap bool

	leaderNotifyCh chan bool
	mu             sync.Mutex
	m              map[string][]byte // The key-value store for the system.

	raft *raft.Raft // The consensus mechanism
}

// New returns a new RaftConsensusService.
func NewRaftConsensusService() *RaftConsensusService {
	return &RaftConsensusService{
		m:               make(map[string][]byte),
		leaderNotifyCh:  make(chan bool, 3),
		SingleBootstrap: false,
	}
}

func (s *RaftConsensusService) LeaderTransfer(nodeID, addr string) error {
	var transferFutrue raft.Future
	if addr == "" {
		transferFutrue = s.raft.LeadershipTransfer()
	} else {
		transferFutrue = s.raft.LeadershipTransferToServer(raft.ServerID(nodeID), raft.ServerAddress(addr))
	}

	if transferFutrue.Error() != nil {
		return errors.Wrapf(transferFutrue.Error(), "Failed to transfer leader to %s:%s", nodeID, addr)
	} else {
		return nil
	}
}

func (s *RaftConsensusService) MemberStatus() map[string]string {
	//return s.raft.ReplicaStats()
	return map[string]string{}
}

func (s *RaftConsensusService) Status() string {
	res, err := json.MarshalIndent(s.raft.Stats(), "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(res)
}

// Get returns the value for the given key.
func (s *RaftConsensusService) Get(key string) ([]byte, error) {
	if err := s.consistentRead(); err != nil {
		return nil, err
	}

	if err := s.WaitForApplied(3 * time.Second); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if v, exist := s.m[key]; exist {
		return v, nil
	} else {
		return nil, errors.New("not found")
	}
}

// Set sets the value for the given key.
func (s *RaftConsensusService) Set(key string, value []byte) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *RaftConsensusService) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *RaftConsensusService) Run(enableRecovery bool, ctx context.Context, lec leaderelection.LeaderElectionConfig) error {
	isLeader := false
	err := s.Open(true, enableRecovery, lec)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case leader := <-s.leaderNotifyCh:
			if leader && !isLeader {
				lec.Callbacks.OnStartedLeading(ctx)
				isLeader = true
			} else if isLeader && !leader {
				lec.Callbacks.OnStoppedLeading()
				isLeader = false
			}
			id, err := s.LeaderID()
			if err == nil {
				lec.Callbacks.OnNewLeader(id)
			} else {
				log.Warnf("Failed to get leader id %s", err.Error())
			}
		case <-ticker.C:
			if isLeader {
				verify := s.raft.VerifyLeader()
				if err := verify.Error(); err != nil {
					log.Warnf("%s is not leader err %s", resource.GetLocalServerAddr(false), err.Error())
					lec.Callbacks.OnStoppedLeading()
					isLeader = false
				}
			} else {
				id, err := s.LeaderID()
				if err == nil {
					lec.Callbacks.OnNewLeader(id)
				} else {
					log.Warnf("Failed to get leader id %s", err.Error())
				}

			}
		case <-ctx.Done():
			s.raft.Shutdown()
			return nil
		}
	}

}

func (s *RaftConsensusService) LeaderAddr() string {
	return string(s.raft.Leader())
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *RaftConsensusService) LeaderID() (string, error) {
	addr := s.LeaderAddr()
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Warnf("failed to get raft configuration: %v", err)
		return "", err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}
	return "", nil
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *RaftConsensusService) Open(enableSingle bool, enableRecovery bool, lec leaderelection.LeaderElectionConfig) error {
	// Setup Raft configuration.
	//config.LeaderLeaseTimeout = time.Second * 5
	//config.HeartbeatTimeout = time.Second * 2
	//config.ElectionTimeout = time.Second * 5

	//config.HeartbeatTimeout = lec.RetryPeriod
	//config.ElectionTimeout = lec.RetryPeriod
	//config.LeaderLeaseTimeout = lec.LeaseDuration

	newNode := !pathExists(filepath.Join(s.RaftDir, "raft.db"))

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, log.Logger().Writer())
	if err != nil {
		return err
	}

	config := raft.DefaultConfig()
	config.NotifyCh = s.leaderNotifyCh
	config.LocalID = raft.ServerID(lec.Lock.Identity())
	config.SnapshotInterval = 60 * time.Second
	config.SnapshotThreshold = 128
	if *common.LogFile != "" {
		f, err := os.OpenFile(*common.WorkDir+"/log/"+*common.LogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log.Fatal(err)
		}
		config.LogOutput = f
	}
	config.LogLevel = "TRACE"

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, log.Logger().Writer())
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle && newNode {
		log.Infof("bootstrap new node with single")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	} else if enableRecovery {
		log.Infof("recovery with single node")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = raft.RecoverCluster(config, (*fsm)(s), logStore, stableStore, snapshots, transport, configuration)
		if err != nil {
			return fmt.Errorf("Failed to RecoverCluster err %s", err.Error())
		}
	} else {
		log.Infof("no bootstrap needed")
	}

	return nil
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *RaftConsensusService) WaitForLeader(timeout time.Duration) (string, error) {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			l := s.LeaderAddr()
			if l != "" {
				return l, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

// WaitForAppliedIndex blocks until a given log index has been applied,
// or the timeout expires.
func (s *RaftConsensusService) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// WaitForApplied waits for all Raft log entries to to be applied to the
// underlying database.
func (s *RaftConsensusService) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	log.Debugf("waiting for up to %s for application of initial logs", timeout)
	if err := s.WaitForAppliedIndex(s.raft.LastIndex(), timeout); err != nil {
		return ErrOpenTimeout
	}
	return nil
}

// consistentRead is used to ensure we do not perform a stale
// read. This is done by verifying leadership before the read.
func (s *RaftConsensusService) consistentRead() error {
	future := s.raft.VerifyLeader()
	if err := future.Error(); err != nil {
		return err //fail fast if leader verification fails
	}

	return nil
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *RaftConsensusService) Join(nodeID, addr string) error {
	log.Infof("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Warnf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				log.Infof("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	log.Infof("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *RaftConsensusService) Remove(nodeID, addr string) error {
	future := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
	}
	log.Infof("node %s at %s removed successfully", nodeID, addr)
	return nil
}

type fsm RaftConsensusService

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string][]byte)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string][]byte)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string][]byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

// pathExists returns true if the given path exists.
func pathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
