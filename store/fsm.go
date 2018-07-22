package store

import (
	"github.com/hashicorp/raft"
	"io"
	"sync"
	"github.com/sirupsen/logrus"
	"encoding/json"
	"errors"
	"encoding/binary"
)

// buckets
var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")
	dbData = []byte("data")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
	ErrBucketNotFound = errors.New("bucket is not exist")
)

// FSM provides an interface that can be implemented by
// clients to make use of the replicated log.
type FSM struct {
	sync.RWMutex
	// apply logs
	rb 	*BoltDB
}

func NewFSM(rb *BoltDB) (*FSM, error) {

	return &FSM{
		rb: rb,
	}, nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	logrus.Infof("apply log, index:%d,term:%d,type:%v,data:%s", log.Index, log.Term, log.Type, string(log.Data))

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		logrus.Errorf("failed to unmarshal log: %v", err)
		return err
	}

	switch cmd.OP {
	case SET:
		return f.set(cmd.Key, cmd.Value)
	case DEL:
		return f.del(cmd.Key)
	default:
		logrus.Errorf("unknown operator command: %s", cmd.OP)
		return ErrUnknownCommand
	}

	return nil
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.RLock()
	defer f.RUnlock()

	logrus.Infoln("execute fsm snapshot...")
	return &FSMSnapshot{rb: f.rb}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FSM) Restore(reader io.ReadCloser) error {
	return nil
}

// set key -> value
func (f *FSM) set(key, value string) error {
	f.Lock()
	defer f.Unlock()

	return f.rb.Bucket(dbData).Set([]byte(key), []byte(value))
}

// del key and value
func (f *FSM) del(key string) error {
	f.Lock()
	defer f.Unlock()

	return f.rb.Bucket(dbData).Del([]byte(key))
}

// get value by key
func (f *FSM) get(key string) (string, error) {
	f.RLock()
	defer f.RUnlock()

	return f.rb.Bucket(dbData).Get([]byte(key))
}


// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot struct {
	rb 	*BoltDB
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	logrus.Infoln("execute fmt snapshot persist...")

	dataC, err := f.rb.Bucket(dbData).AllData()
	if err != nil {
		logrus.Errorf("failed to get all data channel: %v", err)
		return err
	}

	// persist
	for item := range dataC {
		if item.Err != nil {
			return err
		}

		bs, err := json.Marshal(item)
		if err != nil {
			return err
		}

		size := uint16(len(bs))
		bf := make([]byte, size + 2)
		binary.LittleEndian.PutUint16(bf, size)
		if _, err := sink.Write(bs); err != nil {
			sink.Cancel()
			return err
		}
	}

	return sink.Close()
}

// Release is invoked when we are finished with the snapshot.
func (f *FSMSnapshot) Release() {}




