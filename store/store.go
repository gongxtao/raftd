package store

import (
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"time"
	"github.com/sirupsen/logrus"
	"fmt"
	"k8s.io/kubernetes/staging/src/k8s.io/apimachinery/pkg/util/json"
	"path/filepath"
)

const (
	SET = "set"
	GET = "get"
	DEL = "del"
)

var (
	Timeout = 10 * time.Second

	DataName = "raft.db"
)

var (
	ErrUnknownCommand = errors.New("unknown command")
)

type Command struct {
	OP		string	`json:"op;omitempty"`
	Key 	string	`json:"key;omitempty"`
	Value 	string	`json:"value;omitempty"`
}

type Logger struct {
}

func (l *Logger) Write(bs []byte) (int, error) {
	logrus.Infoln(string(bs))

	return len(bs), nil
}


type Store struct {
	localAddr 	string
	localId 	string
	dataPath 	string

	raft 	*raft.Raft
	db 		*BoltDB
}

// local addr: addr:port
func NewStore(path, localId string, localAddr string) (*Store, error) {
	s := &Store{
		localAddr: localAddr,
		dataPath: path,
		localId: localId,
	}

	raftC := raft.DefaultConfig()
	raftC.LocalID = raft.ServerID(s.localId)
	raftC.LogOutput = &Logger{}

	// transport
	//addr, err := net.ResolveIPAddr("tcp", s.localAddr)
	//if err != nil {
	//	logrus.WithField("addr", s.localAddr).Errorf("failed to resolve ip addr: %v", err)
	//	return nil, err
	//}
	transport, err := raft.NewTCPTransport(s.localAddr, nil, 3, Timeout, &Logger{})
	if err != nil {
		logrus.WithField("addr", s.localAddr).Errorf("failed to new tcp transport: %v", err)
		return nil, err
	}

	snapshots, err := raft.NewFileSnapshotStore(path, 1, &Logger{})
	if err != nil {
		logrus.WithField("path", path).Errorf("failed to new file snapshot store: %v", err)
		return nil, err
	}

	logDB, err := NewBoltDB(filepath.Join(path, DataName))
	if err != nil {
		logrus.WithField("path", path).Errorf("failed to new log store: %v", err)
		return nil, err
	}

	fsm, err := NewFSM(logDB)
	if err != nil {
		logrus.Errorf("failed to new fsm: %v", err)
		return nil, err
	}

	ra, err := raft.NewRaft(raftC, fsm, logDB, logDB, snapshots, transport)
	if err != nil {
		logrus.Errorf("failed to new raft node: %v", err)
		return nil, err
	}

	lastIndex, err := logDB.LastIndex()
	if err != nil {
		logrus.Errorf("failed to get last index: %v", err)
		return nil, err
	}
	if lastIndex == 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:	raftC.LocalID,
					Address: raft.ServerAddress(s.localAddr),
				},
			},
		}

		if err := ra.BootstrapCluster(configuration).Error(); err != nil {
			logrus.Errorf("failed to bootstrap cluster: %v", err)
			return nil, err
		}
	}

	s.raft = ra
	s.db = logDB

	return s, nil
}

func (s *Store) Set(key string, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	cmd := &Command{
		OP: SET,
		Key: key,
		Value: value,
	}

	bs, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	f := s.raft.Apply(bs, Timeout)
	return f.Error()
}

func (s *Store) Get(key string) (string, error) {
	v, err := s.db.Bucket(dbData).Get([]byte(key))
	if err != nil {
		return "", err
	}

	return string(v), nil
}

