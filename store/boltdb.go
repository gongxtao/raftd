package store

import (
	"github.com/sirupsen/logrus"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/boltdb/bolt"
	"github.com/gongxtao/raftd/util"
)

const (
	dbFileMode = 0600
)

type Item struct {
	Err 	error 	`json:"-"`
	Key		[]byte	`json:"key"`
	Value 	[]byte	`json:"value"`
}


type BoltDB struct {
	// db client
	db 	*bolt.DB

	// store path
	path string
}

// boltdb option
type Options struct {
	// store path
	Path	string
}

func NewOptions(path string) (*Options, error) {
	if path == "" {
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	return &Options{
		Path: path,
	}, nil
}


func NewBoltDB(path string) (*BoltDB, error) {
	opts, err := NewOptions(path)
	if err != nil {
		return nil, err
	}

	conn, err := bolt.Open(opts.Path, dbFileMode, bolt.DefaultOptions)
	if err != nil {
		logrus.Errorf("failed to open raft db: %v", err)
		return nil, err
	}

	db := &BoltDB{
		db: conn,
		path: opts.Path,
	}

	if err := db.initialize(); err != nil {
		logrus.Errorf("failed to initialize: %v", err)
		return nil, err
	}

	return db, nil
}

func (b *BoltDB) Close() error {
	return b.db.Close()
}

func (b *BoltDB) Bucket(name []byte) *Bucket {
	return &Bucket{name: name, db: b.db}
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.

// FirstIndex returns the first index written. 0 for no entries.
func (b *BoltDB) FirstIndex() (uint64, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(dbLogs).Cursor()
	if first, _ := cursor.First(); first == nil {
		return 0, nil
	} else {
		return util.BytesToUint64(first), nil
	}

	return 0, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (b *BoltDB) LastIndex() (uint64, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(dbLogs).Cursor()
	if last, _ := cursor.Last(); last == nil {
		return 0, nil
	} else {
		return util.BytesToUint64(last), nil
	}

	return 0, nil
}

// GetLog gets a log entry at a given index.
func (b *BoltDB) GetLog(index uint64, log *raft.Log) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	val := tx.Bucket(dbLogs).Get(util.Uint64ToBytes(index))
	if val == nil {
		return ErrKeyNotFound
	}

	return util.DecodeMsgPack(val, log)
}


// StableStore is used to provide stable storage
// of key configurations to ensure safety.

// StoreLog stores a log entry.
func (b *BoltDB) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (b *BoltDB) StoreLogs(logs []*raft.Log) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		index := util.Uint64ToBytes(log.Index)
		val, err := util.EncodeMsgPack(log)
		if err != nil {
			return err
		}

		if err := tx.Bucket(dbLogs).Put(index, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (b *BoltDB) DeleteRange(min, max uint64) error {
	minKey := util.Uint64ToBytes(min)

	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if util.BytesToUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (b *BoltDB) Set(key []byte, val []byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	if err := bucket.Put(key, val); err != nil {
		return err
	}

	return tx.Commit()
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (b *BoltDB) Get(key []byte) ([]byte, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	val := tx.Bucket(dbConf).Get(key)
	if val == nil {
		return nil, ErrKeyNotFound
	}

	return append([]byte(nil), val...), nil
}

func (b *BoltDB) SetUint64(key []byte, val uint64) error {
	return b.Set(key, util.Uint64ToBytes(val))
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (b *BoltDB) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}

	return util.BytesToUint64(val), nil
}

func (b *BoltDB) initialize() error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil
	}
	defer tx.Rollback()

	if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
		return err
	}

	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}

	return tx.Commit()
}

type Bucket struct {
	name	[]byte

	db 		*bolt.DB
}

func (b *Bucket) Set(key, value []byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bkt := tx.Bucket([]byte(b.name))
	if bkt == nil {
		bkt, err = tx.CreateBucket([]byte(b.name))
		if err != nil {
			return err
		}
	}

	if err := bkt.Put([]byte(key), []byte(value)); err != nil {
		return err
	}

	return tx.Commit()
}

func (b *Bucket) Get(key []byte) (string, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	bkt := tx.Bucket([]byte(b.name))
	if bkt == nil {
		return "", ErrBucketNotFound
	}

	return string(bkt.Get([]byte(key))), nil
}

func (b *Bucket) Del(key []byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bkt := tx.Bucket([]byte(b.name))
	if bkt == nil {
		return ErrBucketNotFound
	}

	if err := bkt.Delete([]byte(key)); err != nil {
		return err
	}

	return tx.Commit()
}

func (b *Bucket) AllData() (<- chan *Item, error) {
	itemCh := make(chan *Item)
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bkt := tx.Bucket([]byte(b.name))
	if bkt == nil {
		return nil, ErrBucketNotFound
	}

	go func() {
		defer close(itemCh)

		if err := bkt.ForEach(func(k, v []byte) error {
			itemCh <- &Item{Key: k, Value: v}

			return nil
		}); err != nil {
			itemCh <- &Item{Err: err}
		}
	}()

	return itemCh, nil
}

