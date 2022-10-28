package server

import (
	"encoding/json"
	tidwall "github.com/tidwall/wal"
	"oxia/coordination"
)

type tidwallLog struct {
	log *tidwall.Log
}

func (t *tidwallLog) LogLength() uint64 {
	index, err := t.log.LastIndex()
	if err != nil {
		return 0
	}
	return index + 1
}

func (t *tidwallLog) EntryIdAt(index uint64) *coordination.EntryId {
	read, err := t.log.Read(index)
	if err != nil {
		return nil
	}
	logEntry := &coordination.LogEntry{}
	err = json.Unmarshal(read, logEntry)
	if err != nil {
		return nil
	}
	return logEntry.EntryId
}

func (t *tidwallLog) Close() error {
	return t.log.Close()
}

func (t *tidwallLog) Append(entry *coordination.LogEntry) error {
	bytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	lastIndex, err := t.log.LastIndex()
	if err != nil {
		return err
	}
	return t.log.Write(lastIndex+1, bytes)
}

func (t *tidwallLog) Read(lastPushedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) (err error) {
	//TODO implement me
	panic("implement me")
}

func (t *tidwallLog) GetHighestEntryOfEpoch(epoch uint64) (*coordination.EntryId, error) {
	//TODO implement me
	panic("implement me")
}

func (t *tidwallLog) TruncateLog(headIndex *coordination.EntryId) (*coordination.EntryId, error) {
	//TODO implement me
	panic("implement me")
}

func (t *tidwallLog) StopReaders() {
	//TODO implement me
	panic("implement me")
}

func (t *tidwallLog) ReadOne(id *coordination.EntryId) (*coordination.LogEntry, error) {
	//TODO implement me
	panic("implement me")
}

func NewTidwallLog(walDir string, shard string) (Wal, error) {
	log, err := tidwall.Open(walDir+"/"+shard, tidwall.DefaultOptions)
	if err != nil {
		return nil, err
	}
	return &tidwallLog{
		log: log,
	}, nil
}
