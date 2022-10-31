package server

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	tidwall "github.com/tidwall/wal"
	"oxia/coordination"
)

type EntryId struct {
	epoch  uint64
	offset uint64
}

func EntryIdFromProto(id *coordination.EntryId) EntryId {
	return EntryId{
		epoch:  id.Epoch,
		offset: id.Offset,
	}
}

func (id EntryId) toProto() *coordination.EntryId {
	return &coordination.EntryId{
		Epoch:  id.epoch,
		Offset: id.offset,
	}
}

type tidwallLog struct {
	log       *tidwall.Log
	index     map[EntryId]uint64
	reverse   map[uint64]EntryId
	callbacks []func(*coordination.LogEntry) error
}

func NewTidwallLog(walDir string, shard string) (Wal, error) {
	logImpl, err := tidwall.Open(walDir+"/"+shard, tidwall.DefaultOptions)
	if err != nil {
		return nil, err
	}
	// TODO persist index
	if err != nil {
		return nil, err
	}
	impl := &tidwallLog{
		log:       logImpl,
		index:     nil,
		reverse:   nil,
		callbacks: make([]func(*coordination.LogEntry) error, 0, 10000),
	}
	err = impl.buildIndexes()
	if err != nil {
		return nil, err
	}
	return impl, nil
}

func (t *tidwallLog) buildIndexes() error {
	m := make(map[EntryId]uint64)
	r := make(map[uint64]EntryId)
	first, err := t.log.FirstIndex()
	if err != nil {
		return err
	}
	last, err := t.log.LastIndex()
	if err != nil {
		return err
	}
	for index := first; index != 0 && index <= last; index++ {
		logEntry, err2 := t.readEntryAt(index)
		if err2 != nil {
			return err2
		}
		entryId := EntryIdFromProto(logEntry.EntryId)
		m[entryId] = index
		r[index] = entryId
	}
	t.index = m
	t.reverse = r
	return nil
}
func (t *tidwallLog) readEntryAt(index uint64) (*coordination.LogEntry, error) {
	read, err2 := t.log.Read(index)
	if err2 != nil {
		return nil, err2
	}
	logEntry := &coordination.LogEntry{}
	err2 = json.Unmarshal(read, logEntry)
	if err2 != nil {
		return nil, err2
	}
	return logEntry, nil

}

func (t *tidwallLog) LogLength() uint64 {
	return uint64(len(t.index))
}

func (t *tidwallLog) EntryIdAt(internalOffset uint64) *coordination.EntryId {
	id := t.reverse[internalOffset]
	return id.toProto()
}

func (t *tidwallLog) Close() error {
	return t.log.Close()
}

func (t *tidwallLog) Append(entry *coordination.LogEntry) error {
	bytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	index := t.LogLength() + 1
	err = t.log.Write(index, bytes)
	if err != nil {
		return err
	}
	id := EntryIdFromProto(entry.EntryId)
	t.index[id] = index
	t.reverse[index] = id

	for i := 0; i < len(t.callbacks); {
		callback := t.callbacks[i]
		err2 := callback(entry)
		if err2 != nil {
			// TODO retry
			log.Error().Err(err2).Msg("Encountered error. Removing callback")
			t.callbacks[i] = t.callbacks[len(t.callbacks)-1]
			t.callbacks = t.callbacks[:len(t.callbacks)-1]
		} else {
			i++
		}
	}
	return nil
}

func (t *tidwallLog) ReadOne(id *coordination.EntryId) (*coordination.LogEntry, error) {
	index, ok := t.index[EntryIdFromProto(id)]
	if !ok {
		return nil, errors.Errorf("Can't read id %s", id)
	}
	logEntry, err2 := t.readEntryAt(index)
	if err2 != nil {
		return nil, err2
	}
	return logEntry, nil
}

func (t *tidwallLog) ReadSync(previousCommittedEntryId *coordination.EntryId, lastCommittedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error {
	start, ok := t.index[EntryIdFromProto(previousCommittedEntryId)]
	if !ok {
		return errors.Errorf("Can't read id %s", previousCommittedEntryId)
	}
	start++
	end, ok := t.index[EntryIdFromProto(lastCommittedEntryId)]
	if !ok {
		return errors.Errorf("Can't read id %s", lastCommittedEntryId)
	}
	for i := start; i <= end; i++ {
		logEntry, err := t.readEntryAt(i)
		if err != nil {
			return err
		}
		err = callback(logEntry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tidwallLog) Read(lastPushedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error {
	start, ok := t.index[EntryIdFromProto(lastPushedEntryId)]
	if !ok {
		return errors.Errorf("Can't read id %s", lastPushedEntryId)
	}
	start++
	for i := start; i <= t.LogLength(); i++ {
		logEntry, err := t.readEntryAt(i)
		if err != nil {
			return err
		}
		err = callback(logEntry)
		if err != nil {
			return err
		}
	}
	t.callbacks = append(t.callbacks, callback)
	return nil
}

func (t *tidwallLog) StopReaders() {
	t.callbacks = make([]func(*coordination.LogEntry) error, 0, 10000)
}

func (t *tidwallLog) GetHighestEntryOfEpoch(epoch uint64) (*coordination.EntryId, error) {
	offset, ok := t.index[EntryId{
		epoch:  epoch + 1,
		offset: 1,
	}]
	if ok {
		return t.reverse[offset-1].toProto(), nil
	} else {
		return t.reverse[t.LogLength()].toProto(), nil
	}
}

func (t *tidwallLog) TruncateLog(lastSafeEntryId *coordination.EntryId) (*coordination.EntryId, error) {
	lastSafe, ok := t.index[EntryIdFromProto(lastSafeEntryId)]
	if !ok {
		return nil, errors.Errorf("Can't read id %s", lastSafeEntryId)
	}
	end := t.LogLength()
	for i := lastSafe + 1; i <= end; i++ {
		id := t.reverse[i]
		delete(t.index, id)
		delete(t.reverse, i)
	}
	err := t.log.TruncateBack(lastSafe)
	if err != nil {
		return nil, err
	}
	return t.reverse[lastSafe].toProto(), nil

}
