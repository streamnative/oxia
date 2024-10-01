// Copyright 2024 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

//func TestSegment(t *testing.T) {
//	entries := 1000
//	path := t.TempDir()
//	rw, err := newReadWriteSegment(path, 0, 128*1024, 0)
//	assert.NoError(t, err)
//	for i := 0; i < entries; i++ {
//		err := rw.Append(int64(i), []byte(fmt.Sprintf("%d", i)))
//		assert.NoError(t, err)
//	}
//	rwSegment := rw.(*readWriteSegment)
//	actualRwMmap := rwSegment.txnMappedFile
//
//	var cursor uint32
//	var expectedPreviousCrc uint32
//	for i := 0; i < entries; i++ {
//		payloadSize := readInt(actualRwMmap, cursor)
//		cursor += SizeLen
//		previousCRC := readInt(actualRwMmap, cursor)
//		cursor += CrcLen
//		assert.EqualValues(t, expectedPreviousCrc, previousCRC)
//		payloadCRC := readInt(actualRwMmap, cursor)
//		cursor += CrcLen
//		expectedCrc := crc.Checksum(previousCRC).Update(actualRwMmap[cursor : cursor+payloadSize]).Value()
//		assert.EqualValues(t, expectedCrc, payloadCRC)
//		expectedPreviousCrc = payloadCRC
//		cursor += payloadSize
//	}
//
//	err = rw.Close()
//	assert.NoError(t, err)
//
//	ro, err := newReadOnlySegment(path, 0)
//	assert.NoError(t, err)
//
//	roSegment := ro.(*readonlySegment)
//	actualRoMmap := roSegment.txnMappedFile
//
//	cursor = 0
//	expectedPreviousCrc = 0
//	for i := 0; i < entries; i++ {
//		payloadSize := readInt(actualRoMmap, cursor)
//		cursor += SizeLen
//		previousCRC := readInt(actualRoMmap, cursor)
//		cursor += CrcLen
//		assert.EqualValues(t, expectedPreviousCrc, previousCRC)
//		payloadCRC := readInt(actualRoMmap, cursor)
//		cursor += CrcLen
//		expectedCrc := crc.Checksum(previousCRC).Update(actualRoMmap[cursor : cursor+payloadSize]).Value()
//		assert.EqualValues(t, expectedCrc, payloadCRC)
//		expectedPreviousCrc = payloadCRC
//		cursor += payloadSize
//	}
//	err = ro.Close()
//	assert.NoError(t, err)
//}
//
//func TestModifiedSegment(t *testing.T) {
//	entries := 1000
//	path := t.TempDir()
//	rw, err := newReadWriteSegment(path, 0, 128*1024, 0)
//	assert.NoError(t, err)
//	for i := 0; i < entries; i++ {
//		err := rw.Append(int64(i), []byte(fmt.Sprintf("%d", i)))
//		assert.NoError(t, err)
//	}
//
//	segment := rw.(*readWriteSegment)
//	actualMmap := segment.txnMappedFile
//
//	// inject failure
//	randomPosition := rand.Uint64() % uint64(segment.currentFileOffset)
//	binary.BigEndian.PutUint64(actualMmap[randomPosition:], randomPosition)
//
//	detectCorruption := false
//	var corruptionOffset int64
//	for i := 0; i < entries; i++ {
//		_, err := rw.Read(int64(i))
//		if err != nil {
//			if errors.Is(err, ErrDataCorrupted) {
//				detectCorruption = true
//				corruptionOffset = int64(i)
//			} else {
//				assert.Fail(t, "expected err", err)
//			}
//		}
//	}
//	assert.True(t, detectCorruption)
//	assert.NotZero(t, corruptionOffset)
//
//	err = rw.Close()
//	assert.NoError(t, err)
//
//	ro, err := newReadOnlySegment(path, 0)
//	assert.NoError(t, err)
//
//	roDetectCorruption := false
//	var roCorruptionOffset int64
//	for i := 0; i < entries; i++ {
//		_, err := ro.Read(int64(i))
//		if err != nil {
//			if errors.Is(err, ErrDataCorrupted) {
//				roDetectCorruption = true
//				roCorruptionOffset = int64(i)
//			} else {
//				assert.Error(t, err, "unexpected err")
//			}
//		}
//	}
//	assert.EqualValues(t, detectCorruption, roDetectCorruption)
//	assert.NotZero(t, corruptionOffset, roCorruptionOffset)
//}
//
//func TestWal(t *testing.T) {
//	entries := 10000
//	f, w := createWal(t)
//
//	for i := 0; i < entries; i++ {
//		assert.NoError(t, w.Append(&proto.LogEntry{
//			Term:   1,
//			Offset: int64(i),
//			Value:  []byte(fmt.Sprintf("entry-%d", i)),
//		}))
//	}
//
//	r, err := w.NewReader(0)
//	assert.NoError(t, err)
//	entryIndex := 0
//	for r.HasNext() {
//		entryIndex++
//		value, err := r.ReadNext()
//		assert.NoError(t, err)
//		assert.EqualValues(t, value.Value, fmt.Sprintf("entry-%d", entryIndex))
//	}
//	assert.EqualValues(t, entries-1, entryIndex)
//	assert.NoError(t, r.Close())
//	assert.NoError(t, w.Close())
//	assert.NoError(t, f.Close())
//}
//
//func TestModifiedWal(t *testing.T) {
//	entries := 10000
//	f, w := createWal(t)
//
//	for i := 0; i < entries; i++ {
//		assert.NoError(t, w.Append(&proto.LogEntry{
//			Term:   1,
//			Offset: int64(i),
//			Value:  []byte(fmt.Sprintf("entry-%d", i)),
//		}))
//	}
//
//	actualWal := w.(*wal)
//
//	lastOffset := actualWal.lastAppendedOffset.Load()
//
//	// inject failure
//	randomOffset := rand.Uint64() % uint64(lastOffset)
//	currentOffset := actualWal.currentSegment.BaseOffset()
//	if int64(randomOffset) >= currentOffset {
//		segment := actualWal.currentSegment
//		rwSegment := segment.(*readWriteSegment)
//		randomFileOffset := rand.Uint64() % uint64(rwSegment.currentFileOffset)
//		binary.BigEndian.PutUint64(rwSegment.txnMappedFile[randomFileOffset:], randomFileOffset)
//	} else {
//		roSegment, err := actualWal.readOnlySegments.Get(currentOffset)
//		assert.NoError(t, err)
//		actualSegment := roSegment.Get().(*readonlySegment)
//		lastFileOffset := fileOffset(actualSegment.idxMappedFile, actualSegment.baseOffset, actualSegment.lastOffset)
//		var txFile *os.File
//		txFile, err = os.OpenFile(actualSegment.txnPath, os.O_RDWR, 0644)
//		assert.NoError(t, err)
//		randomFileOffset := rand.Uint64() % uint64(lastFileOffset)
//		value, err := uuid.New().MarshalBinary()
//		assert.NoError(t, err)
//		_, err = txFile.WriteAt(value, int64(randomFileOffset))
//		assert.NoError(t, err)
//		assert.NoError(t, txFile.Sync())
//		// cleanup
//		assert.NoError(t, roSegment.Close())
//		assert.NoError(t, txFile.Close())
//	}
//
//	r, err := w.NewReader(0)
//	assert.NoError(t, err)
//	entryIndex := 0
//	roDetectCorruption := false
//	var roCorruptionOffset int64
//	for r.HasNext() {
//		entryIndex++
//		value, err := r.ReadNext()
//		if err != nil {
//			if errors.Is(err, ErrDataCorrupted) {
//				roDetectCorruption = true
//				roCorruptionOffset = int64(entryIndex)
//			} else {
//				assert.Error(t, err, "unexpected err")
//			}
//			break
//		}
//		assert.EqualValues(t, value.Value, fmt.Sprintf("entry-%d", entryIndex))
//	}
//
//	assert.True(t, roDetectCorruption)
//	assert.NotZero(t, roCorruptionOffset)
//
//	assert.NoError(t, r.Close())
//	assert.NoError(t, w.Close())
//	assert.NoError(t, f.Close())
//}
//
//func TestSameWals(t *testing.T) {
//	entries := 10000
//	f, w := createWal(t)
//	defer w.Close()
//	defer f.Close()
//
//	for i := 0; i < entries; i++ {
//		assert.NoError(t, w.Append(&proto.LogEntry{
//			Term:   1,
//			Offset: int64(i),
//			Value:  []byte(fmt.Sprintf("entry-%d", i)),
//		}))
//	}
//
//	w2, err := f.NewWal("other", 0, nil)
//	assert.NoError(t, err)
//	for i := 0; i < entries; i++ {
//		assert.NoError(t, w2.Append(&proto.LogEntry{
//			Term:   1,
//			Offset: int64(i),
//			Value:  []byte(fmt.Sprintf("entry-%d", i)),
//		}))
//	}
//
//	actualWal := w.(*wal)
//	segment := actualWal.currentSegment
//	actualSegment := segment.(*readWriteSegment)
//	previousCrc, currentCrc := getCrc(actualSegment, segment.LastOffset())
//
//	actualWal2 := w2.(*wal)
//	segment2 := actualWal2.currentSegment
//	actualSegment2 := segment2.(*readWriteSegment)
//	previousCrc2, currentCrc2 := getCrc(actualSegment2, segment.LastOffset())
//
//	assert.EqualValues(t, previousCrc2, previousCrc)
//	assert.EqualValues(t, currentCrc2, currentCrc)
//}
//
//func TestDeviatingWals(t *testing.T) {
//	entries := 10000
//	deviatingEntry := 50
//	f, w := createWal(t)
//	defer w.Close()
//	defer f.Close()
//
//	for i := 0; i < entries; i++ {
//		assert.NoError(t, w.Append(&proto.LogEntry{
//			Term:   1,
//			Offset: int64(i),
//			Value:  []byte(fmt.Sprintf("entry-%d", i)),
//		}))
//	}
//
//	w2, err := f.NewWal("other", 0, nil)
//	assert.NoError(t, err)
//	for i := 0; i < entries; i++ {
//		if i == deviatingEntry {
//			assert.NoError(t, w2.Append(&proto.LogEntry{
//				Term:   1,
//				Offset: int64(i),
//				Value:  []byte(fmt.Sprintf("xcxzczxczxczxc-%d", i)),
//			}))
//		} else {
//			assert.NoError(t, w2.Append(&proto.LogEntry{
//				Term:   1,
//				Offset: int64(i),
//				Value:  []byte(fmt.Sprintf("entry-%d", i)),
//			}))
//		}
//	}
//
//	actualWal := w.(*wal)
//	segment := actualWal.currentSegment
//	actualSegment := segment.(*readWriteSegment)
//	previousCrc, currentCrc := getCrc(actualSegment, segment.LastOffset())
//
//	actualWal2 := w2.(*wal)
//	segment2 := actualWal2.currentSegment
//	actualSegment2 := segment2.(*readWriteSegment)
//	previousCrc2, currentCrc2 := getCrc(actualSegment2, segment.LastOffset())
//
//	assert.NotEqualValues(t, previousCrc2, previousCrc)
//	assert.NotEqualValues(t, currentCrc2, currentCrc)
//
//	cursor1 := int64(0)
//	cursor2 := int64(0)
//	roSeg1, err := actualWal.readOnlySegments.Get(cursor1)
//	assert.NoError(t, err)
//	defer roSeg1.Close()
//	roSeg2, err := actualWal2.readOnlySegments.Get(cursor2)
//	assert.NoError(t, err)
//	defer roSeg2.Close()
//
//	errIndex := 0
//	for i := 0; i < 100; i++ {
//		cp1, c1 := getCrc(roSeg1.Get(), cursor1)
//		cp2, c2 := getCrc(roSeg2.Get(), cursor2)
//		if cp1 != cp2 || c1 != c2 {
//			errIndex = i
//			break
//		}
//
//		cursor1++
//		cursor2++
//	}
//	assert.EqualValues(t, errIndex, deviatingEntry)
//}
//
//func getCrc(actualSegment any, offset int64) (previousCRC uint32, payloadCRC uint32) {
//	var position uint32
//	if segment, ok := actualSegment.(*readWriteSegment); ok {
//		position = fileOffset(segment.writingIdx, segment.baseOffset, offset)
//		cursor := position
//		_ = readInt(segment.txnMappedFile, cursor)
//		cursor += SizeLen
//		previousCRC = readInt(segment.txnMappedFile, cursor)
//		cursor += CrcLen
//		payloadCRC = readInt(segment.txnMappedFile, cursor)
//		return previousCRC, payloadCRC
//	}
//	ro := actualSegment.(*readonlySegment)
//	position = fileOffset(ro.idxMappedFile, ro.baseOffset, offset)
//	cursor := position
//	_ = readInt(ro.txnMappedFile, cursor)
//	cursor += SizeLen
//	previousCRC = readInt(ro.txnMappedFile, cursor)
//	cursor += CrcLen
//	payloadCRC = readInt(ro.txnMappedFile, cursor)
//	return previousCRC, payloadCRC
//}
