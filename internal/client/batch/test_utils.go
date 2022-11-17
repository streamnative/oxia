package batch

var (
	shardId = uint32(1)
	one     = int64(1)
	two     = int64(2)
)

func add(batch batch, call any) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	batch.add(call)
	return
}
