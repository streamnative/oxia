package impl

//func TestCoordinator(t *testing.T) {
//	common.LogDebug = true
//	common.ConfigureLogger()
//
//	mp := NewMetadataProviderMemory()
//	cc := ClusterConfig{
//		ReplicationFactor: 3,
//		ShardsCount:       1,
//		StorageServers: []ServerAddress{
//			{"localhost:9190", "localhost:8190"},
//			{"localhost:9191", "localhost:8191"},
//			{"localhost:9192", "localhost:8192"},
//			//{"localhost:9193", "localhost:8193"},
//		},
//	}
//	_, err := NewCoordinator(mp, cc)
//	assert.NoError(t, err)
//
//	time.Sleep(5 * time.Second)
//}
//
//// TODO: this is just temporary
//func TestClient(t *testing.T) {
//	common.LogDebug = true
//	common.ConfigureLogger()
//
//	options, err := oxia.NewClientOptions("localhost:9190")
//	assert.NoError(t, err)
//	client := oxia.NewSyncClient(options)
//
//	pres, err := client.Put("k1", []byte("value-1"), nil)
//	assert.NoError(t, err)
//
//	log.Info().
//		Interface("put-response", pres).
//		Msg("Put operation was successful")
//
//	gr, stat, err := client.Get("k1")
//	assert.NoError(t, err)
//
//	log.Info().
//		Str("value", string(gr)).
//		Interface("stat", stat).
//		Msg("Get operation was successful")
//}
