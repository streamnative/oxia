# Oxia Go client API

The full GoDoc reference for the Oxia Go client can be found at: https://pkg.go.dev/github.com/streamnative/oxia/oxia.

A simple example on how to write and read records:

```go
// import "github.com/streamnative/oxia/oxia"

client, err := oxia.NewSyncClient("localhost:6648")
if err != nil {
	return err
}

insertedKey, v1, err := client.Put(context.Background(), "/my-key", []byte("my-value"))
if err != nil {
    return err
}

storedKey, res, version, err := client.Get(context.Background(), "/my-key")
if err != nil {
    return err
}

fmt.Println(res)
```

## Async client

The above example is using the simpler "sync" client interface. While the sync interface is easier to use, the calls
are blocking and that makes it harder to submit many concurrent requests and achieve high throughput.

Oxia also provides an "async" client API that makes use of channels to track the completions of the read/write operations.

With the async client API, a single go-routine can submit many concurrent requests. In addition, the Oxia client library
will automatically batch the request for better performance.

```go
client, err := oxia.NewAsyncClient("localhost:6648",
		                    oxia.WithBatchLinger(10*time.Millisecond))

c1 := client.Put("/key-1", []byte("hello"))
c2 := client.Put("/key-2", []byte("hello"))
c3 := client.Put("/key-3", []byte("hello"))

if res := <-c1; res.Err != nil {
    return res.Err
}
if res := <-c2; res.Err != nil {
    return res.Err
}
if res := <-c3; res.Err != nil {
    return res.Err
}
```

## Client Options

When creating a client, you can customize its behavior with various options:

```go
client, err := oxia.NewSyncClient("localhost:6648",
    oxia.WithNamespace("my-namespace"),
    oxia.WithBatchLinger(10*time.Millisecond),
    oxia.WithMaxRequestsPerBatch(100),
    oxia.WithRequestTimeout(5*time.Second),
    oxia.WithSessionTimeout(30*time.Second),
    oxia.WithGlobalMeterProvider(),
)
```

Available client options include:

| Option | Description | Default |
|--------|-------------|---------|
| `WithNamespace(namespace string)` | Set the Oxia namespace to use | `"default"` |
| `WithBatchLinger(duration time.Duration)` | How long to wait before sending a batched request | `5ms` |
| `WithMaxRequestsPerBatch(count int)` | Maximum number of requests in a batch | `1000` |
| `WithRequestTimeout(duration time.Duration)` | How long to wait for responses before cancelling | `30s` |
| `WithSessionTimeout(duration time.Duration)` | Session timeout for ephemeral records | `15s` |
| `WithMeterProvider(provider metric.MeterProvider)` | Custom OpenTelemetry meter provider | `noop.NewMeterProvider()` |
| `WithGlobalMeterProvider()` | Use the global OpenTelemetry meter provider | - |
| `WithIdentity(identity string)` | Set a custom client identity | Random UUID |
| `WithTLS(config *tls.Config)` | Configure TLS for secure connections | `nil` |
| `WithAuthentication(auth auth.Authentication)` | Configure authentication | `nil` |

## Namespaces

A client can use a particular Oxia namespace, other than `default`, by specifying an option in the client instantiation:

```go
client, err := oxia.NewSyncClient("localhost:6648", oxia.WithNamespace("my-namespace"))
```

All the operations will be referring to that particular namespace and there are no key conflicts across namespaces.

## Authentication

Oxia supports token-based authentication:

```go
// Using a static token
auth := auth.NewTokenAuthenticationWithToken("my-token", true)
client, err := oxia.NewSyncClient("localhost:6648", oxia.WithAuthentication(auth))

// Using a token provider function
tokenProvider := func() string {
    // Fetch or generate token dynamically
    return "my-dynamic-token"
}
auth := auth.NewTokenAuthenticationWithFunc(tokenProvider, true)
client, err := oxia.NewSyncClient("localhost:6648", oxia.WithAuthentication(auth))
```

## Conditional Operations

You can make operations conditional on the current version of a record:

```go
// Put operation that succeeds only if the record has the expected version
insertedKey, version, err := client.Put(context.Background(), "/my-key", 
    []byte("new-value"), oxia.ExpectedVersionId(existingVersion.VersionId))

// Put operation that succeeds only if the record doesn't exist
insertedKey, version, err := client.Put(context.Background(), "/my-key", 
    []byte("new-value"), oxia.ExpectedRecordNotExists())

// Delete operation that succeeds only if the record has the expected version
err := client.Delete(context.Background(), "/my-key", 
    oxia.ExpectedVersionId(existingVersion.VersionId))
```

## Partition Keys

You can ensure that records are co-located in the same Oxia shard by using partition keys:

```go
// Put operation with a partition key
insertedKey, version, err := client.Put(context.Background(), "/my-key", 
    []byte("value"), oxia.PartitionKey("my-partition"))

// Get operation with the same partition key
storedKey, value, version, err := client.Get(context.Background(), "/my-key", 
    oxia.PartitionKey("my-partition"))
```

## Sequential Keys

You can create records with server-assigned sequential keys:

```go
// Create a record with a sequential key
// The key will be "/my-key-prefix-1" where "1" is the first available sequence number
insertedKey, version, err := client.Put(context.Background(), "/my-key-prefix-", 
    []byte("value"), 
    oxia.PartitionKey("my-partition"),
    oxia.SequenceKeysDeltas(1))

// Create multiple records with sequential keys in a single operation
// The keys will be "/my-key-prefix-1", "/my-key-prefix-2", "/my-key-prefix-4"
insertedKey, version, err := client.Put(context.Background(), "/my-key-prefix-", 
    []byte("value"), 
    oxia.PartitionKey("my-partition"),
    oxia.SequenceKeysDeltas(1, 1, 2))
```

## Secondary Indexes

You can create secondary indexes for your records to enable efficient lookups:

```go
// Put operation with secondary indexes
insertedKey, version, err := client.Put(context.Background(), "/user/123", 
    []byte(`{"name":"John","email":"john@example.com"}`), 
    oxia.SecondaryIndex("email", "john@example.com"),
    oxia.SecondaryIndex("name", "John"))

// List records using a secondary index
keys, err := client.List(context.Background(), "j", "k", 
    oxia.UseIndex("name"))

// Range scan using a secondary index
results := client.RangeScan(context.Background(), "j", "k", 
    oxia.UseIndex("name"))
for result := range results {
    if result.Err != nil {
        // Handle error
        continue
    }
    fmt.Printf("Key: %s, Value: %s\n", result.Key, string(result.Value))
}
```

## Key Comparison Operations

Oxia supports various key comparison operations for Get requests:

```go
// Get the exact key (default behavior)
storedKey, value, version, err := client.Get(context.Background(), "/my-key", 
    oxia.ComparisonEqual())

// Get the highest key that is <= the given key
storedKey, value, version, err := client.Get(context.Background(), "/my-key", 
    oxia.ComparisonFloor())

// Get the lowest key that is >= the given key
storedKey, value, version, err := client.Get(context.Background(), "/my-key", 
    oxia.ComparisonCeiling())

// Get the highest key that is < the given key
storedKey, value, version, err := client.Get(context.Background(), "/my-key", 
    oxia.ComparisonLower())

// Get the lowest key that is > the given key
storedKey, value, version, err := client.Get(context.Background(), "/my-key", 
    oxia.ComparisonHigher())
```

## Range Operations

Oxia provides operations to work with ranges of keys:

```go
// List all keys in a range
keys, err := client.List(context.Background(), "/users/", "/users/z")

// Scan all records in a range
results := client.RangeScan(context.Background(), "/users/", "/users/z")
for result := range results {
    if result.Err != nil {
        // Handle error
        continue
    }
    fmt.Printf("Key: %s, Value: %s\n", result.Key, string(result.Value))
}

// Delete all records in a range
err := client.DeleteRange(context.Background(), "/users/", "/users/z")
```

## Notifications

Client can subscribe to receive a feed of notification with all the events happening in the namespace they're using.

Notifications can be used to replicate exactly the state of an oxia namespace or to invalidate a cache.

```go
client, err := oxia.NewSyncClient("localhost:6648")
notifications, err := client.GetNotifications()
if err != nil {
    return err
}

// Print a log line for each event
for notification := range notifications.Ch() {
    log.Info().
        Stringer("type", notification.Type).
        Str("key", notification.Key).
        Int64("version-id", notification.VersionId).
        Msg("")
}
```

Notification types include:
- `KeyCreated`: A record that didn't exist was created
- `KeyModified`: An existing record was modified
- `KeyDeleted`: A record was deleted
- `KeyRangeRangeDeleted`: A range of keys was deleted

## Ephemeral records

Applications can create records that will automatically be removed once the client session expires.

```go
client, err := oxia.NewSyncClient("localhost:6648")
insertedKey, version, err := client.Put(context.Background(), "/my-key", []byte("my-value"), oxia.Ephemeral())
```

Ephemeral records have their lifecycle tied to a particular client instance, and they
are automatically deleted when the client instance is closed.

These records are also deleted if the client cannot communicate with the Oxia
service for some extended amount of time, and the session between the client and
the service "expires".

Application can control the session behavior by setting the session timeout
appropriately with `oxia.WithSessionTimeout()` option when creating the client instance.

## Caching values in client

Oxia client provides a built-in optional cache that will store the deserialized values.

Example:

```go
type myStruct struct {
    A string `json:"a"`
    B int    `json:"b"`
}

client, _ := NewSyncClient(serviceAddress)
// Create a cache specialized for `myStruct`, using JSON serialization
cache, _ := NewCache[myStruct](client, json.Marshal, json.Unmarshal)

// We can pass a struct to the Put(), instead of bytes
insertedKey, v1, err := cache.Put(context.Background(), "/my-key", myStruct{"hello", 1})

// The returned value is already deserialized
value, version, err := cache.Get(context.Background(), "/my-key")
fmt.Printf("A: %s - B: %d\n", value.A, value.B)

// We can also do atomic read-modify-updates through the cache
// This will not incur in a read from the server if the value is already
// in cache and up to the latest version
err = cache.ReadModifyUpdate(context.Background(), "/my-key",
    func(existingValue Optional[myStruct]) (myStruct, error) {
        return myStruct{
            A: existingValue.MustGet().A,
            B: 3,
        }, nil
    })
```

### Consistency

The cache is kept up to date using Oxia notification, to invalidate whenever a record is updated.

Changes done through the cache are also immediately reflected in the cache. For updates done outside the cache instance,
the cache will be eventually consistent, meaning that a cache read could return a stale value for a short amount of time.

## Optional Type

Oxia provides an `Optional` type for handling values that may or may not be present:

```go
// Create an optional with a value
opt := oxia.optionalOf(myValue)

// Create an empty optional
opt := oxia.empty[MyType]()

// Check if a value is present
if opt.Present() {
    // Value is present
}

// Check if a value is absent
if opt.Empty() {
    // Value is not present
}

// Get the value safely
if value, ok := opt.Get(); ok {
    // Use value
}

// Get the value, panicking if not present
value := opt.MustGet()
```

## Error Handling

Oxia defines several error types that you should handle in your application:

```go
// Check for specific error types
if errors.Is(err, oxia.ErrKeyNotFound) {
    // Handle key not found error
} else if errors.Is(err, oxia.ErrUnexpectedVersionId) {
    // Handle version conflict error
} else if errors.Is(err, oxia.ErrRequestTooLarge) {
    // Handle request too large error
} else if errors.Is(err, oxia.ErrInvalidOptions) {
    // Handle invalid options error
} else {
    // Handle other errors
}
```