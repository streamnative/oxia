# Oxia Go client API

The full GoDoc reference for the Oxia Go Client can be found at https://pkg.go.dev/github.com/streamnative/oxia/oxia

A simple example on how to write and read records:

```go
// import "github.com/streamnative/oxia/oxia"


client, err := oxia.NewSyncClient("localhost:6648")
if err != nil {
	return err
}

v1, err := client.Put(context.Background(), "/my-key", []byte("my-value"))
if err != nil {
    return err
}

res, version, err := client.Get(context.Background(), "/my-key")
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


## Namespaces

A client can use a particular Oxia namespace, other than `default`, by specifying an option in the client instantiation:

```go
client, err := oxia.NewSyncClient("localhost:6648", oxia.WithNamespace("my-namespace"))
```

All the operations will be referring to that particular namespace and there are no key conflicts across namespaces.

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

## Ephemeral records

Applications can create records that will automatically be removed once the client session expires.

```go
client, err := oxia.NewSyncClient("localhost:6648")
version, err := client.Put(context.Background(), "/my-key", []byte("my-value"), oxia.Ephemeral())
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
v1, err := cache.Put(context.Background(), "/my-key", myStruct{"hello", 1})

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

Change don through the cache are also immediately reflected in the cache. For updates done outside the cache instance, 
the cache will be eventually consistent, meaning that a cache read could return a stale value for a short amount of time.

