
# Oxia Keys Sorting

Oxia uses a custom comparison operator for sorting the keys of the stored records. 

In Oxia all the keys are independent of each other and there is no explicit tree-like structure
with parent-children relations.

Having said that, Oxia assign some special meaning to the `/` character in keys. This is done in
order to ensure efficient traversal of small portions of the key space.

In the specific, Oxia ensure that keys with a common prefix and the same number of `/` segments
are sorted in such a way that they are stored close to each other in the database.

For example, if we are considering the following keys and the order in which they will be stored:
 * `/xyz/A`
 * `/xyz/B`
 * `/xyz/C`
 * `/xyz/A/1`
 * `/xyz/A/2`
 * `/xyz/B/1`
 * `/xyz/A/1/a`

This sorting, aware of `/` characters, makes possible to do efficient range queries to 
retrieve the list of "first level children" of a given key.

For example, in Go: 

```go
client.List(context.Background(), "/xyz/", "/xyz//")
```

This will return a list with `["/xyz/A", "/xyz/B", "/xyz/C"]`, doing the minimum scan in the database.
