# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [client/client.proto](#client_client-proto)
    - [BatchRequest](#io-streamnative-oxia-client-BatchRequest)
    - [BatchResponse](#io-streamnative-oxia-client-BatchResponse)
    - [DeleteRangeRequest](#io-streamnative-oxia-client-DeleteRangeRequest)
    - [DeleteRangeResponse](#io-streamnative-oxia-client-DeleteRangeResponse)
    - [DeleteRequest](#io-streamnative-oxia-client-DeleteRequest)
    - [DeleteResponse](#io-streamnative-oxia-client-DeleteResponse)
    - [Error](#io-streamnative-oxia-client-Error)
    - [GetRangeRequest](#io-streamnative-oxia-client-GetRangeRequest)
    - [GetRangeResponse](#io-streamnative-oxia-client-GetRangeResponse)
    - [GetRequest](#io-streamnative-oxia-client-GetRequest)
    - [GetResponse](#io-streamnative-oxia-client-GetResponse)
    - [PutRequest](#io-streamnative-oxia-client-PutRequest)
    - [PutResponse](#io-streamnative-oxia-client-PutResponse)
    - [Shard](#io-streamnative-oxia-client-Shard)
    - [ShardAssignment](#io-streamnative-oxia-client-ShardAssignment)
    - [ShardAssignmentsRequest](#io-streamnative-oxia-client-ShardAssignmentsRequest)
    - [ShardAssignmentsResponse](#io-streamnative-oxia-client-ShardAssignmentsResponse)
    - [Stat](#io-streamnative-oxia-client-Stat)
  
    - [ErrorType](#io-streamnative-oxia-client-ErrorType)
  
    - [OxiaClient](#io-streamnative-oxia-client-OxiaClient)
  
- [Scalar Value Types](#scalar-value-types)



<a name="client_client-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## client/client.proto



<a name="io-streamnative-oxia-client-BatchRequest"></a>

### BatchRequest
A batch request. Applies the batches of requests. Requests are processed in
positional order within batches and the batch types are processed in the
following order: puts, deletes, delete_ranges, gets, get_ranges.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) | optional | The shard id. This is also the minimum inclusive hash contained by the shard. The maximum exclusive hash is the shard id of the adjacent shard. This is optional allow for support for server-side hashing and proxying in the future. |
| puts | [PutRequest](#io-streamnative-oxia-client-PutRequest) | repeated | The put requests |
| deletes | [DeleteRequest](#io-streamnative-oxia-client-DeleteRequest) | repeated | The delete requests |
| delete_ranges | [DeleteRangeRequest](#io-streamnative-oxia-client-DeleteRangeRequest) | repeated | The delete range requests |
| gets | [GetRequest](#io-streamnative-oxia-client-GetRequest) | repeated | The get requests |
| get_ranges | [GetRangeRequest](#io-streamnative-oxia-client-GetRangeRequest) | repeated | The rangd requests |






<a name="io-streamnative-oxia-client-BatchResponse"></a>

### BatchResponse
The response to a batch request. Responses of each type respect the order
of the original requests.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| puts | [PutResponse](#io-streamnative-oxia-client-PutResponse) | repeated | The put responses |
| deletes | [DeleteResponse](#io-streamnative-oxia-client-DeleteResponse) | repeated | The delete responses |
| delete_ranges | [DeleteRangeResponse](#io-streamnative-oxia-client-DeleteRangeResponse) | repeated | The delete range responses |
| gets | [GetResponse](#io-streamnative-oxia-client-GetResponse) | repeated | The get responses |
| get_ranges | [GetRangeResponse](#io-streamnative-oxia-client-GetRangeResponse) | repeated | The get range responses |






<a name="io-streamnative-oxia-client-DeleteRangeRequest"></a>

### DeleteRangeRequest
A delete range request. Deletes all keys that exist within the bounds of the
range. The client should broadcast this request to all shards.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_inclusive | [string](#string) |  | The start of the range, inclusive |
| end_exclusive | [string](#string) |  | The end of the range, exclusive |






<a name="io-streamnative-oxia-client-DeleteRangeResponse"></a>

### DeleteRangeResponse
The response for a delete range request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deletes | [DeleteResponse](#io-streamnative-oxia-client-DeleteResponse) | repeated | All the keys deleted within the specified range |






<a name="io-streamnative-oxia-client-DeleteRequest"></a>

### DeleteRequest
A delete request. Deletes the specified key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key |
| expected_version | [uint64](#uint64) | optional | An optional expected version. The delete will fail if the actual version does not match |






<a name="io-streamnative-oxia-client-DeleteResponse"></a>

### DeleteResponse
The response to a delete request or an item in a response to the
delete range request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key |
| error | [Error](#io-streamnative-oxia-client-Error) | optional | The error if the delete failed |






<a name="io-streamnative-oxia-client-Error"></a>

### Error
One or more items in a batch may fail. For this scenario an error must be
returned for a single item so that the whole batch is not failed and can
continue being processed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ErrorType](#io-streamnative-oxia-client-ErrorType) |  | The specific error type |
| message | [string](#string) |  | The message associated with the error |






<a name="io-streamnative-oxia-client-GetRangeRequest"></a>

### GetRangeRequest
A get range request. Gets all keys that exist within the bounds of the
range. The client should broadcast this request to all shards.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_inclusive | [string](#string) |  | The start of the range, inclusive |
| end_exclusive | [string](#string) |  | The end of the range, exclusive |
| include_payloads | [bool](#bool) |  | Specified whether response should include the payloads or only the stat |






<a name="io-streamnative-oxia-client-GetRangeResponse"></a>

### GetRangeResponse
The response to a get range request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| gets | [GetResponse](#io-streamnative-oxia-client-GetResponse) | repeated | All the keys found within the specified range |






<a name="io-streamnative-oxia-client-GetRequest"></a>

### GetRequest
A get request. Gets the stat and optionally the payload for the specified
key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key |
| include_payload | [bool](#bool) |  | Specifies whether the response should include the payload |






<a name="io-streamnative-oxia-client-GetResponse"></a>

### GetResponse
The response to a get request or an item in a response to the get range
request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key |
| payload | [bytes](#bytes) | optional | The payload, if it was requested and there was no error |
| stat | [Stat](#io-streamnative-oxia-client-Stat) |  | The stat if the key exists |
| error | [Error](#io-streamnative-oxia-client-Error) |  | The error if the get failed |






<a name="io-streamnative-oxia-client-PutRequest"></a>

### PutRequest
A put request. Persists the specified key and payload


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key |
| payload | [bytes](#bytes) |  | The payload |
| expected_version | [uint64](#uint64) | optional | An optional expected version. The put will fail if the actual version does not match |






<a name="io-streamnative-oxia-client-PutResponse"></a>

### PutResponse
The response to a put request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The key |
| stat | [Stat](#io-streamnative-oxia-client-Stat) |  | The stat if the put was successful |
| error | [Error](#io-streamnative-oxia-client-Error) |  | The error if the put failed |






<a name="io-streamnative-oxia-client-Shard"></a>

### Shard
A shard with the range of hashes that it can contain.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint32](#uint32) |  | The shard id |
| min_hash_inclusive | [uint32](#uint32) |  | The minimum inclusive hash that the shard can contain |
| max_hash_exclusive | [uint32](#uint32) |  | The maximum exclusive hash that the shard can contain |






<a name="io-streamnative-oxia-client-ShardAssignment"></a>

### ShardAssignment
The assignment of a shard to a server.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard | [Shard](#io-streamnative-oxia-client-Shard) |  | The shard |
| leader | [string](#string) |  | The shard leader, e.g. `host:port` |






<a name="io-streamnative-oxia-client-ShardAssignmentsRequest"></a>

### ShardAssignmentsRequest
A shard assignments request. Gets all shard-to-server assignments as a
stream. Each set of assignments in the response stream will contain all the
assignments to bring the client up to date. For example, if a shard is split,
the stream will return a single response containing all the new shard
assignments as opposed to multiple stream responses, each containing a single
shard assignment.

Placeholder empty message. `namespace` may be added in the future.






<a name="io-streamnative-oxia-client-ShardAssignmentsResponse"></a>

### ShardAssignmentsResponse
The response to a shard assignments request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| assignments | [ShardAssignment](#io-streamnative-oxia-client-ShardAssignment) | repeated | All assignments in the response stream will contain all the assignments to bring the client up to date. For example, if a shard is split, the stream will return a single response containing all the new shard assignments as opposed to multiple stream responses, each containing a single shard assignment. |






<a name="io-streamnative-oxia-client-Stat"></a>

### Stat
Stats about the current version of a key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  | The current version of the key |
| created_timestamp | [fixed64](#fixed64) |  | The creation timestamp of the first version of the key |
| modified_timestamp | [fixed64](#fixed64) |  | The modified timestamp of the current version of the key |





 


<a name="io-streamnative-oxia-client-ErrorType"></a>

### ErrorType
Represents all the possible errors and that can be encountered.

| Name | Number | Description |
| ---- | ------ | ----------- |
| ERROR_TYPE_UNSPECIFIED | 0 | Unspecified error |
| ERROR_TYPE_KEY_NOT_FOUND | 1 | The key was not found |


 

 


<a name="io-streamnative-oxia-client-OxiaClient"></a>

### OxiaClient
Oxia service that allows clients to discover shard-to-server assignments and
submit batches of requests.

Clients should connect to a random server to discover the shard-to-server
assignments and then send the actual batched requests to the appropriate
shard leader. In the future, this may be handled server-side in a proxy
layer to allows clients to not be concerned with sharding.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ShardAssignments | [ShardAssignmentsRequest](#io-streamnative-oxia-client-ShardAssignmentsRequest) | [ShardAssignmentsResponse](#io-streamnative-oxia-client-ShardAssignmentsResponse) stream | Gets all shard-to-server assignments as a stream. Each set of assignments in the response stream will contain all the assignments to bring the client up to date. For example, if a shard is split, the stream will return a single response containing all the new shard assignments as opposed to multiple stream responses, each containing a single shard assignment.

Clients should connect to a single random server which will stream the assignments for all shards on all servers. |
| Batch | [BatchRequest](#io-streamnative-oxia-client-BatchRequest) | [BatchResponse](#io-streamnative-oxia-client-BatchResponse) | Batches put, delete, delete_range, get and get_range requests.

Clients should send this request to the shard leader. In the future, this may be handled server-side in a proxy layer. |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

