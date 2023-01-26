// Copyright 2023 StreamNative, Inc.
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

package oxia

import "oxia/proto"

func toPutResult(r *proto.PutResponse) PutResult {
	if err := toError(r.Status); err != nil {
		return PutResult{
			Err: err,
		}
	}
	return PutResult{
		Version: toVersion(r.Version),
	}
}

func toDeleteResult(r *proto.DeleteResponse) error {
	return toError(r.Status)
}

func toDeleteRangeResult(r *proto.DeleteRangeResponse) error {
	return toError(r.Status)
}

func toGetResult(r *proto.GetResponse) GetResult {
	if err := toError(r.Status); err != nil {
		return GetResult{
			Err: err,
		}
	}
	return GetResult{
		Value:   r.Value,
		Version: toVersion(r.Version),
	}
}

func toListResult(r *proto.ListResponse) ListResult {
	return ListResult{
		Keys: r.Keys,
	}
}

func toVersion(version *proto.Version) Version {
	v := Version{
		VersionId:          version.VersionId,
		ModificationsCount: version.ModificationsCount,
		CreatedTimestamp:   version.CreatedTimestamp,
		ModifiedTimestamp:  version.ModifiedTimestamp,
		Ephemeral:          version.SessionId != nil,
	}
	if version.ClientIdentity != nil {
		v.ClientIdentity = *version.ClientIdentity
	}

	return v
}

func toError(status proto.Status) error {
	switch status {
	case proto.Status_OK:
		return nil
	case proto.Status_UNEXPECTED_VERSION_ID:
		return ErrorUnexpectedVersionId
	case proto.Status_KEY_NOT_FOUND:
		return ErrorKeyNotFound
	default:
		return ErrorUnknownStatus
	}
}
