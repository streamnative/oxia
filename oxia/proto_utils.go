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
		Payload: r.Payload,
		Version: toVersion(r.Version),
	}
}

func toGetRangeResult(r *proto.GetRangeResponse) GetRangeResult {
	return GetRangeResult{
		Keys: r.Keys,
	}
}

func toVersion(version *proto.Version) Version {
	return Version{
		VersionId:         version.VersionId,
		CreatedTimestamp:  version.CreatedTimestamp,
		ModifiedTimestamp: version.ModifiedTimestamp,
	}
}

func toError(status proto.Status) error {
	switch status {
	case proto.Status_OK:
		return nil
	case proto.Status_BAD_VERSION:
		return ErrorBadVersion
	case proto.Status_KEY_NOT_FOUND:
		return ErrorKeyNotFound
	default:
		return ErrorUnknownStatus
	}
}
