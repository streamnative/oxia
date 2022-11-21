package oxia

import "oxia/proto"

func toPutResult(r *proto.PutResponse) PutResult {
	if err := toError(r.Status); err != nil {
		return PutResult{
			Err: err,
		}
	}
	return PutResult{
		Stat: toStat(r.Stat),
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
		Stat:    toStat(r.Stat),
	}
}

func toGetRangeResult(r *proto.GetRangeResponse) GetRangeResult {
	return GetRangeResult{
		Keys: r.Keys,
	}
}

func toStat(stat *proto.Stat) Stat {
	return Stat{
		Version:           stat.Version,
		CreatedTimestamp:  stat.CreatedTimestamp,
		ModifiedTimestamp: stat.ModifiedTimestamp,
	}
}

func toError(status proto.Status) error {
	switch status {
	case proto.Status_OK:
		return nil
	case proto.Status_UNEXPECTED_VERSION:
		return ErrorUnexpectedVersion
	case proto.Status_KEY_NOT_FOUND:
		return ErrorKeyNotFound
	default:
		return ErrorUnknownStatus
	}
}
