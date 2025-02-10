package model

type NodeInfo struct {
	// ID is the server's unique identifier
	ID *string `json:"id" yaml:"id"`
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`
	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

func (info *NodeInfo) GetID() string {
	if info.ID == nil {
		return info.Internal
	}
	return *info.ID
}
