package model

type ServerInfo struct {
	// ID is the node unique identifier
	ID *string `json:"id" yaml:"id"`
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`
	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

func (si *ServerInfo) GetID() string {
	if si.ID == nil {
		return si.Internal
	}
	return *si.ID
}
