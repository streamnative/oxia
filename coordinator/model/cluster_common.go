package model

type Server struct {
	// Name is the unique identification for clusters
	Name *string `json:"name" yaml:"name"`

	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`

	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

func (sv *Server) GetIdentifier() string {
	if sv.Name == nil {
		return sv.Internal
	}
	return *sv.Name
}
