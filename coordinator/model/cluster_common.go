package model

type NodeInfo struct {
	// ID is the server's unique identifier
	ID *string `json:"id" yaml:"id"`
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`
	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`

	// Labels are key-value pairs used to categorize or tag the node
	// These can represent various properties of the node, such as its
	// data center or region it belongs to, or any other attributes
	// that help identify or filter nodes in the system (e.g., "zone": "us-east-1").
	Labels map[string]string `json:"labels" yaml:"labels"`
}

func (info *NodeInfo) GetID() string {
	if info.ID == nil {
		return info.Internal
	}
	return *info.ID
}
