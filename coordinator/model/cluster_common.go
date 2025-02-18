package model

type NodeInfo struct {
	ID *string `json:"id" yaml:"id"`

	Labels map[string]string `json:"labels" yaml:"labels"`

	ServerAddress
}

func (node *NodeInfo) GetID() string {
	if node.ID == nil {
		return node.Internal
	}
	return *node.ID
}
