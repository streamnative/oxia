package model

type HierarchyPolicies struct {
	AntiAffinity *AntiAffinityPolicy `json:"antiAffinity,omitempty" yaml:"antiAffinity,omitempty"`
}

const AntiAffinityLevelRequired = "required"
const AntiAffinityLevelPreferred = "preferred"

type AntiAffinityPolicy struct {
	Level  string   `json:"level" yaml:"level"`
	Labels []string `json:"labels" yaml:"labels"`
}
