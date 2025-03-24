package policies

type Policies struct {
	AntiAffinities []AntiAffinity `json:"antiAffinities,omitempty" yaml:"antiAffinities,omitempty"`
}
