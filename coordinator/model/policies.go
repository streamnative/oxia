package model

type UnsatisfiableConstraintAction string

const (
	// DoNotSchedule instructs the scheduler not to schedule the shard
	DoNotSchedule UnsatisfiableConstraintAction = "DoNotSchedule"
	// ScheduleAnyway instructs the scheduler to schedule the shard
	ScheduleAnyway UnsatisfiableConstraintAction = "ScheduleAnyway"
)

type HierarchyPolicies struct {
	AntiAffinity AntiAffinity `json:"antiAffinity" yaml:"antiAffinity"`
}

type AntiAffinity struct {
	Labels            map[string]string             `json:"labels" yaml:"labels"`
	WhenUnsatisfiable UnsatisfiableConstraintAction `json:"whenUnsatisfiable" yaml:"whenUnsatisfiable"`
}
