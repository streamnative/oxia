package policies

type UnsatisfiableAction string

const (
	// DoNotSchedule instructs the scheduler not to schedule the pod
	// when constraints are not satisfied.
	DoNotSchedule UnsatisfiableAction = "DoNotSchedule"
	// ScheduleAnyway instructs the scheduler to schedule the pod
	// even if constraints are not satisfied.
	ScheduleAnyway UnsatisfiableAction = "ScheduleAnyway"
)

type AntiAffinity struct {
	Labels              []string            `json:"labels,omitempty" yaml:"labels,omitempty"`
	UnsatisfiableAction UnsatisfiableAction `json:"UnsatisfiableAction,omitempty" yaml:"unsatisfiableAction,omitempty"`
}
