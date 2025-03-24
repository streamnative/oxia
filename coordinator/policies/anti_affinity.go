package policies

type UnsatisfiableConstraintAction string

const (
	// DoNotSchedule instructs the scheduler not to schedule the pod
	// when constraints are not satisfied.
	DoNotSchedule UnsatisfiableConstraintAction = "DoNotSchedule"
	// ScheduleAnyway instructs the scheduler to schedule the pod
	// even if constraints are not satisfied.
	ScheduleAnyway UnsatisfiableConstraintAction = "ScheduleAnyway"
)

type AntiAffinity struct {
	Labels                        []string                      `json:"labels" yaml:"labels"`
	UnsatisfiableConstraintAction UnsatisfiableConstraintAction `json:"unsatisfiableConstraintAction" yaml:"unsatisfiableConstraintAction"`
}
