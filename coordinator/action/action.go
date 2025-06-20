package action

type Type string

const (
	SwapNode Type = "swap-node"
	Election Type = "election"
)

type Action interface {
	Type() Type

	Done(t any)
}
