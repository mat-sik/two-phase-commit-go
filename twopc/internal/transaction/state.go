package transaction

type State int

const (
	NotStarted State = iota
	Prepared
	PrepareFailed
	Committed
	RolledBack
)
