package common

import "time"

type PersistentVars struct {
	LogLevelStr      string
	PprofEnable      bool
	PprofBindAddress string
	ServiceAddress   string
	Namespace        string
	RequestTimeout   time.Duration
}

var ProofPv = &PersistentVars{}
