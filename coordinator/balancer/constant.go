package balancer

import "time"

const (
	loadGapRatio                 float64 = 10.0
	loadBalancerScheduleInterval         = time.Second * 30
	quarantineTime                       = time.Minute * 5
)

var triggerEvent = struct{}{}
