package uptime

// Seconds is the published uptimeSeconds: the kernel's figure truncated to
// whole seconds.
func Seconds(s Sample) int64 {
	return int64(s.Seconds)
}
