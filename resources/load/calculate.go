package load

// Averages returns the three load figures unchanged — the kernel already
// publishes them in their final form. It exists so every metric package has
// the same read → parse → calculate shape.
func Averages(s Sample) (one, five, fifteen float64) {
	return s.One, s.Five, s.Fifteen
}
