package domain

// Currency is an ISO 4217 alphabetic code. Amounts are always int64 minor units
// (cents, yen, ...) - never floats - so arithmetic is exact and comparable.
type Currency string

func (c Currency) Valid() bool {
	if len(c) != 3 {
		return false
	}
	for _, r := range c {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

// MaxBalance keeps balance+amount safely inside int64.
const MaxBalance int64 = 1 << 62

// ValidAmount rejects zero, negatives and anything that could overflow when
// summed with a balance that is itself bounded by MaxBalance.
func ValidAmount(minor int64) bool { return minor > 0 && minor <= MaxBalance }
