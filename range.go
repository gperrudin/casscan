package casscanner

import (
	"math"
	"math/big"
)

type tokenRange struct {
	// from is the lower bound of the range (exclusive)
	from *int64
	// to is the upper bound of the range (inclusive)
	to *int64
}

// splitTokenRing splits the cassandra token ring into nbSplits ranges.
func splitTokenRing(nbSplits int) []tokenRange {
	min := int64(math.MinInt64)
	max := int64(math.MaxInt64)

	card := new(big.Int).Sub(big.NewInt(max), big.NewInt(min))
	step := new(big.Int).Div(card, big.NewInt(int64(nbSplits)))

	ranges := make([]tokenRange, 0, nbSplits)

	for i := 0; i < nbSplits; i++ {
		var rng tokenRange

		// from = i * step or nil if first range
		from := big.NewInt(min)
		from = from.Add(from, new(big.Int).Mul(big.NewInt(int64(i)), step))
		fromVal := from.Int64()

		if i == 0 {
			rng.from = nil
		} else {
			rng.from = &fromVal
		}

		// to = (i + 1) * step or max if last range
		if i == nbSplits-1 {
			rng.to = &max
		} else {
			// to = (i + 1) * step
			to := new(big.Int).Add(from, step).Int64()
			rng.to = &to
		}

		ranges = append(ranges, rng)
	}

	return ranges
}
