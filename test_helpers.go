package gotoqueue

import (
	"crypto/rand"
	"math/big"
)

// AddRandomElementsToSlice adds n random elements to the provided slice
func AddRandomElementsToSlice(original []string, n int) ([]string, error) {
	// Define possible elements to add
	possibleElements := []string{
		"alpha", "beta", "gamma", "delta", "epsilon",
		"zeta", "eta", "theta", "iota", "kappa",
		"lambda", "mu", "nu", "xi", "omicron",
		"pi", "rho", "sigma", "tau", "upsilon",
		"phi", "chi", "psi", "omega",
	}

	result := make([]string, len(original))
	copy(result, original)

	max := big.NewInt(int64(len(possibleElements)))

	for i := 0; i < n; i++ {
		randomIndex, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, err
		}
		result = append(result, possibleElements[randomIndex.Int64()])
	}

	return result, nil
}
