package config

import "os"

// lookupEnv wraps os.LookupEnv. It returns the value and a boolean indicating
// whether the variable was present in the environment.
func lookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}
