//go:build !windows

package main

func getSystemMemory() (totalMB, availMB, usedPercent uint64, err error) {
	return 0, 0, 0, nil
}
