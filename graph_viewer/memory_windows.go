//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

type memoryStatusEx struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

func getSystemMemory() (totalMB, availMB, usedPercent uint64, err error) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")

	var memStatus memoryStatusEx
	memStatus.dwLength = uint32(unsafe.Sizeof(memStatus))

	ret, _, callErr := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0, 0, 0, callErr
	}
	totalMB = memStatus.ullTotalPhys / (1024 * 1024)
	availMB = memStatus.ullAvailPhys / (1024 * 1024)
	usedPercent = uint64(memStatus.dwMemoryLoad)
	return totalMB, availMB, usedPercent, nil
}
