package client

import "net"

func GetFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic("could not find free port")
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic("could not find free port")
	}
	defer func() {
		err := l.Close()
		if err != nil {
			panic("could not find free port")
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}
