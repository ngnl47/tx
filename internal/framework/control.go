package framework

import (
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/control"
)

// controlServer 封装进程控制面的启停和端点查询。
type controlServer struct {
	server *control.Server
}

type controlServerOptions = control.ServerOptions

func startControlServer(root *lifecycle.WaitGroupContext, endpoint string, options controlServerOptions) (*controlServer, error) {
	server, err := control.StartServer(root, endpoint, options)
	if err != nil {
		return nil, err
	}
	if server == nil {
		return nil, nil
	}
	return &controlServer{server: server}, nil
}

func (s *controlServer) endpoint() string {
	if s == nil {
		return ""
	}
	return s.server.Endpoint()
}

func (s *controlServer) stop() {
	if s == nil {
		return
	}
	s.server.Stop()
}
