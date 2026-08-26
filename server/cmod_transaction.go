package server

func (server *Server) transactionCMod() CMod {
	return &builtInCMod{
		spec:  CModSpec{ID: CModTransaction},
		start: func(CModContext) error { return server.startTransaction() },
	}
}
