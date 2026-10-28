package msgq

// checkStartup 只在首次开放前检查服务准入。本地与远程入口共享此边界，
// 长流不持有 Admission 计数，开放后的排水及收尾继续由邮箱生命周期负责。
func (s *Server) checkStartup() error {
	if s.startupAdmission == nil || s.startupOpened.Load() {
		return nil
	}
	done, err := s.startupAdmission.Enter()
	if err != nil {
		return err
	}
	s.startupOpened.Store(true)
	done()
	return nil
}
