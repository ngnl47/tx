package redis

type documentPatchMode string

const (
	documentPatchModeMerge          documentPatchMode = "0"
	documentPatchModeReplace        documentPatchMode = "1"
	documentPatchModePartialReplace documentPatchMode = "2"
	documentPatchModeOperations     documentPatchMode = "3"
	documentPatchModeKeepEmpty      documentPatchMode = "4"
	documentPatchModeTTLOnly        documentPatchMode = "-1"
)

const (
	scriptFlagFalse = "0"
	scriptFlagTrue  = "1"

	scriptResultOK               = "ok"
	scriptResultBusy             = "busy"
	scriptResultStale            = "stale"
	scriptResultHandoffFull      = "hf"
	scriptResultNotExists        = "ne"
	scriptResultPathTypeMismatch = "pt"
)
