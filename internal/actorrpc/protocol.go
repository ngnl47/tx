package actorrpc

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"strings"
	"unicode/utf8"
)

// ValidateRequest 统一校验内部传输的目标与调用期限，不负责选址或激活。
func ValidateRequest(ctx context.Context, target actorhost.Target, deadline bool) error {
	if _, ok := ctx.Deadline(); deadline && !ok {
		return errors.New("actortrial: RPC requires deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target.Instance == "" || target.Identity.Kind == "" || target.Identity.Key == "" {
		return errors.New("actortrial: invalid request")
	}
	return nil
}

// ResultError 校验内部 Actor 结果信封，非法结果明确失败而不是等超时。
func ResultError(result *actorpb.Result) error {
	if result == nil {
		return errors.New("actortrial: handler returned invalid result")
	}
	if !utf8.ValidString(result.MessageType) || !utf8.ValidString(result.ErrorMessage) {
		return errors.New("actortrial: invalid result text")
	}
	if result.ErrorCode != 0 {
		return &actorhost.RemoteError{Code: result.ErrorCode, Message: result.ErrorMessage}
	}
	if result.ErrorMessage != "" {
		return errors.New("actortrial: error message without code")
	}
	if result.Completed {
		if result.MessageType != "" || result.CommandHash != 0 || len(result.Payload) != 0 {
			return errors.New("actor: completion result contains business payload")
		}
		return nil
	}
	if result.MessageType == "" || result.MessageType != strings.TrimSpace(result.MessageType) {
		return errors.New("actortrial: invalid result")
	}
	return nil
}
