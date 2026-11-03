package msgq

import (
	"context"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"time"
)

func initToProto(request InitRequest) *msgqpb.InitRequest {
	result := &msgqpb.InitRequest{Header: headerToProto(request.Header), Restore: request.Restore, Ack: request.Ack}
	if a := request.Admission; a != nil {
		result.Admission = &msgqpb.SessionAdmission{ReplaceExisting: a.Policy == ReplaceExisting, TicketOrder: a.TicketOrder, ExpiresAtUnixMilli: a.ExpiresAt.UnixMilli()}
	}
	return result
}
func initFromProto(request *msgqpb.InitRequest) InitRequest {
	result := InitRequest{Header: headerFromProto(request.GetHeader()), Restore: request.GetRestore(), Ack: request.GetAck()}
	if a := request.GetAdmission(); a != nil {
		policy := RejectExisting
		if a.GetReplaceExisting() {
			policy = ReplaceExisting
		}
		result.Admission = &SessionAdmission{Policy: policy, TicketOrder: a.GetTicketOrder(), ExpiresAt: time.UnixMilli(a.GetExpiresAtUnixMilli())}
	}
	return result
}
func (s *mailboxService) TerminateLogicalSession(ctx context.Context, r *msgqpb.LogicalSessionRequest) (*msgqpb.Empty, error) {
	return emptyResponse(s.mailbox.TerminateLogicalSession(ctx, r.GetRoleId(), r.GetSessionId()))
}
func (c *grpcConnection) TerminateLogicalSession(ctx context.Context, roleID, sessionID int64) error {
	_, err := c.client.TerminateLogicalSession(ctx, &msgqpb.LogicalSessionRequest{RoleId: roleID, SessionId: sessionID})
	return mailboxClientError(err)
}
func (s *mailboxService) InspectSession(ctx context.Context, r *msgqpb.InspectSessionRequest) (*msgqpb.AdmissionRecord, error) {
	record, err := s.mailbox.InspectSession(ctx, r.GetRoleId())
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.AdmissionRecord{SessionId: record.SessionID, TicketOrder: record.TicketOrder, Header: headerToProto(record.Header), Ready: record.Ready, Attached: record.Attached, ExpiresAtUnixMilli: record.ExpiresAt.UnixMilli()}, nil
}
func (c *grpcConnection) InspectSession(ctx context.Context, roleID int64) (AdmissionRecord, error) {
	record, err := c.client.InspectSession(ctx, &msgqpb.InspectSessionRequest{RoleId: roleID})
	if err != nil {
		return AdmissionRecord{}, mailboxClientError(err)
	}
	return AdmissionRecord{SessionID: record.GetSessionId(), TicketOrder: record.GetTicketOrder(), Header: headerFromProto(record.GetHeader()), Ready: record.GetReady(), Attached: record.GetAttached(), ExpiresAt: time.UnixMilli(record.GetExpiresAtUnixMilli())}, nil
}
