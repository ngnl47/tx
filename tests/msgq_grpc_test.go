package tests

import (
	"slices"
	"testing"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
)

func TestMsgQGRPCDescriptorExposesOnlyExplicitPublishRPC(t *testing.T) {
	methods := make([]string, len(msgqpb.MailboxService_ServiceDesc.Methods))
	for index, method := range msgqpb.MailboxService_ServiceDesc.Methods {
		methods[index] = method.MethodName
	}
	if !slices.Contains(methods, "PubSession") {
		t.Fatalf("service methods = %#v, missing PubSession", methods)
	}
	if slices.Contains(methods, "Pub") {
		t.Fatalf("service methods = %#v, legacy Pub remains", methods)
	}
	requestFields := (&msgqpb.PubSessionRequest{}).ProtoReflect().Descriptor().Fields()
	if requestFields.Len() != 2 || requestFields.ByNumber(1).Name() != "session_id" || requestFields.ByNumber(2).Name() != "payload" {
		t.Fatalf("PubSession request fields = %v", requestFields)
	}
}
