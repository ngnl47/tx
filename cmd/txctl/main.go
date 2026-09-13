// txctl 是可选 tx 控制服务的轻量运维客户端。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/ngnl47/tx/modules/control"
	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

const defaultTimeout = 30 * time.Second

type dialFunc func(context.Context, string) (net.Conn, error)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, nil); err != nil {
		fmt.Fprintln(os.Stderr, "txctl:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, arguments []string, output io.Writer, dialer dialFunc) error {
	flags := flag.NewFlagSet("txctl", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	endpoint := flags.String("endpoint", "", "control gRPC endpoint")
	timeout := flags.Duration("timeout", defaultTimeout, "RPC deadline")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *endpoint == "" {
		return errors.New("--endpoint is required")
	}
	if *timeout <= 0 {
		return errors.New("--timeout must be greater than zero")
	}
	if flags.NArg() == 0 {
		return errors.New("usage: txctl --endpoint host:port [--timeout 30s] status|drain|wait-drained|prepare-handoff|create-slot|promote-slot|storage-io")
	}
	command := flags.Arg(0)
	commandArguments := flags.Args()[1:]
	var creation *controlpb.CreateSlotRequest
	var promotion *controlpb.PromoteSlotRequest
	var handoffInstance string
	if command == "create-slot" {
		var err error
		creation, err = parseCreateSlot(commandArguments)
		if err != nil {
			return err
		}
	} else if command == "promote-slot" {
		var err error
		promotion, err = parsePromoteSlot(commandArguments)
		if err != nil {
			return err
		}
	} else if command == "prepare-handoff" {
		options := flag.NewFlagSet(command, flag.ContinueOnError)
		options.SetOutput(io.Discard)
		options.StringVar(&handoffInstance, "instance", "", "预期源运行实例 ID")
		if err := options.Parse(commandArguments); err != nil {
			return err
		}
		if options.NArg() != 0 || strings.TrimSpace(handoffInstance) == "" {
			return errors.New("prepare-handoff requires --instance and no positional arguments")
		}
	} else if len(commandArguments) != 0 {
		return fmt.Errorf("command %q does not accept arguments", command)
	}

	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if dialer != nil {
		options = append(options, grpc.WithContextDialer(dialer))
	}
	connection, err := grpc.NewClient("passthrough:///"+*endpoint, options...)
	if err != nil {
		return fmt.Errorf("connect control endpoint: %w", err)
	}
	defer connection.Close()

	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	lifecycleClient := controlpb.NewLifecycleServiceClient(connection)
	var response *controlpb.LifecycleStatus
	switch command {
	case "prepare-handoff":
		err := control.NewHandoffClient(connection).PrepareHandoff(ctx, handoffInstance)
		if err != nil {
			return err
		}
		if output == nil {
			output = io.Discard
		}
		_, err = fmt.Fprintf(output, "prepared=true runtimeInstance=%s\n", handoffInstance)
		return err
	case "storage-io":
		snapshot, err := controlpb.NewStorageIOServiceClient(connection).Snapshot(ctx, &controlpb.Empty{})
		if err != nil {
			return err
		}
		encoded, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(snapshot)
		if err != nil {
			return err
		}
		if output == nil {
			output = io.Discard
		}
		_, err = fmt.Fprintln(output, string(encoded))
		return err
	case "status":
		response, err = lifecycleClient.Status(ctx, &controlpb.Empty{})
	case "drain":
		response, err = lifecycleClient.Drain(ctx, &controlpb.Empty{})
	case "wait-drained":
		response, err = lifecycleClient.WaitDrained(ctx, &controlpb.Empty{})
	case "create-slot":
		created, createErr := controlpb.NewSlotServiceClient(connection).CreateSlot(ctx, creation)
		if createErr != nil {
			return createErr
		}
		if created == nil || created.GetAssignment() == nil {
			return errors.New("control service returned an empty slot assignment")
		}
		printSlotAssignment(output, created.GetAssignment())
		return nil
	case "promote-slot":
		promoted, promoteErr := controlpb.NewSlotServiceClient(connection).PromoteSlot(ctx, promotion)
		if promoteErr != nil {
			return promoteErr
		}
		if promoted == nil || promoted.GetAssignment() == nil {
			return errors.New("control service returned an empty slot assignment")
		}
		printSlotAssignment(output, promoted.GetAssignment())
		return nil
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("control service returned an empty lifecycle status")
	}
	if output == nil {
		output = io.Discard
	}
	state := lifecycleStateName(response.GetState())
	fmt.Fprintf(output, "state=%s\n", state)
	if response.GetError() != "" {
		fmt.Fprintf(output, "error=%s\n", response.GetError())
	}
	if response.GetState() == controlpb.LifecycleState_LIFECYCLE_STATE_FAILED {
		return fmt.Errorf("lifecycle state failed: %s", response.GetError())
	}
	return nil
}

func parseCreateSlot(arguments []string) (*controlpb.CreateSlotRequest, error) {
	flags := flag.NewFlagSet("create-slot", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	slotID := flags.String("slot", "", "logical slot ID")
	instanceID := flags.String("instance", "", "initial running runtime instance ID")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, errors.New("create-slot does not accept positional arguments")
	}
	if strings.TrimSpace(*slotID) == "" || strings.TrimSpace(*instanceID) == "" {
		return nil, errors.New("create-slot requires --slot and --instance")
	}
	return &controlpb.CreateSlotRequest{SlotId: *slotID, RuntimeInstanceId: *instanceID}, nil
}

func printSlotAssignment(output io.Writer, assignment *controlpb.SlotAssignment) {
	if output == nil {
		output = io.Discard
	}
	fmt.Fprintf(output, "slot=%s runtimeInstance=%s generation=%d\n", assignment.GetSlotId(), assignment.GetRuntimeInstanceId(), assignment.GetGeneration())
}

func parsePromoteSlot(arguments []string) (*controlpb.PromoteSlotRequest, error) {
	flags := flag.NewFlagSet("promote-slot", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	slotID := flags.String("slot", "", "logical slot ID")
	currentInstance := flags.String("current-instance", "", "expected current runtime instance ID")
	currentGeneration := flags.Uint64("current-generation", 0, "expected current generation")
	nextInstance := flags.String("next-instance", "", "next running runtime instance ID")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, errors.New("promote-slot does not accept positional arguments")
	}
	if strings.TrimSpace(*slotID) == "" || strings.TrimSpace(*currentInstance) == "" || *currentGeneration == 0 || strings.TrimSpace(*nextInstance) == "" {
		return nil, errors.New("promote-slot requires --slot, --current-instance, --current-generation and --next-instance")
	}
	return &controlpb.PromoteSlotRequest{
		Current: &controlpb.SlotAssignment{
			SlotId:            *slotID,
			RuntimeInstanceId: *currentInstance,
			Generation:        *currentGeneration,
		},
		NextRuntimeInstanceId: *nextInstance,
	}, nil
}

func lifecycleStateName(state controlpb.LifecycleState) string {
	switch state {
	case controlpb.LifecycleState_LIFECYCLE_STATE_RUNNING:
		return "running"
	case controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING:
		return "draining"
	case controlpb.LifecycleState_LIFECYCLE_STATE_DRAINED:
		return "drained"
	case controlpb.LifecycleState_LIFECYCLE_STATE_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}
