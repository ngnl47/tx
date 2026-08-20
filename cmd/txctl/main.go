// txctl is a small operational client for the optional tx control service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	if flags.NArg() != 1 {
		return errors.New("usage: txctl --endpoint host:port [--timeout 30s] status|drain|wait-drained")
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
	client := controlpb.NewLifecycleServiceClient(connection)
	var response *controlpb.LifecycleStatus
	switch flags.Arg(0) {
	case "status":
		response, err = client.Status(ctx, &controlpb.Empty{})
	case "drain":
		response, err = client.Drain(ctx, &controlpb.Empty{})
	case "wait-drained":
		response, err = client.WaitDrained(ctx, &controlpb.Empty{})
	default:
		return fmt.Errorf("unknown command %q", flags.Arg(0))
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
