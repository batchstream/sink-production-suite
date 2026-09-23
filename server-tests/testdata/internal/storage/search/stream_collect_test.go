package search_test

import (
	"context"
	sink "github.com/batchstream/sink-protocol/sink/v1"
	"google.golang.org/grpc"
	"io"
	"sort"
)

func collectWrite(ctx context.Context, client sink.SinkClient, req *sink.WriteRequest, opts ...grpc.CallOption) (*sink.WriteResponse, error) {
	response := &sink.WriteResponse{}
	stream, err := client.Write(ctx, req, opts...)
	if err != nil {
		return response, err
	}
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			sort.Slice(response.Results, func(i, j int) bool { return response.Results[i].OperationIndex < response.Results[j].OperationIndex })
			return response, nil
		}
		if err != nil {
			return response, err
		}
		response.Results = append(response.Results, frame.Results...)
	}
}
