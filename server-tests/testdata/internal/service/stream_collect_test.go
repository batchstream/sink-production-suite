package service_test

import (
	"context"
	"fmt"
	sink "github.com/batchstream/sink-protocol/sink/v1"
	"google.golang.org/grpc"
	"io"
	"sort"
)

type collectorStream[T any] struct {
	grpc.ServerStream
	ctx  context.Context
	emit func(*T) error
}

func (s *collectorStream[T]) Context() context.Context { return s.ctx }
func (s *collectorStream[T]) Send(frame *T) error      { return s.emit(frame) }
func collectRead(ctx context.Context, target any, req *sink.ReadRequest, opts ...grpc.CallOption) (*sink.ReadResponse, error) {
	response := &sink.ReadResponse{}
	defer func() {
		sort.Slice(response.Results, func(i, j int) bool {
			return response.Results[i].GetOperationIndex() < response.Results[j].GetOperationIndex()
		})
	}()
	emit := func(frame *sink.ReadResponse) error {
		response.Results = append(response.Results, frame.Results...)
		return nil
	}
	switch target := target.(type) {
	case interface {
		Read(context.Context, *sink.ReadRequest) (*sink.ReadResponse, error)
	}:
		return target.Read(ctx, req)
	case interface {
		Read(*sink.ReadRequest, grpc.ServerStreamingServer[sink.ReadResponse]) error
	}:
		stream := &collectorStream[sink.ReadResponse]{ctx: ctx, emit: emit}
		err := target.Read(req, stream)
		return response, err
	case sink.SinkClient:
		stream, err := target.Read(ctx, req, opts...)
		if err != nil {
			return response, err
		}
		for {
			frame, err := stream.Recv()
			if err == io.EOF {
				return response, nil
			}
			if err != nil {
				return response, err
			}
			if err := emit(frame); err != nil {
				return response, err
			}
		}
	default:
		return response, fmt.Errorf("unsupported stream test target %T", target)
	}
}
func collectWrite(ctx context.Context, target any, req *sink.WriteRequest, opts ...grpc.CallOption) (*sink.WriteResponse, error) {
	response := &sink.WriteResponse{}
	defer func() {
		sort.Slice(response.Results, func(i, j int) bool {
			return response.Results[i].GetOperationIndex() < response.Results[j].GetOperationIndex()
		})
	}()
	emit := func(frame *sink.WriteResponse) error {
		response.Results = append(response.Results, frame.Results...)
		return nil
	}
	switch target := target.(type) {
	case interface {
		Write(context.Context, *sink.WriteRequest) (*sink.WriteResponse, error)
	}:
		return target.Write(ctx, req)
	case interface {
		Write(*sink.WriteRequest, grpc.ServerStreamingServer[sink.WriteResponse]) error
	}:
		stream := &collectorStream[sink.WriteResponse]{ctx: ctx, emit: emit}
		err := target.Write(req, stream)
		return response, err
	case sink.SinkClient:
		stream, err := target.Write(ctx, req, opts...)
		if err != nil {
			return response, err
		}
		for {
			frame, err := stream.Recv()
			if err == io.EOF {
				return response, nil
			}
			if err != nil {
				return response, err
			}
			if err := emit(frame); err != nil {
				return response, err
			}
		}
	default:
		return response, fmt.Errorf("unsupported stream test target %T", target)
	}
}
func collectQuery(ctx context.Context, target any, req *sink.QueryRequest, opts ...grpc.CallOption) (*sink.QueryResponse, error) {
	response := &sink.QueryResponse{}

	emit := func(frame *sink.QueryResponse) error {
		response.Documents = append(response.Documents, frame.Documents...)
		response.HasMore = frame.HasMore
		response.Complete = frame.Complete
		return nil
	}
	switch target := target.(type) {
	case interface {
		Query(context.Context, *sink.QueryRequest) (*sink.QueryResponse, error)
	}:
		return target.Query(ctx, req)
	case interface {
		Query(*sink.QueryRequest, grpc.ServerStreamingServer[sink.QueryResponse]) error
	}:
		stream := &collectorStream[sink.QueryResponse]{ctx: ctx, emit: emit}
		err := target.Query(req, stream)
		return response, err
	case sink.SinkClient:
		stream, err := target.Query(ctx, req, opts...)
		if err != nil {
			return response, err
		}
		for {
			frame, err := stream.Recv()
			if err == io.EOF {
				return response, nil
			}
			if err != nil {
				return response, err
			}
			if err := emit(frame); err != nil {
				return response, err
			}
		}
	default:
		return response, fmt.Errorf("unsupported stream test target %T", target)
	}
}
func collectScan(ctx context.Context, target any, req *sink.ScanRequest, opts ...grpc.CallOption) (*sink.ScanResponse, error) {
	response := &sink.ScanResponse{}

	emit := func(frame *sink.ScanResponse) error {
		response.Documents = append(response.Documents, frame.Documents...)
		response.NextCursor = frame.NextCursor
		response.Complete = frame.Complete
		return nil
	}
	switch target := target.(type) {
	case interface {
		Scan(context.Context, *sink.ScanRequest) (*sink.ScanResponse, error)
	}:
		return target.Scan(ctx, req)
	case interface {
		Scan(*sink.ScanRequest, grpc.ServerStreamingServer[sink.ScanResponse]) error
	}:
		stream := &collectorStream[sink.ScanResponse]{ctx: ctx, emit: emit}
		err := target.Scan(req, stream)
		return response, err
	case sink.SinkClient:
		stream, err := target.Scan(ctx, req, opts...)
		if err != nil {
			return response, err
		}
		for {
			frame, err := stream.Recv()
			if err == io.EOF {
				return response, nil
			}
			if err != nil {
				return response, err
			}
			if err := emit(frame); err != nil {
				return response, err
			}
		}
	default:
		return response, fmt.Errorf("unsupported stream test target %T", target)
	}
}
