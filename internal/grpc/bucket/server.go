package bucket

import (
	"bucketService/internal/data"
	"context"
	bckt "github.com/spacecowboytobykty123/bucketProto/gen/go/bucket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverAPI struct {
	bckt.UnimplementedBucketServer
	bucket Bucket
}

type Bucket interface {
	AddToBucket(ctx context.Context, toys []data.ToyShort) (bckt.OperationStatus, string)
	DelFromBucket(ctx context.Context, toysID []int64) (bckt.OperationStatus, string)
	GetBucket(ctx context.Context) ([]*data.Toy, int32)
	CreateBucket(ctx context.Context) (bckt.OperationStatus, string)
}

func Register(gRPC *grpc.Server, bucket Bucket) {
	bckt.RegisterBucketServer(gRPC, &serverAPI{bucket: bucket})
}

func (s *serverAPI) AddToBucket(ctx context.Context, r *bckt.AddToBucketRequest) (*bckt.AddToBucketResponse, error) {
	toyList := r.GetToys()
	for _, id := range toyList {
		if id.ToyId == 0 {
			return nil, status.Error(codes.InvalidArgument, "missing toy ids")
		}
		if id.Quantity == 0 {
			return nil, status.Error(codes.InvalidArgument, "missing toy qty")
		}

	}

	opStatus, msg := s.bucket.AddToBucket(ctx, mapToyShort(toyList))
	if opStatus != bckt.OperationStatus_STATUS_OK {
		return &bckt.AddToBucketResponse{
			Status: opStatus,
			Msg:    msg,
		}, status.Error(codes.Internal, "internal error!")
	}

	return &bckt.AddToBucketResponse{
		Status: opStatus,
		Msg:    msg,
	}, nil
}

func (s *serverAPI) DelFromBucket(ctx context.Context, r *bckt.DelFromBucketRequest) (*bckt.DelFromBucketResponse, error) {
	toyId := r.GetToyId()
	if len(toyId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing toys ids")
	}

	opStatus, msg := s.bucket.DelFromBucket(ctx, toyId)
	if opStatus != bckt.OperationStatus_STATUS_OK {
		return &bckt.DelFromBucketResponse{
			Status: opStatus,
			Msg:    msg,
		}, status.Error(codes.Internal, "internal error!")
	}
	return &bckt.DelFromBucketResponse{
		Status: opStatus,
		Msg:    msg,
	}, nil
}

func (s *serverAPI) GetBucket(ctx context.Context, r *bckt.GetBucketRequest) (*bckt.GetBucketResponse, error) {
	toyList, qty := s.bucket.GetBucket(ctx)

	return &bckt.GetBucketResponse{
		Toys:     mapToyList(toyList),
		Quantity: qty,
	}, nil
}

func (s *serverAPI) CreateBucket(ctx context.Context, r *bckt.CreateBucketRequest) (*bckt.CreateBucketResponse, error) {
	opStatus, msg := s.bucket.CreateBucket(ctx)
	if opStatus != bckt.OperationStatus_STATUS_OK {
		return &bckt.CreateBucketResponse{
			Status: opStatus,
			Msg:    msg,
		}, status.Error(codes.Internal, "internal error!")
	}
	return &bckt.CreateBucketResponse{
		Status: opStatus,
		Msg:    msg,
	}, nil

}

func mapToyShort(toyList []*bckt.ToyBucket) []data.ToyShort {
	domain := make([]data.ToyShort, 0, len(toyList))
	for _, t := range toyList {
		domain = append(domain, data.ToyShort{
			ID:  t.GetToyId(),
			Qty: t.GetQuantity(),
		})
	}
	return domain
}

func mapToyList(toyList []*data.Toy) []*bckt.Toy {
	domainToys := make([]*bckt.Toy, 0, len(toyList))
	for _, t := range toyList {
		domainToys = append(domainToys, &bckt.Toy{
			ToyId:    t.ID,
			Name:     t.Title,
			Value:    t.Value,
			ImageUrl: t.ImageURL,
			Quantity: t.Quantity,
		})
	}
	return domainToys
}
