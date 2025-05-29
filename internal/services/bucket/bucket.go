package bucket

import (
	subsgrpc "bucketService/internal/clients/subscription/grpc"
	toygrpc "bucketService/internal/clients/toys/grpc"
	"bucketService/internal/contextkeys"
	"bucketService/internal/data"
	"bucketService/internal/jsonlog"
	"context"
	bckt "github.com/spacecowboytobykty123/bucketProto/gen/go/bucket"
	subs "github.com/spacecowboytobykty123/subsProto/gen/go/subscription"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type Buckets struct {
	log            *jsonlog.Logger
	bucketProvider bucketProvider
	tokenTTL       time.Duration
	subsClient     *subsgrpc.Client
	toyClient      *toygrpc.ToyClient
}

type bucketProvider interface {
	AddToBucket(ctx context.Context, toys []data.ToyShort, userId int64) (bckt.OperationStatus, string)
	DelFromBucket(ctx context.Context, toysID []int64, userId int64) (bckt.OperationStatus, string)
	GetBucket(ctx context.Context, userId int64) ([]*data.Toy, int32)
	CreateBucket(ctx context.Context, userId int64) (bckt.OperationStatus, string)
}

func New(log *jsonlog.Logger, provider bucketProvider, tokenTTL time.Duration, subClient *subsgrpc.Client, toyClient *toygrpc.ToyClient) *Buckets {
	return &Buckets{
		log:            log,
		bucketProvider: provider,
		tokenTTL:       tokenTTL,
		subsClient:     subClient,
		toyClient:      toyClient,
	}
}

func (b *Buckets) AddToBucket(ctx context.Context, toysID []data.ToyShort) (bckt.OperationStatus, string) {
	userID, err := getUserFromContext(ctx)
	if err != nil {
		return bckt.OperationStatus_STATUS_UNAUTHORIZED, "invalid user!"
	}

	subsResp := b.subsClient.CheckSubscription(ctx, userID)
	if subsResp.SubStatus != subs.Status_STATUS_SUBSCRIBED {
		return bckt.OperationStatus_STATUS_UNAUTHORIZED, "user is not subscribed"
	}

	opStatus, msg := b.bucketProvider.AddToBucket(ctx, toysID, userID)
	return opStatus, msg
}

func (b *Buckets) DelFromBucket(ctx context.Context, toysID []int64) (bckt.OperationStatus, string) {
	userID, err := getUserFromContext(ctx)
	if err != nil {
		return bckt.OperationStatus_STATUS_UNAUTHORIZED, "invalid user!"
	}

	subsResp := b.subsClient.CheckSubscription(ctx, userID)
	if subsResp.SubStatus != subs.Status_STATUS_SUBSCRIBED {
		return bckt.OperationStatus_STATUS_UNAUTHORIZED, "user is not subscribed"
	}

	opStatus, msg := b.bucketProvider.DelFromBucket(ctx, toysID, userID)
	return opStatus, msg
}

func (b *Buckets) GetBucket(ctx context.Context) ([]*data.Toy, int32) {
	userID, err := getUserFromContext(ctx)
	if err != nil {
		return []*data.Toy{}, 0
	}

	subsResp := b.subsClient.CheckSubscription(ctx, userID)
	if subsResp.SubStatus != subs.Status_STATUS_SUBSCRIBED {
		return []*data.Toy{}, 0
	}
	toyList, qty := b.bucketProvider.GetBucket(ctx, userID)
	var toyIds []int64

	for _, item := range toyList {
		toyIds = append(toyIds, item.ID)
		println(item.ID)
	}
	println(toyIds)

	toyResp := b.toyClient.GetToysByIds(ctx, toyIds)
	if len(toyResp.Toy) == 0 {
		println("empty")
		return []*data.Toy{}, 0
	}

	for i, item := range toyList {
		item.Title = toyResp.Toy[i].Title
		item.ImageURL = toyResp.Toy[i].ImageUrl
		item.Value = toyResp.Toy[i].Value
	}

	return toyList, qty
}

func (b *Buckets) CreateBucket(ctx context.Context) (bckt.OperationStatus, string) {
	userID, err := getUserFromContext(ctx)
	if err != nil {
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "cannot get user from token"
	}

	subsResp := b.subsClient.CheckSubscription(ctx, userID)
	if subsResp.SubStatus != subs.Status_STATUS_SUBSCRIBED {
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "user is not subscribed!"
	}
	opStatus, msg := b.bucketProvider.CreateBucket(ctx, userID)
	return opStatus, msg
}

func getUserFromContext(ctx context.Context) (int64, error) {
	val := ctx.Value(contextkeys.UserIDKey)
	userID, ok := val.(int64)
	if !ok {
		return 0, status.Error(codes.Unauthenticated, "user id is missing or invalid in context")
	}

	return userID, nil

}

//func mapToysToBucketItems() {
//	ds
//}
