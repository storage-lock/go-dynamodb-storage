package dynamodb_storage

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/storage-lock/go-storage"
)

// DynamoDBConnectionManager 管理 DynamoDB 客户端连接
type DynamoDBConnectionManager struct {
	Client *dynamodb.Client

	initMu      sync.Mutex
	initialized bool
	client      *dynamodb.Client
}

var _ storage.ConnectionManager[*dynamodb.Client] = &DynamoDBConnectionManager{}

// NewDynamoDBConnectionManagerFromClient 从已有客户端创建
func NewDynamoDBConnectionManagerFromClient(client *dynamodb.Client) *DynamoDBConnectionManager {
	return &DynamoDBConnectionManager{
		Client:      client,
		initialized: true,
		client:      client,
	}
}

func (x *DynamoDBConnectionManager) Name() string {
	return "dynamodb-connection-manager"
}

func (x *DynamoDBConnectionManager) Take(ctx context.Context) (*dynamodb.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.client, nil
	}

	x.client = x.Client
	x.initialized = true
	return x.client, nil
}

func (x *DynamoDBConnectionManager) Return(ctx context.Context, connection *dynamodb.Client) error {
	return nil
}

func (x *DynamoDBConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	x.client = nil
	x.initialized = false
	return nil
}
