package dynamodb_storage

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/storage-lock/go-storage"
)

// DynamoDBStorageOptions AWS DynamoDB 存储的选项
type DynamoDBStorageOptions struct {
	ConnectionManager storage.ConnectionManager[*dynamodb.Client]
	TableName         string
	TimeProvider      storage.TimeProvider
}

func NewDynamoDBStorageOptions() *DynamoDBStorageOptions {
	return &DynamoDBStorageOptions{
		TableName: storage.DefaultStorageTableName,
	}
}

func (x *DynamoDBStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*dynamodb.Client]) *DynamoDBStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *DynamoDBStorageOptions) SetTableName(tableName string) *DynamoDBStorageOptions {
	x.TableName = tableName
	return x
}

func (x *DynamoDBStorageOptions) SetTimeProvider(tp storage.TimeProvider) *DynamoDBStorageOptions {
	x.TimeProvider = tp
	return x
}

func (x *DynamoDBStorageOptions) Check() error {
	if x.TableName == "" {
		return ErrTableNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
