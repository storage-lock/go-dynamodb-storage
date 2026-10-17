package dynamodb_storage

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
)

// TestDynamoDBLocalSemantics 连接本地 dynamodb-local（官方 DynamoDB 本地模拟，
// 支持条件表达式/LWT）跑接口级语义套件。需要环境变量 STORAGE_LOCK_DYNAMODB_ENDPOINT。
func TestDynamoDBLocalSemantics(t *testing.T) {
	endpoint := os.Getenv("STORAGE_LOCK_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_DYNAMODB_ENDPOINT not set, skipping dynamodb-local semantics test")
	}

	client := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("local", "local", "")),
	})

	ctx := context.Background()
	tableName := storage_test_helper.TestTableName

	// 建表（幂等）：dynamodb-local 用 -sharedDb 共享，首次需建，二次跳过。
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("LockId"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("LockId"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		// ResourceInUseException = 已存在，忽略；其他错误上报。
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			t.Fatalf("CreateTable: %v", err)
		}
	}

	cm := NewDynamoDBConnectionManagerFromClient(client)
	opts := NewDynamoDBStorageOptions()
	opts.SetConnectionManager(cm)
	opts.SetTableName(tableName)
	s, err := NewDynamoDBStorage(ctx, opts)
	if err != nil {
		t.Fatalf("NewDynamoDBStorage: %v", err)
	}
	storage_test_helper.TestStorage(t, s)
}