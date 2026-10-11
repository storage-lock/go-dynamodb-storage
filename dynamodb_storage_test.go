package dynamodb_storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

func TestNewDynamoDBStorageOptions(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	assert.Equal(t, storage.DefaultStorageTableName, opts.TableName)
	assert.Nil(t, opts.ConnectionManager)
	assert.Nil(t, opts.TimeProvider)
}

func TestDynamoDBStorageOptions_SetConnectionManager(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	cm := &testDynamoDBConnectionManager{}
	opts.SetConnectionManager(cm)
	assert.Equal(t, cm, opts.ConnectionManager)
}

func TestDynamoDBStorageOptions_SetTableName(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	opts.SetTableName("custom_table")
	assert.Equal(t, "custom_table", opts.TableName)
}

func TestDynamoDBStorageOptions_SetTimeProvider(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	tp := &LocalTimeProvider{}
	opts.SetTimeProvider(tp)
	assert.Equal(t, tp, opts.TimeProvider)
}

func TestDynamoDBStorageOptions_Check_EmptyTableName(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	opts.TableName = ""
	err := opts.Check()
	assert.Error(t, err)
	assert.Equal(t, ErrTableNameEmpty, err)
}

func TestDynamoDBStorageOptions_Check_NilConnectionManager(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	opts.ConnectionManager = nil
	err := opts.Check()
	assert.Error(t, err)
	assert.Equal(t, ErrConnectionManagerNil, err)
}

func TestDynamoDBStorageOptions_Check_ValidOptions(t *testing.T) {
	opts := NewDynamoDBStorageOptions()
	assert.NotNil(t, opts)
	opts.ConnectionManager = &testDynamoDBConnectionManager{}
	err := opts.Check()
	assert.NoError(t, err)
}

func TestNewDynamoDBStorage_NilOptions(t *testing.T) {
	assert.Panics(t, func() {
		NewDynamoDBStorage(context.Background(), nil)
	})
}

func TestDynamoDBStorage_GetName(t *testing.T) {
	s := &DynamoDBStorage{}
	name := s.GetName()
	assert.Equal(t, DynamoDBStorageName, name)
}

func TestDynamoDBStorage_Capabilities(t *testing.T) {
	s := &DynamoDBStorage{}
	caps := s.Capabilities()
	assert.NotNil(t, caps)
	assert.Len(t, caps, 4)
	assert.Equal(t, storage.CapabilityCAS, caps[0])
	assert.Equal(t, storage.CapabilityReliableTime, caps[1])
	assert.Equal(t, storage.CapabilityAtomicDelete, caps[2])
	assert.Equal(t, storage.CapabilityWatch, caps[3])
}

func TestLocalTimeProvider_GetTime(t *testing.T) {
	p := &LocalTimeProvider{}
	now, err := p.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
}

type testDynamoDBConnectionManager struct{}

func (t *testDynamoDBConnectionManager) Name() string {
	return "test-dynamodb-connection-manager"
}

func (t *testDynamoDBConnectionManager) Take(ctx context.Context) (*dynamodb.Client, error) {
	return nil, nil
}

func (t *testDynamoDBConnectionManager) Return(ctx context.Context, client *dynamodb.Client) error {
	return nil
}

func (t *testDynamoDBConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

// TestIsConditionalCheckFailed 验证条件冲突判定能穿透 smithy 错误包装。
// v2 SDK 的错误经常被 smithy.OperationError 等包装，裸指针类型断言会失效，
// 导致条件冲突被当成硬错误抛给核心层，破坏可重试语义。
func TestIsConditionalCheckFailed(t *testing.T) {
	assert.False(t, isConditionalCheckFailed(nil))

	// 裸的条件冲突错误
	assert.True(t, isConditionalCheckFailed(&types.ConditionalCheckFailedException{}))

	// 被包装一层（fmt %w）
	assert.True(t, isConditionalCheckFailed(fmt.Errorf("put item: %w", &types.ConditionalCheckFailedException{})))

	// smithy OperationError 包装（SDK 真实返回形态）
	opErr := &smithy.OperationError{
		ServiceID:     "DynamoDB",
		OperationName: "PutItem",
		Err:           &types.ConditionalCheckFailedException{},
	}
	assert.True(t, isConditionalCheckFailed(opErr))

	// 其它错误不误判
	assert.False(t, isConditionalCheckFailed(errors.New("boom")))
	assert.False(t, isConditionalCheckFailed(&types.InternalServerError{}))
	assert.False(t, isConditionalCheckFailed(&smithy.OperationError{
		ServiceID:     "DynamoDB",
		OperationName: "PutItem",
		Err:           &types.InternalServerError{},
	}))
}
