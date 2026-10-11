package dynamodb_storage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (*dynamodb.Client, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, *dynamodb.Client) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error                 { return nil }

func newTestDynamo(t *testing.T) *DynamoDBStorage {
	t.Helper()
	s, err := NewDynamoDBStorage(context.Background(), &DynamoDBStorageOptions{
		ConnectionManager: NewDynamoDBConnectionManagerFromClient(&dynamodb.Client{}),
		TableName:         "locks",
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.NoError(t, err)
	return s
}

func restoreDynamo(t *testing.T) {
	t.Helper()
	put, del, get, scan := dynamoPutItem, dynamoDeleteItem, dynamoGetItem, dynamoScan
	t.Cleanup(func() {
		dynamoPutItem, dynamoDeleteItem, dynamoGetItem, dynamoScan = put, del, get, scan
	})
}

func TestDynamoDBConnectionManager(t *testing.T) {
	preset := &dynamodb.Client{}
	fromClient := NewDynamoDBConnectionManagerFromClient(preset)
	assert.Equal(t, "dynamodb-connection-manager", fromClient.Name())
	got, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	again, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, got, again)
	assert.NoError(t, fromClient.Return(context.Background(), got))
	assert.NoError(t, fromClient.Shutdown(context.Background()))

	fresh := &DynamoDBConnectionManager{Client: preset}
	got, err = fresh.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	assert.NoError(t, fresh.Shutdown(context.Background()))
}

func TestDynamoDBStorage_Lifecycle(t *testing.T) {
	_, err := NewDynamoDBStorage(context.Background(), &DynamoDBStorageOptions{
		ConnectionManager: takeErrorManager{},
		TableName:         "locks",
	})
	assert.EqualError(t, err, "take failed")

	s := newTestDynamo(t)
	now, err := s.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	plain, err := NewDynamoDBStorage(context.Background(), &DynamoDBStorageOptions{
		ConnectionManager: NewDynamoDBConnectionManagerFromClient(&dynamodb.Client{}),
		TableName:         "locks",
	})
	assert.NoError(t, err)
	now, err = plain.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	assert.NoError(t, s.Close(context.Background()))
	_, err = (&DynamoDBStorage{}).Watch(context.Background(), "")
	assert.Error(t, err)
}

func TestDynamoDBStorage_Mutations(t *testing.T) {
	s := newTestDynamo(t)
	restoreDynamo(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()
	cond := &types.ConditionalCheckFailedException{}

	dynamoPutItem = func(*dynamodb.Client, context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		return &dynamodb.PutItemOutput{}, nil
	}
	assert.NoError(t, s.CreateWithVersion(ctx, "id", 1, info))
	assert.NoError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))
	dynamoPutItem = func(*dynamodb.Client, context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		return nil, cond
	}
	assert.ErrorIs(t, s.CreateWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)
	dynamoPutItem = func(*dynamodb.Client, context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		return nil, errors.New("put failed")
	}
	assert.EqualError(t, s.CreateWithVersion(ctx, "id", 1, info), "put failed")
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "put failed")

	dynamoDeleteItem = func(*dynamodb.Client, context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		return &dynamodb.DeleteItemOutput{}, nil
	}
	assert.NoError(t, s.DeleteWithVersion(ctx, "id", 1, info))
	dynamoDeleteItem = func(*dynamodb.Client, context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		return nil, cond
	}
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)
	dynamoDeleteItem = func(*dynamodb.Client, context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		return nil, errors.New("delete failed")
	}
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "delete failed")
}

func TestDynamoDBStorage_GetAndList(t *testing.T) {
	s := newTestDynamo(t)
	restoreDynamo(t)
	ctx := context.Background()
	jsonText := (&storage.LockInformation{OwnerId: "owner"}).ToJsonString()

	dynamoGetItem = func(*dynamodb.Client, context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return nil, errors.New("get failed")
	}
	_, err := s.Get(ctx, "id")
	assert.EqualError(t, err, "get failed")

	dynamoGetItem = func(*dynamodb.Client, context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	dynamoGetItem = func(*dynamodb.Client, context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{"Version": &types.AttributeValueMemberN{Value: "1"}}}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	dynamoGetItem = func(*dynamodb.Client, context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{"Json": &types.AttributeValueMemberN{Value: "1"}}}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.EqualError(t, err, "invalid Json attribute type")

	dynamoGetItem = func(*dynamodb.Client, context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{"Json": &types.AttributeValueMemberS{Value: jsonText}}}, nil
	}
	got, err := s.Get(ctx, "id")
	assert.NoError(t, err)
	assert.Contains(t, got, "owner")

	dynamoScan = func(*dynamodb.Client, context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		return nil, errors.New("scan failed")
	}
	_, err = s.List(ctx)
	assert.EqualError(t, err, "scan failed")

	dynamoScan = func(*dynamodb.Client, context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		return &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{
			{"Version": &types.AttributeValueMemberN{Value: "1"}},
			{"Json": &types.AttributeValueMemberN{Value: "1"}},
			{"Json": &types.AttributeValueMemberS{Value: "not-json"}},
			{"Json": &types.AttributeValueMemberS{Value: jsonText}},
		}}, nil
	}
	listed, err := s.List(ctx)
	assert.NoError(t, err)
	count := 0
	for listed.Next() {
		count++
		assert.Equal(t, "owner", listed.Value().OwnerId)
	}
	assert.Equal(t, 1, count)
}

func TestIsConditionalCheckFailed_ErrorCode(t *testing.T) {
	assert.True(t, isConditionalCheckFailed(&smithy.GenericAPIError{Code: "ConditionalCheckFailedException"}))
	assert.False(t, isConditionalCheckFailed(&smithy.GenericAPIError{Code: "Other"}))
}

func TestDynamoDefaultOperations_Delegate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		http.Error(w, `{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"offline"}`, http.StatusInternalServerError)
	}))
	defer srv.Close()
	client := dynamodb.New(dynamodb.Options{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  srv.Client(),
		Retryer:     aws.NopRetryer{},
	}, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
	ctx := context.Background()
	_, err := dynamoPutItem(client, ctx, &dynamodb.PutItemInput{TableName: aws.String("locks")})
	assert.Error(t, err)
	_, err = dynamoDeleteItem(client, ctx, &dynamodb.DeleteItemInput{TableName: aws.String("locks")})
	assert.Error(t, err)
	_, err = dynamoGetItem(client, ctx, &dynamodb.GetItemInput{TableName: aws.String("locks")})
	assert.Error(t, err)
	_, err = dynamoScan(client, ctx, &dynamodb.ScanInput{TableName: aws.String("locks")})
	assert.Error(t, err)
}

func TestDynamoDBStorage_CheckError(t *testing.T) {
	_, err := NewDynamoDBStorage(context.Background(), NewDynamoDBStorageOptions())
	assert.ErrorIs(t, err, ErrConnectionManagerNil)
}
