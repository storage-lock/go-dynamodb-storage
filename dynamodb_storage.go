package dynamodb_storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// DynamoDBStorage 基于 AWS DynamoDB 的存储引擎实现
// DynamoDB 支持条件写入，可实现真正的 CAS
type DynamoDBStorage struct {
	options      *DynamoDBStorageOptions
	client       *dynamodb.Client
	timeProvider storage.TimeProvider
}

var _ storage.Storage = &DynamoDBStorage{}

// 对象操作默认走真实客户端。Client 方法是值接收者且需要 AWS 配置，测试通过替换这些函数覆盖分支。
var (
	dynamoPutItem = func(client *dynamodb.Client, ctx context.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		return client.PutItem(ctx, input)
	}
	dynamoDeleteItem = func(client *dynamodb.Client, ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		return client.DeleteItem(ctx, input)
	}
	dynamoGetItem = func(client *dynamodb.Client, ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return client.GetItem(ctx, input)
	}
	dynamoScan = func(client *dynamodb.Client, ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		return client.Scan(ctx, input)
	}
)

func NewDynamoDBStorage(ctx context.Context, options *DynamoDBStorageOptions) (*DynamoDBStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &DynamoDBStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const DynamoDBStorageName = "dynamodb-storage"

func (x *DynamoDBStorage) GetName() string {
	return DynamoDBStorageName
}

func (x *DynamoDBStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
	}
}

func (x *DynamoDBStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// CreateWithVersion 原子创建锁记录
// 使用 ConditionExpression 确保 lockId 不存在
func (x *DynamoDBStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	_, err := dynamoPutItem(x.client, ctx, &dynamodb.PutItemInput{
		TableName: aws.String(x.options.TableName),
		Item: map[string]types.AttributeValue{
			"LockId":  &types.AttributeValueMemberS{Value: lockId},
			"Version": &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(version), 10)},
			"Json":    &types.AttributeValueMemberS{Value: lockInformation.ToJsonString()},
		},
		ConditionExpression: aws.String("attribute_not_exists(LockId)"),
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
func (x *DynamoDBStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	_, err := dynamoPutItem(x.client, ctx, &dynamodb.PutItemInput{
		TableName: aws.String(x.options.TableName),
		Item: map[string]types.AttributeValue{
			"LockId":  &types.AttributeValueMemberS{Value: lockId},
			"Version": &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(newVersion), 10)},
			"Json":    &types.AttributeValueMemberS{Value: lockInformation.ToJsonString()},
		},
		ConditionExpression: aws.String("Version = :expectedVersion"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expectedVersion": &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(expectedVersion), 10)},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion 原子删除锁记录
func (x *DynamoDBStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	_, err := dynamoDeleteItem(x.client, ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(x.options.TableName),
		Key: map[string]types.AttributeValue{
			"LockId": &types.AttributeValueMemberS{Value: lockId},
		},
		ConditionExpression: aws.String("Version = :expectedVersion"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expectedVersion": &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(expectedVersion), 10)},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

func (x *DynamoDBStorage) Get(ctx context.Context, lockId string) (string, error) {
	result, err := dynamoGetItem(x.client, ctx, &dynamodb.GetItemInput{
		TableName: aws.String(x.options.TableName),
		Key: map[string]types.AttributeValue{
			"LockId": &types.AttributeValueMemberS{Value: lockId},
		},
	})
	if err != nil {
		return "", err
	}
	if result.Item == nil {
		return "", storage_lock.ErrLockNotFound
	}
	jsonAttr, ok := result.Item["Json"]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	jsonVal, ok := jsonAttr.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("invalid Json attribute type")
	}
	return jsonVal.Value, nil
}

func (x *DynamoDBStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *DynamoDBStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

func (x *DynamoDBStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	slice := make([]*storage.LockInformation, 0)

	result, err := dynamoScan(x.client, ctx, &dynamodb.ScanInput{
		TableName: aws.String(x.options.TableName),
	})
	if err != nil {
		return nil, err
	}

	for _, item := range result.Items {
		jsonAttr, ok := item["Json"]
		if !ok {
			continue
		}
		jsonVal, ok := jsonAttr.(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		lockInfo, err := storage.LockInformationFromJsonString(jsonVal.Value)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}

	return go_iterator.FromSlice(slice), nil
}

func (x *DynamoDBStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func isConditionalCheckFailed(err error) bool {
	if err == nil {
		return false
	}
	// v2 SDK 的错误经常被 smithy.OperationError 等包装，裸指针类型断言会失效。
	// 先 errors.As 解包具体类型，再兜底按 ErrorCode 字符串判定（对齐 mongo 仓
	// 用 mongo.IsDuplicateKeyError 而不用字符串匹配的思路）。
	var condErr *types.ConditionalCheckFailedException
	if errors.As(err, &condErr) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "ConditionalCheckFailedException"
	}
	return false
}
