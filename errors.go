package dynamodb_storage

import "fmt"

var (
	ErrTableNameEmpty       = fmt.Errorf("TableName can not empty")
	ErrConnectionManagerNil  = fmt.Errorf("ConnectionManager can not nil")
)