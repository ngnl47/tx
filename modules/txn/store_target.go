package txn

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/storage"
)

// storeIdentity 对持久根使用记录身份；纯内存 Store 使用对象身份。
func storeIdentity(store change.UndoBindable) StoreKey {
	if isNil(store) {
		return StoreKey{}
	}
	if value, ok := store.(storage.ChangeStore); ok {
		key, err := json.Marshal(value.RecordKey())
		if err != nil {
			panic(fmt.Errorf("transaction: invalid record identity: %w", err))
		}
		name := value.TypeSpec().Type.Ref.FullName
		if name == "" {
			name = reflect.TypeOf(store).String()
		}
		return StoreKey{Type: name, Key: string(key)}
	}
	return StoreKey{Type: reflect.TypeOf(store).String(), Key: fmt.Sprintf("%p", store)}
}
