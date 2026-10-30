package txn

import (
	"fmt"
	"math/big"
	"sort"
)

// Lockey 是不依赖对象加载的本机一致性域锁。Order 越大越先取得，不参与锁身份。
type Lockey struct {
	Type  string
	Key   string
	Order int
}

// Lockeys 是 Procedure 预声明的一组锁，运行时统一排序和去重。
type Lockeys []Lockey

// NewLockey 由生成的 LockType 入口调用；名称用于身份，枚举值只用于排序。
func NewLockey(lockType string, order int, key any) Lockey {
	return Lockey{Type: lockType, Key: fmt.Sprint(key), Order: order}
}
func (lock Lockey) identity() Lockey { return Lockey{Type: lock.Type, Key: lock.Key} }
func (lock Lockey) valid() bool      { return lock.Type != "" && lock.Key != "" }
func (lock Lockey) String() string   { return lock.Type + ":" + lock.Key }

func prepareLockeys(locks Lockeys) (Lockeys, error) {
	result := append(Lockeys(nil), locks...)
	seen := make(map[Lockey]int, len(result))
	count := 0
	for _, lock := range result {
		if !lock.identity().valid() || lock.Order < 0 {
			return nil, fmt.Errorf("transaction: invalid lock %v", lock)
		}
		if order, ok := seen[lock.identity()]; ok {
			if order != lock.Order {
				return nil, fmt.Errorf("transaction: conflicting order for %s", lock)
			}
			continue
		}
		seen[lock.identity()] = lock.Order
		result[count] = lock
		count++
	}
	result = result[:count]
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Order != b.Order {
			return a.Order > b.Order
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		ai, aok := new(big.Int).SetString(a.Key, 10)
		bi, bok := new(big.Int).SetString(b.Key, 10)
		if aok != bok {
			return aok
		}
		if aok && bok {
			if n := ai.Cmp(bi); n != 0 {
				return n < 0
			}
		}
		return a.Key < b.Key
	})
	return result, nil
}
