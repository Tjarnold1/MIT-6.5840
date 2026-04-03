package lock

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key     string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: l,
	}
	// You may add code here
	_ = ck.Put(l, "unlocked", rpc.Tversion(0))
	return lk
}

func generateHash() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func (lk *Lock) Acquire() {
	// Your code here
	var val string
	var ver rpc.Tversion
	hash, hashErr := generateHash()
	if hashErr != nil {
		panic(hashErr)
	}
	for {
		//fmt.Println("stuck in acquire loop")
		val, ver, _ = lk.ck.Get(lk.key)
		if val != "unlocked" {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		lockErr := lk.ck.Put(lk.key, hash, ver)
		if lockErr == rpc.ErrVersion {
			// ErrVersion in this case tells us that someone else has acquired the lock since we performed our Get
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if lockErr == rpc.ErrMaybe {
			// We COULD have gotten the lock. We can check by seeing if the value matches our hash
			val, _, _ = lk.ck.Get(lk.key)
			if val == hash {
				break
			}
		}
		if lockErr == rpc.OK {
			break
		}
	}
	lk.version = ver + 1
}

func (lk *Lock) Release() {
	// Your code here
	for {
		//fmt.Println("stuck in release loop")
		if err := lk.ck.Put(lk.key, "unlocked", lk.version); err == rpc.OK || err == rpc.ErrMaybe {
			break
		}
	}
}
