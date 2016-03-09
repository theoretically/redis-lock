package lock

import (
	"strconv"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

type ClusterLock struct {
	client *redis.ClusterClient
	key    string
	opts   *LockOptions

	token string
	mutex sync.Mutex
}

// ObtainClusterLock is a shortcut for NewClusterLock().ClusterLock()
func ObtainClusterLock(client *redis.ClusterClient, key string, opts *LockOptions) (*ClusterLock, error) {
	ClusterLock := NewClusterLock(client, key, opts)
	if ok, err := ClusterLock.ClusterLock(); err != nil || !ok {
		return nil, err
	}
	return ClusterLock, nil
}

// NewClusterLock creates a new distributed ClusterLock on key
func NewClusterLock(client *redis.ClusterClient, key string, opts *LockOptions) *ClusterLock {
	opts = opts.normalize()
	return &ClusterLock{client: client, key: key, opts: opts}
}

// IsLocked returns true if a ClusterLock is acquired
func (l *ClusterLock) IsLocked() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.token != ""
}

// ClusterLock applies the ClusterLock, don't forget to defer the UnClusterLock() function to release the ClusterLock after usage
func (l *ClusterLock) ClusterLock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.token != "" {
		return l.refresh()
	}
	return l.create()
}

// Unlock releases the ClusterLock
func (l *ClusterLock) Unlock() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.release()
}

// Helpers

func (l *ClusterLock) create() (bool, error) {
	l.reset()

	// Create a random token
	token, err := randomToken()
	if err != nil {
		return false, err
	}

	// Calculate the timestamp we are willing to wait for
	stop := time.Now().Add(l.opts.WaitTimeout)
	for {
		// Try to obtain a ClusterLock
		ok, err := l.obtain(token)
		if err != nil {
			return false, err
		} else if ok {
			l.token = token
			return true, nil
		}

		if time.Now().Add(l.opts.WaitRetry).After(stop) {
			break
		}
		time.Sleep(l.opts.WaitRetry)
	}
	return false, nil
}

func (l *ClusterLock) refresh() (bool, error) {
	ttl := strconv.FormatInt(int64(l.opts.LockTimeout/time.Millisecond), 10)
	status, err := l.client.Eval(luaRefresh, []string{l.key}, []string{l.token, ttl}).Result()
	if err != nil {
		return false, err
	} else if status == int64(1) {
		return true, nil
	}
	return l.create()
}

func (l *ClusterLock) obtain(token string) (bool, error) {
	ok, err := l.client.SetNX(l.key, token, l.opts.LockTimeout).Result()
	if err == redis.Nil {
		err = nil
	}
	return ok, err
}

func (l *ClusterLock) release() error {
	defer l.reset()

	err := l.client.Eval(luaRelease, []string{l.key}, []string{l.token}).Err()
	if err == redis.Nil {
		err = nil
	}
	return err
}

func (l *ClusterLock) reset() {
	l.token = ""
}
