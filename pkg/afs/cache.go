package afs

import(
	"os"
	"sync"
	"errors"
	"time"
	"hash"
	"hash/fnv"
	"strconv"
	"strings"
	"bufio"
	"io"
)
type Cache {
	dir string
	locks map[string]sync.RWMutex
	valid map[string]bool
	timeout map[string]Time
	toBeCleaned [string]
}

// Creates and returns new cache
// side effect: will destroy target directory if it already exists,
// and make a new one
func NewCache(dir string, cleanUpRate int64) (*Cache, error) {
	os.removeAll(dir)
	os.MkdirAll(dir, os.FileMode(int(0777)))
	cache := {
		dir: dir,
		locks: make(map[string]sync.RWMutex),
		valid: make(map[string]bool),
		timeout make(map[string]Time)
		toBeCleaned: []
	}
	garbageQuit = cache.makeCleaner(cleanUpRate)

	defer() func { close(garbageQuit) }()
	return cache
}

// has a 1 in 4 billion chance of collision
// can change underlying hash function if necessary
// when scaling
func hash(val string) string {
	hasher = fnv.New32a()
	hasher.Write([]byte(s))
	return strconv.Itoa(hasher.Sum32())
}

// Returns True if new cache entry is created, returns false if cache entry already
// exists, returns error if an erro ris encountered
func (c *Cache) Store(name string, content byte[], timeout int64) (string, error) {
	key := hash(name)
	if c.isValid(key) {
		return key, nil
	}
	loc := c.getFileName(key)
	os.WriteFile(loc, content, os.FileMode(int(0777)))
	c.valid[key] = true
	c.locks[key] = sync.RWMutex
	c.timeout[key] = Time.Now().add(timeout * time.Second)
	return key, nil
}

// if item is in cache, returns item, else returns and empty byte array
// returns any error ran into
func (c *Cache) Get(name string) (byte[], error) {
	key := hash(name)
	if !c.isValid(key) {
		return [], nil
	}
	c.locks[key].RLock()
	loc = c.getFileName(key)
	f, err := os.Open(loc)
	if err != nil {
		c.locks[key].RUnlock()
		return [], err
	}
	stat, err := f.Stat()
	if err != nil {
		os.close(loc)
		c.locks[key].RUnlock()
		return [], err
	}
	bs := make([]byte, stat.Size())
	_, err = bufio.NewReader(f).Read(bs)
	if err != nil && err != io.EOF {
		os.close(loc)
		c.locks[key].RUnlock()
		return [], err
	}
	os.close(loc)
	c.locks[key].RUnlock()
	return bs, nil
}

// Removes item from valid return pool and marks it
// for clean up
func (c *Cache) Remove(name string) (bool, error) {
	key := hash(name)
	val := c.markForCleanup(key)
	return val
}

// Returns wheter an item is cached
func (c *Cache) Present(name string) bool {
	key := hash(name)
	return c.isValid(key)
}


func (c *Cache) isValid(key string) bool {
	return bool(c.valid[key])
}

func (c *Cache) markForCleanup(key string) bool {
	if (!c.isValid(key)) {
		return false
	}
	c.toBeCleaned = append(c.toBeCleaned, key)
	return true
}

func (c *Cahce) clean() error[] {
	var errs []error
	for _, key := range c.toBeCleaned {
		delete(c.valid, key)
		c.locks[key].Lock()
		loc = c.getFileName(key)
		err = os.Remove(loc)
		if (err != nil) {
			errs = append(errs, err)
		}
		c.locks[key].Unlock()
		delete(c.timeout, key)
		delete(c.locks, key)
	}
	c.toBeCleaned = []
	timeoutsToDelete = []
	for key, value := range c.timeout {
		if time.Now().After(value) {
			delete(c.valid, key)
			c.locks[key].Lock()
			loc = c.getFileName(key)
			err = os.Remove(loc)
			if(err != nil) {
				errs = append(errs, err)
			}
			c.locks[key].Unlock()
			delete(c.locks, key)
			timeoutsToDelete = timeoutsToDelete.append(timeoutsToDelete, key)
		}
	}
	for _, key := range timeoutsToDelete {
		delete(c.timeout, key)
	}
	return errs
}

func (c *Cache) makeCleaner(rate int64) <-chan struct{}{
	cleanUpTicker =clean time.newTicker(rate * time.Second) 
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-cleanUpTicker.C:
				errs = c.clean()
				if len(errs) > 0 {
					panic()
				}
			case <-quit:
				cleanUpTicker.Stop()
				return
			}
		}
	}()
	return quit
}

func (c *Cache) getFileName(key sting) string {
	var sb = strings.Builder
	sb.WriteString(c.dir)
	sb.WriteString(key)
	return sb.String()
}
