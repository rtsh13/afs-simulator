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
	filesMeta map[string]FileMetaData
	toBeCleaned [string]
	garbageQuit <-chan struct{}
}

type FileMetaData {
	valid bool
	timeout time.Time
	lock sync.RWMutext
	open: bool
	version: int64
}

// Creates and returns new cache
// side effect: will destroy target directory if it already exists,
// and make a new one
func NewCache(dir string, cleanUpRate int64) (*Cache, error) {
	os.removeAll(dir)
	os.MkdirAll(dir, os.FileMode(int(0777)))
	cache := {
		dir: dir,
		filesMeta: make(map[string]FileMetaData)
		toBeCleaned: []
		garbageQuit: nil
	}
	cache.garbageQuit := cache.makeCleaner(cleanUpRate)
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

// Wipes the entire Cache
func (c *Cache) Kill() bool {
	close(c.garbageQuit)
	os.removeAll(c.dir)
	os.MkdirAll(c.dir, os.FileMode(int(0777)))
	return true
}
// Returns True if new cache entry is created, returns false if cache entry already
// exists, returns error if an erro ris encountered
func (c *Cache) Store(name string, content *os.File, version int64, timeout int64) (bool, error) {
	key := hash(name)
	if c.isValid(key) {
		return false, nil
	}
	index := -1
	for i, v := range c.toBeCleaned {
		if v == key {
			index = i
		}
	}
	if index != -1 {
		c.toBeCleaned = append(c.toBeCleaned[:index], c.toBeCleaned[index + 1:]...)
	}
	loc := c.getFileName(key)
	os.WriteFile(loc, content, os.FileMode(int(0777)))
	c.filesMeta[key] := {
		valid: true
		lock: sync.RWMutex
		timeout: time.Now().add(timeout * time.Second)
		version: int64
	}
	return true, nil
}

// if item is in cache, returns item, else returns and empty byte array
// returns any error ran into
func (c *Cache) Get(name string) (*os.File, int64, error) {
	key := hash(name)
	if !c.isValid(key) {
		return [], -1, nil
	}
	fm = c.filesMeta[key]
	if time.Now().After(fm.timeout) {
		c.markForCleanup(key)
		return [], -1, nil
	}
	fm.lock.RLock()
	loc := c.getFileName(key)
	f, err := os.OpenFile(loc, os.O_RDWR, 0644)
	if err != nil {
		fm.lock.RUnlock()
		return [], -1, err
	}
	return f, fm.version, nil
}

func (c *Cache) Close(file *os.File, name string) (bool, error) {
	key := hash(name)
	if !c.isValid(key) {
		return false, nil
	}
	fm = c.filesMeta[key]
	fm.lock.RUnlock()
	file.close()
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
	val, ok = c.filesMeta[key]
	if !ok {
		return false
	}
	return bool(c.filesMeta[key].valid)
}

func (c *Cache) markForCleanup(key string) bool {
	fm, ok = c.filesMeta[key]
	if (!ok) {
		return false
	}
	fm.lock.RLock()
	c.toBeCleaned = append(c.toBeCleaned, key)
	fm.valid = false
	fm.lock.RUnlock()
	return true
}

func (c *Cache) clean() error[] {
	var errs []error
	for _, value := range c.toBeCleaned {
		fm = c.fileMeta[value]
		fm.Lock()
		fm.valid = false
		loc := c.getFileName(value)
		err := os.Remove(loc)
		if (err != nil) {
			errs = append(errs, err)
		}
		fm.Unlock()
		delete(c.filesMeta, key)
	}
	return errs
}

func (c *Cache) makeCleaner(rate int64) <-chan struct{}{
	// If our cache starts expanding beyond control we can also add
	// a deep clean which can remove even more files (i.e. those with invalid timeouts)
	// and not just those marked for removal
	cleanUpTicker =clean time.newTicker(rate * time.Second) 
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-cleanUpTicker.C:
				errs = c.clean()
				if len(errs) > 0 {
					// We can talk about handling errors later, but I think in this instance
					// We could set up a sub protocol to re-init cache
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
