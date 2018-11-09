package processor

import (
	"sync"
)

type DimObj struct {
	Key        int
	Id         string
	Attributes map[string]interface{}
}

type DimStore struct {
	sync.RWMutex
	Dims    map[string]*DimObj
	nextKey int
}

func NewDimStore() *DimStore {
	return &DimStore{
		Dims: make(map[string]*DimObj),
	}
}

func (d *DimStore) Find(id string) *DimObj {
	d.RLock()
	defer d.RUnlock()
	if obj, ok := d.Dims[id]; ok {
		return obj
	}
	return nil
}

func (d *DimStore) Create(id string, attributes map[string]interface{}) *DimObj {
	obj := &DimObj{
		Key:        d.nextKey,
		Id:         id,
		Attributes: attributes,
	}
	d.Lock()
	defer d.Unlock()
	d.Dims[id] = obj
	d.nextKey++
	return obj
}
