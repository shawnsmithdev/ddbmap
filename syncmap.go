package ddbmap

import "sync"

// KeyFromValue is a function that can generate a hashable key from a value.
type KeyFromValue func(interface{}) (interface{}, error)

type syncMap struct {
	m            sync.Map
	keyFromValue KeyFromValue
}

// NewSyncMap creates a new Map that uses sync.Map as storage. This is intended for use in tests.
func NewSyncMap(keyFromValue KeyFromValue) Map {
	return &syncMap{keyFromValue: keyFromValue}
}

func (sm *syncMap) Delete(toDelete interface{}) (err error) {
	if key, err := sm.keyFromValue(toDelete); err == nil {
		sm.m.Delete(key)
		return nil
	}
	return err
}

func (sm *syncMap) Load(toLoad interface{}) (result interface{}, ok bool, err error) {
	if key, err := sm.keyFromValue(toLoad); err == nil {
		result, ok = sm.m.Load(key)
		return result, ok, err
	}
	return nil, false, err
}

func (sm *syncMap) LoadOrStore(value interface{}) (actual interface{}, loaded bool, err error) {
	if key, err := sm.keyFromValue(value); err == nil {
		actual, loaded = sm.m.LoadOrStore(key, value)
		return actual, loaded, err
	}
	return nil, false, err
}

func (sm *syncMap) Range(consumer func(value interface{}) bool) error {
	sm.m.Range(func(_, v interface{}) bool {
		return consumer(v)
	})
	return nil
}

func (sm *syncMap) Store(value interface{}) (err error) {
	if k, err := sm.keyFromValue(value); err == nil {
		sm.m.Store(k, value)
	}
	return err
}

func (sm *syncMap) StoreIfAbsent(value interface{}) (bool, error) {
	_, loaded, err := sm.LoadOrStore(value)
	return loaded, err
}
