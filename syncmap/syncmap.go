package syncmap

import (
	"fmt"
	"sync"
	"github.com/shawnsmithdev/ddbmap"
)

// FromItemFunc is a function that converts items into something else.
type FromItemFunc func(ddbmap.Item) interface{}

// SyncItemMap embeds sync.Map and implements ddbmap.ItemMap
type SyncItemMap struct {
	sync.Map
	// Keyer converts items to whatever keys should be used in the map.
	// If Keyer is nil, the item (as a %+v string) is used as the key directly.
	Keyer FromItemFunc
	// Valuer converts items to whatever values should be stored in the map, which must be Itemable.
	// If Valuer is nil, the item is used as the value directly.
	Valuer FromItemFunc
}

// TODO: common item key extraction

// TODO: Just checks we actually conform
func (m *SyncItemMap) asItemMap() ddbmap.ItemMap {
	return m
}

func (m *SyncItemMap) keyer(item ddbmap.Item) interface{} {
	if m.Keyer == nil {
		return fmt.Sprintf("%+v", item)
	}
	return m.Keyer(item)
}

func (m *SyncItemMap) valuer(item ddbmap.Item) interface{} {
	if m.Valuer == nil {
		return item
	}
	return m.Valuer(item)
}

// DeleteItem deletes any existing item with the same key(s) as the given item.
func (m *SyncItemMap) DeleteItem(keys ddbmap.Itemable) error {
	m.Delete(m.keyer(keys.AsItem()))
	return nil
}

// LoadItem returns the existing item, if present, with the same key(s) as the given item.
// The ok result returns true if the value was found.
func (m *SyncItemMap) LoadItem(keys ddbmap.Itemable) (ddbmap.Item, bool, error) {
	result, ok := m.Load(m.keyer(keys.AsItem()))
	if ok {
		if resultItem, toItemOk := result.(ddbmap.Item); toItemOk {
			return resultItem, toItemOk, nil
		}
	}
	return nil, false, nil
}

// StoreItem stores the given item, clobbering any existing item with the same key(s).
func (m *SyncItemMap) StoreItem(item ddbmap.Itemable) error {
	asItem := item.AsItem()
	m.Store(m.keyer(asItem), m.valuer(asItem))
	return nil
}

// LoadOrStoreItem returns the existing item, if present, with the same key(s) as the given item.
// Otherwise, it stores and returns the given item.
// The loaded result is true if the value was loaded, false if stored.
func (m *SyncItemMap) LoadOrStoreItem(item ddbmap.Itemable) (actual ddbmap.Item, loaded bool, err error) {
	asItem := item.AsItem()
	maybe, loaded := m.LoadOrStore(m.keyer(asItem), m.valuer(asItem))
	if loaded {
		if result, isItemable := maybe.(ddbmap.Itemable); isItemable {
			return result.AsItem(), isItemable, nil
		}
		panic("value in SyncItemMap is not Itemable")
	}
	return asItem, false, nil
}

// StoreIfAbsent stores the given value if there is no existing value with the same key(s),
// returning true if stored.
func (m *SyncItemMap) StoreIfAbsent(key, val interface{}) bool {
	_, ok := m.LoadOrStore(key, val)
	return !ok
}

// StoreItemIfAbsent stores the given item if there is no existing item with the same key(s),
// returning true if stored.
func (m *SyncItemMap) StoreItemIfAbsent(item ddbmap.Itemable) (bool, error) {
	asItem := item.AsItem()
	return m.StoreIfAbsent(m.keyer(asItem), m.valuer(asItem)), nil
}

// RangeItems calls the given consumer for each stored item.
// If the consumer returns false, range eventually stops the iteration.
// If a consumer returns false once, it should eventually always return false.
func (m *SyncItemMap) RangeItems(consumer func(ddbmap.Item) bool) error {
	m.Range(func(_, value interface{}) bool {
		if itemable, ok := value.(ddbmap.Itemable); ok {
			consumer(itemable.AsItem())
			return true
		}
		panic("value in SyncItemMap is not Itemable")
	})
	return nil
}
