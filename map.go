package ddbmap

// Map implements a key-value store where keys can always be determined from values.
type Map interface {
	// Delete delete the value stored under the given key, if any.
	Delete(toDelete interface{}) error
	// Load returns the value stored under the given key, if any.
	// The ok result indicates if there a value was found for the key.
	Load(toLoad interface{}) (result interface{}, ok bool, err error)
	// LoadOrStore returns the value stored under the same key as the given value, if any,
	// else stores and returns the given value.
	// The loaded result is true if the value was loaded, false if stored.
	LoadOrStore(value interface{}) (actual interface{}, loaded bool, err error)
	// Range iterates over the map and applies the given function to every value.
	// Range stops iteration if the given function returns false.
	Range(consumer func(value interface{}) (resume bool)) error
	// Store stores the given value.
	Store(toStore interface{}) error
	// Store stores the given value is no value with the same key is stored.
	// The stored result is true if the value was stored.
	StoreIfAbsent(toStore interface{}) (stored bool, err error)
}
