package ddbmap

// Map is an interface implemented by *sync.Map.
// Users of this interface can store data in *sync.Map or DynamoDB almost interchangeably.
// There are some caveats involving key types and dynamo's eventual consistency.
type Map interface {
	// Delete delete the value stored under the given key, if any.
	Delete(key interface{})
	// Load returns the value stored under the given key, if any.
	// The ok result indicates if there a value was found for the key.
	Load(key interface{}) (value interface{}, ok bool)
	// LoadOrStore returns the value stored under the given key, if any,
	// else stores and returns the given value.
	// The loaded result is true if the value was loaded, false if stored.
	LoadOrStore(key, value interface{}) (actual interface{}, loaded bool)
	// Range iterates over the map and applies the given function to every key and value.
	// Range stops iteration if the given function returns true.
	Range(f func(key, value interface{}) bool)
	// Store stores the given value under the given key.
	Store(key, value interface{})
}
