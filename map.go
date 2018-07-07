package ddbmap

// Map is an interface implemented by *sync.Map.
// Code that uses this interface can use sync.Map and DynamoDB interchangebly.
type Map interface {
	Delete(key interface{})
	Load(key interface{}) (value interface{}, ok bool)
	LoadOrStore(key, value interface{}) (actual interface{}, loaded bool)
	Range(f func(key, value interface{}) bool)
	Store(key, value interface{})
}
