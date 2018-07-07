package ddbmap

import (
	"sync"
	"testing"
)

func testMap(m Map, t *testing.T) {
	x := testRecord{
		Id:       1,
		Name:     "bob",
		Age:      40,
		Friendly: true,
	}
	m.Store(x.Id, x)
	if loaded, loadOk := m.Load(x.Id); loadOk {
		if y, castOk := loaded.(testRecord); castOk {
			if x != y {
				t.Fail()
			}
		} else {
			t.Fail()
		}
	} else {
		t.Fail()
	}
	m.Delete(x.Id)
	if _, loadOk := m.Load(x.Id); loadOk {
		t.Fail()
	}
}

func TestSyncMap(t *testing.T) {
	sm := &sync.Map{}
	testMap(sm, t)
}
