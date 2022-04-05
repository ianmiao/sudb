package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestDataStore(t *testing.T) {

	t.Parallel()

	t.Run("InsertOrUpdate and Search", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		dataElems := []*DataElement{
			&DataElement{
				key:   fmt.Sprintf("normal_key_%d", r.Int()),
				value: []byte(fmt.Sprintf("normal_value_%d", r.Int())),
			},
			&DataElement{
				key:   fmt.Sprintf("zero_key_%d", r.Int()),
				value: []byte{},
			},
			&DataElement{
				key:         fmt.Sprintf("deleted_key_%d", r.Int()),
				value:       []byte{},
				isTombstone: true,
			},
		}

		dataStore := newDataStore(MemTableMaxLayer)

		for _, dataElem := range dataElems {
			oldDataElem := dataStore.InsertOrUpdate(dataElem)
			if oldDataElem != nil {
				t.Fatalf("InsertOrUpdate got unexpected old date element[%+v]", oldDataElem)
			}
		}

		for _, dataElem := range dataElems {
			gotDataElem := dataStore.Search(dataElem.key)
			if !reflect.DeepEqual(dataElem, gotDataElem) {
				t.Fatalf("Search err: exp[%+v], got[%+v]", dataElem, gotDataElem)
			}
		}
	})
}

func TestDataElement(t *testing.T) {

	t.Parallel()

	t.Run("Serialization and Deserialization", func(t *testing.T) {

		t.Parallel()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		for _, dataElem := range []DataElement{
			DataElement{
				key:   fmt.Sprintf("normal_key_%d", r.Int()),
				value: []byte(fmt.Sprintf("normal_value_%d", r.Int())),
			},
			DataElement{
				key:   fmt.Sprintf("zero_key_%d", r.Int()),
				value: []byte{},
			},
			DataElement{
				key:         fmt.Sprintf("deleted_key_%d", r.Int()),
				value:       []byte{},
				isTombstone: true,
			},
		} {
			gotDataElem := DataElement{}
			err := gotDataElem.Deserialization(dataElem.Serialization())
			if err != nil {
				t.Fatalf("data element[%+v], err: %s", dataElem, err)
			}

			if !reflect.DeepEqual(gotDataElem, dataElem) {
				t.Errorf("exp data element[%+v], got[%+v]", dataElem, gotDataElem)
			}
		}
	})
}
