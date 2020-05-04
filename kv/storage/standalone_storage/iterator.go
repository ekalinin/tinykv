package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type saIterator struct {
	badger.Iterator
}

// Item returns pointer to the current key-value pair.
func (i *saIterator) Item() engine_util.DBItem {
	return i.Item()
}
