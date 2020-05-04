package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type saReader struct {
	tx *badger.Txn
}

func (r saReader) GetCF(cf string, key []byte) ([]byte, error) {
	i, err := engine_util.GetCFFromTxn(r.tx, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return i, nil
}

func (r saReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.tx)
}

func (r saReader) Close() {
	r.tx.Discard()
}
