package bdb

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

/*
db对象
*/
type BoltDB interface {
	Open(dbname string, mode os.FileMode) error // 打开
	Close()                                     // 关闭
	CreateTable(tn string) error                // 创建一张表
	DeleteTable(tn string) error                // 删除一张表
	GetDBName() string                          // 获取数据库名

	Set(tn string, key, value interface{}) error // 设置键值,key,value只支持int64,string,[]byte
	Get(tn string, key interface{}) []byte       // 获取键值
	Delete(tn string, key interface{}) error     // 删除键

	Add(tn string, value interface{}) error                  // 直接往表中添加，相当于集合
	Tarverse(tn string, tar func(k, v []byte) []byte) []byte // 遍历库表
}

// 实现BoltDB接口
type dbConnection struct {
	name string   // 数据库名字
	bdb  *bolt.DB // 数据库连接对象
}

// 打开一个数据库对象
func Open(db string, mode os.FileMode) BoltDB {
	bdb := &dbConnection{name: db}
	bdb.Open(db, mode)
	return bdb
}

func (b *dbConnection) Open(dbname string, mode os.FileMode) error {
	db, err := bolt.Open(dbname, mode, nil)
	if err != nil {
		return err
	}
	b.bdb = db
	return nil
}

func (b *dbConnection) Close() {
	if b.bdb != nil {
		b.bdb.Close()
	}
}

func (b *dbConnection) CreateTable(tn string) error {
	if b.bdb == nil {
		return fmt.Errorf("invalid boltdb connection")
	}

	return b.bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(tn))
		if err != nil {
			return fmt.Errorf("create bucket (%v) failed: %s", tn, err)
		}
		return nil
	})
}

func (b *dbConnection) DeleteTable(tn string) error {
	if b.bdb == nil {
		return fmt.Errorf("invalid boltdb connection")
	}

	return b.bdb.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(tn))
		if err != nil {
			return fmt.Errorf("delete bucket (%v) failed: %s", tn, err)
		}
		return nil
	})
}

func (b *dbConnection) GetDBName() string {
	return b.name
}

func (b *dbConnection) Set(tn string, key, value interface{}) (ret error) {
	b.bdb.Update(func(tx *bolt.Tx) error {
		k, err := dataToBytes(key)
		if err != nil {
			ret = fmt.Errorf("invalid key:%v", err)
			return err
		}
		v, err := dataToBytes(value)
		if err != nil {
			ret = fmt.Errorf("invalid value:%v", err)
			return err
		}

		bucket := tx.Bucket([]byte(tn))
		err = bucket.Put(k, v)
		if err != nil {
			ret = fmt.Errorf("set %v.%v failed: %v\n", tn, k, err)
		}
		return err
	})
	return ret
}

func (b *dbConnection) Get(tn string, key interface{}) (ret []byte) {
	b.bdb.Update(func(tx *bolt.Tx) error {
		k, err := dataToBytes(key)
		if err != nil {
			return err
		}

		bucket := tx.Bucket([]byte(tn))
		v := bucket.Get(k)
		// do make space before copy
		if len(v) > 0 {
			ret = make([]byte, len(v))
			copy(ret, v)
		}
		return nil
	})
	return ret
}

func (b *dbConnection) Delete(tn string, key interface{}) (ret error) {
	b.bdb.Update(func(tx *bolt.Tx) error {
		k, err := dataToBytes(key)
		if err != nil {
			ret = fmt.Errorf("invalid key:%v", err)
			return err
		}

		bucket := tx.Bucket([]byte(tn))
		bucket.Delete(k)
		return nil
	})
	return ret
}

func (b *dbConnection) Add(tn string, value interface{}) (ret error) {
	b.bdb.Update(func(tx *bolt.Tx) error {
		v, err := dataToBytes(value)
		if err != nil {
			ret = fmt.Errorf("invalid value:%v", err)
			return err
		}

		bucket := tx.Bucket([]byte(tn))
		id, err := bucket.NextSequence()
		if err != nil {
			ret = fmt.Errorf("next sequence error:%v", err)
			return err
		}

		k, err := dataToBytes(id)
		if err != nil {
			ret = fmt.Errorf("invalid key:%v", err)
			return err
		}

		err = bucket.Put(k, v)
		if err != nil {
			ret = fmt.Errorf("set %v.%v failed: %v\n", tn, k, err)
		}
		return err
	})
	return ret
}

func (b *dbConnection) Tarverse(tn string, tar func(k, v []byte) []byte) []byte {
	var ret string
	b.bdb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(tn))
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			ret = ret + string(tar(k, v)) + " "
		}
		return nil
	})
	return []byte(ret)
}

// 处理支持的key，value类型
func dataToBytes(data interface{}) (v []byte, err error) {
	switch val := data.(type) {
	case string:
		v = []byte(val)
	case []byte:
		v = val
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		v = []byte(fmt.Sprintf("%d", val))
	case float64, float32:
		v = []byte(fmt.Sprintf("%f", val))
	case fmt.Stringer:
		v = []byte(val.String())
	default:
		err = fmt.Errorf("non supported types")
	}
	return v, err
}
