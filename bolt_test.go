package bdb

import (
	"bytes"
	"strconv"
	"testing"
)

func TestMyBoltDB(t *testing.T) {
	dbname := "testmybolt.db"
	var tests = []struct {
		input interface{}
		want  interface{}
	}{
		{1, 1},
		{2, []byte("数字键，byte value")},
		{3, "string测试"},

		{[]byte("byte-key-int"), 2664},
		{[]byte("byte-key-byte"), []byte("byte key value")},
		{[]byte("byte-key-string"), "byte key, string value"},

		{"string-key-int", 9752},
		{"string-key-byte", []byte("string key, byte value")},
		{"string-key-string", "string key value"},
	}

	db := Open(dbname, 0600)
	if db.GetDBName() != dbname {
		t.Errorf("db.GetDBName() failed, want=%q, got=%v", "testmybolt.db", db.GetDBName())
	}
	//db.Open("testmybolt.db", 0600)
	defer db.Close()

	tn := "test"
	err := db.CreateTable(tn)
	if err != nil {
		t.Errorf("db.Create(%q) failed, err=%v", "test", err)
	}

	for _, test := range tests {
		db.Set(tn, test.input, test.want)
		got := db.Get(tn, test.input)
		switch test.want.(type) {
		case string:
			g := string(got)
			if test.want == g {
				t.Logf("string db.Set/Get(%v) passed", test.input)
			} else {
				t.Errorf("string db.Get(%q) == %q, want %q", test.input, g, test.want)
			}
		case []byte:
			g := got
			if bytes.Equal(g, test.want.([]byte)) {
				t.Logf("byte db.Set/Get(%v) passed", test.input)
			} else {
				t.Errorf("byte db.Get(%q) == %q, want %q", test.input, got, test.want)
			}
		case int:
			g, err := strconv.ParseInt(string(got), 10, 64)
			if err != nil {
				t.Errorf("int db.Get(%q) == %v, want %q, err=%v", test.input, got, test.want, err)
			}
			if int(g) == test.want.(int) {
				t.Logf("int db.Set/Get(%v) passed", test.input)
			} else {
				t.Errorf("int db.Get(%q) == %v, want %q", test.input, got, test.want)
			}
		}
	}
}
