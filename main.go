package main

import (
	"log"
)

const walDir = "~/Documents/kv"

func main() {
	kv, err := NewKVStore(walDir)
	if err != nil {
		log.Println("Error creating KVStore:", err)
		return
	}
	defer kv.Close()
	kv.Print()

	// kv.Put([]byte("key-1"), []byte("value1"))
	// kv.Put([]byte("key-2"), []byte("value2"))
	// kv.Put([]byte("key-3"), []byte("value3"))
	// kv.Put([]byte("key-4"), []byte("value4"))
	// kv.Put([]byte("key-5"), []byte("value5"))
	// kv.Put([]byte("key-6"), []byte("value6"))
	// kv.Put([]byte("key-7"), []byte("value7"))

	// kv.Put([]byte(""), []byte("value2"))
	// kv.Put([]byte("color"), []byte(""))
	// kv.Put([]byte("key-1"), []byte("some utf-8 chars ✨ or binary data \x00\x01\x02"))
}
