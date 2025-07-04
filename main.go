package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("Please specify the root directory for WAL and checkpoints.")
		return
	}
	walDir := args[0]

	kv, err := NewKVStore(walDir)
	if err != nil {
		log.Println("Error creating KVStore:", err)
		return
	}
	defer kv.Close()
	kv.Print()

	// for i := 0; i < 100; i++ {
	// 	kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	// }

	// kv.Put([]byte(""), []byte("value2"))
	// kv.Put([]byte("color"), []byte(""))
	// kv.Put([]byte("key-1"), []byte("some utf-8 chars ✨ or binary data \x00\x01\x02"))

	for i := 0; i < 100; i++ {
		value, err := kv.Get([]byte(fmt.Sprintf("key-%d", i)))
		if err != nil {
			log.Printf("Error getting key-%d: %v\n", i, err)
		}
		log.Printf("kv.Get(\"key-%d\"): %v\n", i, string(value))
	}
}
