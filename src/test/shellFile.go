package main

import (
	"fmt"
	"os"
)

func main() {
	for i, arg := range os.Args {
		fmt.Printf("参数 %d: %s\n", i, arg)
	}
}

// go run shellFile.go ../src/main/pg*.txt
