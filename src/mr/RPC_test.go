package mr

import (
	"fmt"
	"testing"
	"time"
)

func TestExampleRPC(t *testing.T) {
	c := MakeCoordinator([]string{"pg-grimm.txt"}, 3)
	c.Done() // do something to prevent not used

	time.Sleep(10 * time.Millisecond) // 等待协调器启动
	fmt.Println("Testing Example RPC...")
	CallExample()
	time.Sleep(10 * time.Millisecond)
	fmt.Println("Test completed.")
	// Output: Not valid
}

func ExamplePrint() {
	// 如果output不是那样，就会报错
	fmt.Println("Learning Go testing")
	// Output: Learning Go testing
}

func ExamplePrint2() {
	fmt.Println("Will not call this function")
}

func ExampleMultiLine() {
	fmt.Println("Line 1")
	fmt.Println("Line 2")
	// Output:
	// Line 1
	// Line 2
}

func ExampleUnordered() {
	for _, v := range []int{1, 2, 3} {
		fmt.Println(v)
	}
	// Unordered output:
	// 2
	// 1
	// 3
}
