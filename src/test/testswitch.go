package main

import "fmt"

func main() {
	// mainloop:
	for num := 0; num < 5; num++ { // 输出一二三，换言之 break 不跳出 for
		switch num {
		case 1:
			fmt.Println("一")
		case 2:
			fmt.Println("二")
			break // 提前退出 switch
			// break mainloop // 提前退出 for
			fmt.Println("这行不会执行")
		case 3:
			fmt.Println("三")
		}
	}
}
