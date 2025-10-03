package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	// 在指定目录创建临时文件
	tmpFile, err := os.CreateTemp("/tmp", "sample.*.tmp")
	if err != nil {
		fmt.Println("创建临时文件失败:", err)
		return
	}
	defer os.Remove(tmpFile.Name())

	fmt.Printf("临时文件路径: %s\n", tmpFile.Name())

	// 写入内容
	if _, err := tmpFile.Write([]byte{65, 66, 67}); err != nil {
		fmt.Println("写入临时文件失败:", err)
		return
	}

	fmt.Println(filepath.Dir("abc.txt"))          // 返回 "."
	fmt.Println(filepath.Dir("gg/abc.txt"))       // 返回 "gg"
	fmt.Println(filepath.Dir("/var/usr/abc.txt")) // 返回 "/var/usr"

	filename := "tasx.txt"
	dir := filepath.Dir(filename)

	// 使用 os.CreateTemp，它会自动生成唯一的随机文件名
	// 格式：dir/prefix-randomSuffix，例如：/tmp/mr-0-1-123456
	file, err := os.CreateTemp(dir, filepath.Base(filename)+"-")
	if err != nil {
		return
	}
	tempFile := file.Name()
	fmt.Println(tempFile)
}
