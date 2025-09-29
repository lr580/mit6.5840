package main

import (
	"fmt"
	"plugin"
)

func main() {
	p, err := plugin.Open("testplugin.so")
	if err != nil {
		panic(err)
	}
	v, err := p.Lookup("V")
	if err != nil {
		panic(err)
	}
	f, err := p.Lookup("F")
	if err != nil {
		panic(err)
	}
	fmt.Println(*v.(*int))
	f.(func())()
	// 更常用：先转再用
	// val := v.(*int)
	// fmt.Println(*val)
	// fv := f.(func())
	// fv()
}
