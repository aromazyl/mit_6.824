//
// test.go
// Copyright (C) 2017 zhangyule <zyl2336709@gmail.com>
//
// Distributed under terms of the MIT license.
//

package main

import "fmt"

func main() {
	test := make(map[string][]string)
	test["1231"] = append(test["1231"], "123")
	fmt.Println(test)
}
