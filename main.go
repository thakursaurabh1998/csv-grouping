package main

import (
	"fmt"
	"os"

	"github.com/thakursaurabh1998/csv-grouping/src"
	"github.com/thakursaurabh1998/csv-grouping/util"
)

func main() {
	fileName := os.Args[1]

	err := os.Remove("output.csv")
	if err != nil {
		fmt.Println(err)
	}

	src.ExternalSort(fileName)
	util.PrintMemUsage()

	src.ReduceStage("input-sorted.csv")
}
