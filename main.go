package main

import (
	"fmt"
	"os"

	"github.com/thakursaurabh1998/csv-grouping/src"
)

func main() {
	// fileName := "./small-input.csv"
	// fileName := "./mid-input.csv"
	fileName := "./big-input.csv"

	err := os.Remove("output.csv")
	if err != nil {
		fmt.Println(err)
	}

	src.ExternalSort(fileName)
	src.PrintMemUsage()

	src.ReduceStage("input-sorted.csv")
}
