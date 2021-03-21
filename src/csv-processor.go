package src

import (
	"bufio"
	"container/heap"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/thakursaurabh1998/csv-grouping/util"
)

var (
	// maxConcurrentOperations for concurrency control
	maxConcurrentOperations = 10
	// using channels to create a pseudo semaphore
	sem                    = make(chan int, maxConcurrentOperations)
	outputMux              = sync.Mutex{}
	oneMb                  = 1 << 20
	maxElemsCollectorFlush = 250000
	chunkSize              = 5 * oneMb
)

type (
	//byDimensions is made for sorting csv
	byDimensions [][]string
)

func createHashNumber(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
		panic(e)
	}
}

func writeDataToFile(fileName string, bufData []byte, data string) {
	f, err := os.Create(fileName)
	check(err)

	defer f.Close()

	if bufData == nil {
		_, err = f.WriteString(data)
	} else {
		_, err = f.Write(bufData)
	}

	check(err)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func appendCsv(csvData *[][]string, header *[]string, fileName string) {
	fileExisted := fileExists(fileName)

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	check(err)
	w := csv.NewWriter(f)

	if !fileExisted {
		// add header to bucket file if it was created now
		headerArr := [][]string{*header}
		w.WriteAll(headerArr)
	}
	w.WriteAll(*csvData)

	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

func (a byDimensions) Len() int {
	return len(a)
}

func (a byDimensions) Less(i, j int) bool {
	iString := strings.Join(a[i][:3], ":")
	jString := strings.Join(a[j][:3], ":")
	return createHashNumber(iString) > createHashNumber(jString)
}

func (a byDimensions) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func sortFile(fileName string) {
	fmt.Printf("Sorting file %s....\n", fileName)
	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	check(err)
	defer f.Close()

	r := csv.NewReader(f)

	header, err := r.Read()
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	sort.Sort(byDimensions(records))

	// empty the file and take the cursor to the start
	f.Truncate(0)
	f.Seek(0, 0)

	w := csv.NewWriter(f)
	err = w.Write(header)
	err = w.WriteAll(records)
	check(err)
	fmt.Printf("Sorting finished for %s....\n", fileName)
}

// combineSortedFiles merges all the sorted smaller chunks using k sorted arrays merging
func combineSortedFiles(fileNames []string) {
	fmt.Printf("Combining files: %s ....\n", strings.Join(fileNames, ", "))

	outputFile := "input-sorted.csv"
	// reader of all the open files
	var readers []*csv.Reader

	// using minheap for merging sorted files
	mh := &util.MinHeap{}
	heap.Init(mh)

	var header []string

	for index, fileName := range fileNames {
		f, err := os.Open(fileName)
		check(err)
		defer f.Close()
		readers = append(readers, csv.NewReader(f))
		h, _ := readers[index].Read()
		if header == nil {
			header = h
		}
		line, _ := readers[index].Read()
		hashKey := createHashNumber(strings.Join(line[:3], ":"))
		heap.Push(mh, util.HeapValue{HashNumber: hashKey, Index: index, Value: line})
	}

	collector := [][]string{}

	for mh.Len() > 0 {
		topLine := heap.Pop(mh).(util.HeapValue)
		collector = append(collector, topLine.Value)

		for {
			nextLine, err := readers[topLine.Index].Read()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatal(err)
					continue
				}
			}
			hashKey := createHashNumber(strings.Join(nextLine[:3], ":"))
			if topLine.HashNumber == hashKey {
				collector = append(collector, nextLine)

				if len(collector) >= maxElemsCollectorFlush {
					appendCsv(&collector, &header, outputFile)
					collector = nil
				}
			} else {
				heap.Push(mh, util.HeapValue{HashNumber: hashKey, Index: topLine.Index, Value: nextLine})
				break
			}
		}
	}

	if len(collector) > 0 {
		appendCsv(&collector, &header, outputFile)
	}

	// remove bucket segments
	chunkNames, err := filepath.Glob(fmt.Sprintf("*:chunk*.csv"))
	check(err)

	for _, chunkName := range chunkNames {
		fmt.Printf("Removing bucket segment: %s....\n", chunkName)
		err = os.Remove(chunkName)
		check(err)
	}
}

func writeToOutputFile(records *map[string][]int64, header *[]string) {
	fileName := "output.csv"
	outputMux.Lock()
	fileExisted := fileExists(fileName)

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	check(err)
	w := csv.NewWriter(f)

	if !fileExisted {
		// add header to bucket file if it was created now
		headerArr := [][]string{*header}
		w.WriteAll(headerArr)
	}

	csvData := [][]string{}

	for k, v := range *records {
		row := strings.Split(k, ":")
		row = append(row, []string{strconv.FormatInt(v[0], 10), strconv.FormatInt(v[1], 10)}...)
		csvData = append(csvData, row)
	}
	w.WriteAll(csvData)
	outputMux.Unlock()
}

func splitCsv(inputFile *os.File, prefix string) {
	fmt.Printf("Splitting the %s file to smaller chunks....\n", inputFile.Name())

	r := bufio.NewReader(inputFile)

	// using a memory pool instead of allocating memory everytime
	bufPool := sync.Pool{New: func() interface{} {
		buf := make([]byte, chunkSize)
		return buf
	}}

	counter := 1

	headerBuf, _, _ := r.ReadLine()
	// putting a chunk of file in the memory
	// and reading till the next complete line
	header := string(headerBuf) + "\n"

	for {
		buf := bufPool.Get().([]byte)
		n, err := io.ReadFull(r, buf)
		buf = buf[:n]

		if n == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("here", err)
				break
			}
		}
		completeLine, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, completeLine...)
		}
		writeDataToFile(fmt.Sprintf("%s%d.csv", prefix, counter), append([]byte(header), buf...), "")

		counter++
	}
}

// ReduceStage reduces the data and aggregates it
func ReduceStage(fileName string) {
	fmt.Println("Aggregating the dimensions....")
	f, err := os.Open(fileName)
	check(err)
	r := csv.NewReader(f)

	header, err := r.Read()
	check(err)

	coll := map[string][]int64{}

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				check(err)
			}
		}
		metric1, err := strconv.ParseInt(record[3], 10, 64)
		metric2, err := strconv.ParseInt(record[4], 10, 64)

		key := strings.Join(record[:3], ":")
		if coll[key] == nil {
			coll[key] = []int64{0, 0}
		}
		coll[key][0] += metric1
		coll[key][1] += metric2

		// flushing coll to the output file and reinitializing
		if len(coll) >= maxElemsCollectorFlush {
			writeToOutputFile(&coll, &header)
			coll = make(map[string][]int64)
		}
	}

	if len(coll) > 0 {
		writeToOutputFile(&coll, &header)
	}

	os.Remove("input-sorted.csv")
}

// ExternalSort sorts the provided file
func ExternalSort(fileName string) {
	var wg sync.WaitGroup

	f, err := os.Open(fileName)
	check(err)
	defer f.Close()

	filePrefix := strings.Split(fileName, ".csv")[0]
	// splitting the csv into multiple chunks
	splitCsv(f, fmt.Sprintf("%s:chunk", filePrefix))

	chunkFiles, err := filepath.Glob(fmt.Sprintf("%s:chunk*.csv", filePrefix))
	check(err)

	// processing those chunked files
	for _, fn := range chunkFiles {
		wg.Add(1)
		// semaphore size defines how many concurrent operations happen
		sem <- 1
		// spawning goroutine for sorting separate chunks
		go func(fn string) {
			defer wg.Done()
			sortFile(fn)
			<-sem
		}(fn)
	}
	wg.Wait()

	combineSortedFiles(chunkFiles)
}
