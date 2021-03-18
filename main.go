package main

import (
	"bufio"
	"container/heap"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/thakursaurabh1998/csv-grouping/util"
)

const (
	// 10 MB
	oneMb = 1 << 20
	// chunkSize                  = 10 * oneMb
	maxBuckets                 = 100
	maxConcurrentMapOperations = 10
	maxElemsCollectorFlush     = 20
	chunkSize                  = 1024
)

var (
	// using channels to create a pseudo semaphore
	sem           = make(chan int, maxConcurrentMapOperations)
	bucketFileMux = make([]sync.Mutex, 100)
	metaMux       = sync.Mutex{}
)

// ChunkMeta contains the meta of a particular chunk
type ChunkMeta struct {
	ID        int    `json:"id"`
	Processed bool   `json:"processed"`
	InputFile string `json:"inputFile"`
}

// CsvMeta contains the meta for the csv
type CsvMeta struct {
	ChunksMeta []ChunkMeta `json:"chunksMeta"`
	ChunkSize  uint64      `json:"chunkSize"`
}

func bToMb(byteNum uint64) uint64 {
	return byteNum / 1024 / 1024
}

func createHashNumber(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
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

func getCsvMeta() *CsvMeta {
	data, err := ioutil.ReadFile("./meta.json")
	if err != nil {
		return nil
	}

	meta := &CsvMeta{}
	err = json.Unmarshal(data, meta)

	return meta
}

func isProcessingPending(meta *CsvMeta) bool {
	for _, chunk := range meta.ChunksMeta {
		if chunk.Processed == false {
			return true
		}
	}

	return false
}

func markChunkComplete(chunkID int) {
	fmt.Printf("Processing complete for chunk %d....\n", chunkID)
	metaMux.Lock()
	csvMeta := getCsvMeta()
	csvMeta.ChunksMeta[chunkID-1].Processed = true

	metaJSON, _ := json.Marshal(csvMeta)

	writeDataToFile("./meta.json", nil, string(metaJSON))

	metaMux.Unlock()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func appendCsv(csvData *[][]string, header *[]string, bucketID int) {
	fileExisted := fileExists(fmt.Sprintf("bucket%d.csv", bucketID))

	f, err := os.OpenFile(fmt.Sprintf("bucket%d.csv", bucketID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	check(err)
	w := csv.NewWriter(f)

	bucketFileMux[bucketID].Lock()
	if !fileExisted {
		// add header to bucket file if it was created now
		headerArr := [][]string{*header}
		w.WriteAll(headerArr)
	}
	w.WriteAll(*csvData)
	bucketFileMux[bucketID].Unlock()

	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

func mapStageOnSegment(chunkMeta ChunkMeta) {
	f, err := os.Open(chunkMeta.InputFile)
	check(err)
	defer f.Close()
	r := csv.NewReader(f)

	var header []string

	stringBucket := [maxBuckets][][]string{}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if len(header) == 0 {
			header = record
			continue
		}

		dimensionKey := fmt.Sprintf("%s:%s:%s", record[0], record[1], record[2])

		hashNumber := createHashNumber(dimensionKey)

		bucketIndex := hashNumber % maxBuckets

		stringBucket[bucketIndex] = append(stringBucket[bucketIndex], record)
	}

	for bucketID, bucketData := range stringBucket {
		if len(bucketData) > 0 {
			appendCsv(&bucketData, &header, bucketID)
		}
	}

	markChunkComplete(chunkMeta.ID)
	// parse int
	// metric1, err := strconv.ParseInt(record[3], 10, 64)
	// metric2, err := strconv.ParseInt(record[4], 10, 64)
}

type (
	//ByDimensions is made for sorting csv
	ByDimensions [][]string
)

func (a ByDimensions) Len() int {
	return len(a)
}

func (a ByDimensions) Less(i, j int) bool {
	iString := strings.Join(a[i][:3], "")
	jString := strings.Join(a[j][:3], "")
	return iString < jString
}

func (a ByDimensions) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func sortFile(fileName string) {
	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	check(err)
	defer f.Close()

	r := csv.NewReader(f)

	header, err := r.Read()
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	sort.Sort(ByDimensions(records))

	// empty the file and take the cursor to the start
	f.Truncate(0)
	f.Seek(0, 0)

	w := csv.NewWriter(f)
	err = w.Write(header)
	err = w.WriteAll(records)
	check(err)
}

func combineSortedFiles(fileNames []string, bucketID int) {
	fmt.Printf("Combining files: %s ....\n", strings.Join(fileNames, ", "))

	var readers []*csv.Reader

	linesMinHeap := &util.StringHeap{}

	heap.Init(linesMinHeap)

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
		heap.Push(linesMinHeap, util.HeapValue{StringValue: strings.Join(line[:3], ":"), Index: index, Value: line})
	}

	collector := [][]string{}

	for linesMinHeap.Len() > 0 {
		topLine := linesMinHeap.Pop().(util.HeapValue)
		collector = append(collector, topLine.Value)

		newLine, err := readers[topLine.Index].Read()
		if err != nil {
			if err == io.EOF {
				continue
			} else {
				check(err)
			}
		}

		heap.Push(linesMinHeap, util.HeapValue{StringValue: strings.Join(newLine[:3], ":"), Index: topLine.Index, Value: newLine})

		if len(collector) >= maxElemsCollectorFlush {
			appendCsv(&collector, &header, bucketID)
			collector = nil
		}
	}
	if len(collector) > 0 {
		appendCsv(&collector, &header, bucketID)
	}
}

func externalSort(fileName string) {
	var wg sync.WaitGroup

	f, err := os.Open(fileName)
	check(err)
	defer f.Close()

	bucketNum := strings.Split(fileName, ".csv")[0]
	splitCsv(f, fmt.Sprintf("%s:input", bucketNum))

	// remove original bucket file
	fmt.Printf("Removing original bucket file %s....\n", fileName)
	err = os.Remove(fileName)
	check(err)

	bucketInputFiles, err := filepath.Glob(fmt.Sprintf("%s:input*.csv", bucketNum))
	check(err)

	for _, fn := range bucketInputFiles {
		wg.Add(1)
		go func(fn string) {
			defer wg.Done()
			sortFile(fn)
		}(fn)
	}
	wg.Wait()

	bucketID16, err := strconv.ParseInt(strings.Split(bucketNum, "bucket")[1], 10, 16)
	bucketID := int(bucketID16)
	check(err)

	combineSortedFiles(bucketInputFiles, bucketID)
}

func reduceStage(fileName string) {
	externalSort(fileName)
}

func startProcessing(fileName string, meta *CsvMeta) {
	var wg sync.WaitGroup

	for _, chunk := range meta.ChunksMeta {
		if !chunk.Processed {
			wg.Add(1)
			sem <- 1
			go func(chunk ChunkMeta) {
				defer wg.Done()
				mapStageOnSegment(chunk)
				<-sem
			}(chunk)
		}
	}

	wg.Wait()

	bucketFiles, err := filepath.Glob("bucket*.csv")
	check(err)

	for _, fileName := range bucketFiles {
		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()
			reduceStage(fileName)
		}(fileName)
	}

	wg.Wait()
}

func splitCsv(inputFile *os.File, prefix string) int {
	fmt.Printf("Splitting the %s file to smaller chunks....\n", inputFile.Name())

	r := bufio.NewReader(inputFile)

	bufPool := sync.Pool{New: func() interface{} {
		buf := make([]byte, chunkSize)
		return buf
	}}

	counter := 1

	headerBuf, _, _ := r.ReadLine()
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

	return counter
}

func createMeta(chunkCount int) *CsvMeta {
	var chunks []ChunkMeta

	for counter := 1; counter < chunkCount; counter++ {
		chunks = append(chunks, ChunkMeta{
			ID:        counter,
			Processed: false,
			InputFile: fmt.Sprintf("input%d.csv", counter),
		})

	}

	meta := CsvMeta{
		ChunkSize:  chunkSize,
		ChunksMeta: chunks,
	}

	metaJSON, _ := json.Marshal(meta)
	fmt.Println("Completed creating chunks....")
	fmt.Println("Now creating meta file....")
	writeDataToFile("./meta.json", nil, string(metaJSON))

	return &meta
}

func cleanTempFiles() {
	fmt.Println("Cleaning up the temporary files now....")
	bucketFiles, err := filepath.Glob("bucket*.csv")
	check(err)
	inputFiles, err := filepath.Glob("input*.csv")
	check(err)

	files := append(inputFiles, bucketFiles...)
	fmt.Println("Removing meta.json....")
	files = append(files, "meta.json")

	for _, f := range files {
		err := os.Remove(f)
		check(err)
	}
}

func main() {
	// fileName := "./small-input.csv"
	// fileName := "./big-input.csv"
	fileName := "./mid-input.csv"

	printMemUsage()

	f, err := os.Open(fileName)
	check(err)
	defer f.Close()

	meta := getCsvMeta()

	if meta != nil && isProcessingPending(meta) {
		fmt.Println("pending processes found, continuing with the unprocessed chunks")
	} else {
		counter := splitCsv(f, "input")
		meta = createMeta(counter)
	}

	startProcessing(fileName, meta)
	printMemUsage()

	// cleanTempFiles()
}

// func main() {
// 	f, err := os.Open("./small-input.csv")
// 	check(err)

// 	r := csv.NewReader(f)

// 	for {
// 		lines, err := r.Read()

// 		fmt.Println(lines, err)

// 		if err != nil {
// 			break
// 		}
// 	}

// }
