package main

import (
	"bufio"
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
	"sync"
)

const (
	// 10 MB
	oneMb                      = 1 << 20
	chunkSize                  = 10 * oneMb
	maxBuckets                 = 100
	maxConcurrentMapOperations = 10
	// chunkSize = 1
)

var (
	// using channels to create a pseudo semaphore
	sem           = make(chan int, maxConcurrentMapOperations)
	bucketFileMux = make([]sync.Mutex, 100)
	metaMux       = sync.Mutex{}
)

type ChunkMeta struct {
	Id        int    `json:"id"`
	Processed bool   `json:"processed"`
	InputFile string `json:"inputFile"`
}

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

func PrintMemUsage() {
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

func markChunkComplete(chunkId int) {
	fmt.Printf("Processing complete for chunk %d....\n", chunkId)
	metaMux.Lock()
	csvMeta := getCsvMeta()
	csvMeta.ChunksMeta[chunkId-1].Processed = true

	metaJson, _ := json.Marshal(csvMeta)

	writeDataToFile("./meta.json", nil, string(metaJson))

	metaMux.Unlock()
}

func appendCsv(csvData *[][]string, bucketId int) {
	f, err := os.OpenFile(fmt.Sprintf("bucket%d.csv", bucketId), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	check(err)
	w := csv.NewWriter(f)

	bucketFileMux[bucketId].Lock()
	w.WriteAll(*csvData)
	bucketFileMux[bucketId].Unlock()

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

	for bucketId, bucketData := range stringBucket {
		if len(bucketData) > 0 {
			appendCsv(&bucketData, bucketId)
		}
	}

	markChunkComplete(chunkMeta.Id)
	// parse int
	// metric1, err := strconv.ParseInt(record[3], 10, 64)
	// metric2, err := strconv.ParseInt(record[4], 10, 64)
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

		counter += 1
	}

	return counter
}

func createMeta(chunkCount int) *CsvMeta {
	var chunks []ChunkMeta

	for counter := 1; counter < chunkCount; counter += 1 {
		chunks = append(chunks, ChunkMeta{
			Id:        counter,
			Processed: false,
			InputFile: fmt.Sprintf("input%d.csv", counter),
		})

	}

	meta := CsvMeta{
		ChunkSize:  chunkSize,
		ChunksMeta: chunks,
	}

	metaJson, _ := json.Marshal(meta)
	fmt.Println("Completed creating chunks....")
	fmt.Println("Now creating meta file....")
	writeDataToFile("./meta.json", nil, string(metaJson))

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
	fileName := "./big-input.csv"

	PrintMemUsage()

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
	PrintMemUsage()

	cleanTempFiles()
}
