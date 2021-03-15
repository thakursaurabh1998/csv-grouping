package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"
)

const (
	// 10 MB
	chunkSize = 10 * (1 << 20)
	// chunkSize = 1
)

type ChunkMeta struct {
	Id         int    `json:"id"`
	Processed  bool   `json:"processed"`
	OutputFile string `json:"outputFile"`
	InputFile  string `json:"inputFile"`
}

type CsvMeta struct {
	ChunksMeta []ChunkMeta `json:"chunksMeta"`
	ChunkSize  uint64      `json:"chunkSize"`
}

func bToMb(byteNum uint64) uint64 {
	return byteNum / 1024 / 1024
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

func processCsvSegment(chunkMeta ChunkMeta) {
	f, err := os.Open(chunkMeta.InputFile)
	check(err)
	defer f.Close()
	r := csv.NewReader(f)

	var header []string

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
		}

		// data processing here
	}
}

func startProcessing(fileName string, meta *CsvMeta) {
	var wg sync.WaitGroup

	for _, chunk := range meta.ChunksMeta {
		if chunk.Processed == false {
			wg.Add(1)
			go func(chunk ChunkMeta) {
				defer wg.Done()
				processCsvSegment(chunk)
			}(chunk)
		}
	}

	wg.Wait()
}

func splitCsvAndCreateMeta(inputFile *os.File) *CsvMeta {
	var chunks []ChunkMeta

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
		writeDataToFile(fmt.Sprintf("input%d.csv", counter), append([]byte(header), buf...), "")

		chunks = append(chunks, ChunkMeta{
			Id:         counter,
			Processed:  false,
			OutputFile: fmt.Sprintf("map%d.json", counter),
			InputFile:  fmt.Sprintf("input%d.csv", counter),
		})

		counter += 1
	}

	meta := CsvMeta{
		ChunkSize:  chunkSize,
		ChunksMeta: chunks,
	}

	metaJson, _ := json.Marshal(meta)

	writeDataToFile("./meta.json", nil, string(metaJson))

	return &meta
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
		meta = splitCsvAndCreateMeta(f)
	}

	startProcessing(fileName, meta)
	PrintMemUsage()
}
