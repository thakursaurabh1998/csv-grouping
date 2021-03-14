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
	"os/exec"
	"runtime"
	"strconv"
	"strings"
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
	StartLine  int    `json:"startLine"`
	EndLine    int    `json:"endLine"`
	OutputFile string `json:"outputFile"`
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
		panic(e)
	}
}

func linesNum(fileName string) (uint64, error) {
	out, err := exec.Command("wc", "-l", fileName).Output()

	if runtime.GOOS == "windows" {
		return 0, fmt.Errorf("Can't execute on windows")
	}
	if err != nil {
		return 0, err
	}

	output := strings.Trim(string(out), " ")
	countStr := strings.Split(output, " ")
	count, err := strconv.ParseUint(countStr[0], 10, 64)

	return count, err
}

// linesInChunk gives an estimate number of lines
// present in a chunk of decided size
func linesInChunk(f *os.File) uint64 {
	r := bufio.NewReader(f)

	buf := make([]byte, chunkSize)
	n, err := r.Read(buf)

	if n == 0 {
		if err == io.EOF {
			log.Fatal("empty file")
		}
		if err != nil {
			log.Fatal("here", err)
		}
	} else {
		completeLine, err := r.ReadBytes('\n')

		if err != io.EOF {
			buf = append(buf, completeLine...)
		}
	}

	lines := string(buf)

	newlines := uint64(strings.Count(lines, "\n"))

	return newlines
}

func mapFunction(buf []byte) {
	content := string(buf)

	r := csv.NewReader(strings.NewReader(content))

	dimensionMap := make(map[string]interface{})

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("here2", err)
		}

		key := record[0] + record[1] + record[2]
		if dimensionMap[key] == nil {
			dimensionMap[key] = 0
		}
	}
}

func csvReader(f *os.File) {
	r := bufio.NewReader(f)

	bufPool := sync.Pool{New: func() interface{} {
		buf := make([]byte, chunkSize)
		return buf
	}}

	var wg sync.WaitGroup

	for {
		buf := bufPool.Get().([]byte)
		n, err := r.Read(buf)
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

		wg.Add(1)

		// fmt.Printf(string(buf))

		PrintMemUsage()

		go func() {
			// mapFunction(buf)
			// data := mapFunction(buf)

			defer wg.Done()
		}()
	}

	wg.Wait()
}

func writeDataToFile(fileName, data string) {
	f, err := os.Create("./meta.json")
	check(err)

	defer f.Close()

	_, err = f.WriteString(data)

	check(err)
}

func createCsvMeta(fileName string, totalLines, chunkLineCount uint64) *CsvMeta {
	var chunks []ChunkMeta

	counter := 0

	for i := 2; i < int(totalLines); i += int(chunkLineCount) + 1 {
		startLine := i
		endLine := i + int(chunkLineCount)
		if endLine > int(totalLines) {
			endLine = int(totalLines)
		}
		chunks = append(chunks, ChunkMeta{
			Id:         counter,
			Processed:  false,
			StartLine:  startLine,
			EndLine:    endLine,
			OutputFile: fmt.Sprintf("map%d.json", counter),
		})
		counter += 1
	}

	meta := CsvMeta{
		ChunkSize:  chunkSize,
		ChunksMeta: chunks,
	}

	metaJson, _ := json.Marshal(meta)

	writeDataToFile(fileName, string(metaJson))

	return &meta
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

func withScanner(input io.ReadSeeker, start int64) error {
	fmt.Println("--SCANNER, start:", start)
	if _, err := input.Seek(start, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(input)

	pos := start
	scanLines := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = bufio.ScanLines(data, atEOF)
		pos += int64(advance)
		return
	}
	scanner.Split(scanLines)

	for scanner.Scan() {
		fmt.Printf("Pos: %d, Scanned: %s\n", pos, scanner.Text())
	}
	return scanner.Err()
}

func processCsvSegment(f *os.File, chunkMeta ChunkMeta) {
	fmt.Println(chunkMeta)

}

func startProcessing(f *os.File, meta *CsvMeta) {
	var wg sync.WaitGroup

	for _, chunk := range meta.ChunksMeta {
		if chunk.Processed == false {
			wg.Add(1)
			go func(chunk ChunkMeta) {
				defer wg.Done()
				processCsvSegment(f, chunk)
			}(chunk)
		}
	}

	wg.Wait()
}

func main() {
	// fileName := "./small-input.csv"
	fileName := "./input.csv"

	PrintMemUsage()

	f, err := os.Open(fileName)
	check(err)
	defer f.Close()

	chunkLineNum := linesInChunk(f)
	totalLines, _ := linesNum(fileName)

	fmt.Println("Total line count\t", "lines in a chunk")
	fmt.Println(totalLines, "\t\t", chunkLineNum)

	PrintMemUsage()

	meta := getCsvMeta()

	if meta != nil && isProcessingPending(meta) {
		fmt.Println("pending processes found, continuing with the unprocessed chunks")
	} else {
		meta = createCsvMeta(fileName, totalLines, chunkLineNum)
	}

	startProcessing(f, meta)
}
