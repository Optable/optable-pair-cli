package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
)

func main() {

	var (
		n       = flag.Int("n", 1000, "Number of records to generate")
		threads = flag.Int("t", 1, "Number of threads to use")
		f       = flag.String("f", "data.csv", "Output file name")
	)

	flag.Parse()

	fs, err := os.Create(*f)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer fs.Close()

	w := csv.NewWriter(fs)

	wLock := sync.Mutex{}

	var (
		wg            sync.WaitGroup
		workPerThread = *n / *threads
		remaining     = *n - workPerThread**threads
		batchSize     = 1000
	)

	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := &bytes.Buffer{}
			records := make([][]string, 0, batchSize)
			for j := 0; j < workPerThread; j++ {
				r := rand.Int64()
				if err := binary.Write(buf, binary.LittleEndian, r); err != nil {
					fmt.Println(err)
					return
				}

				records = append(records, []string{fmt.Sprintf("e:%x", sha256.Sum256(buf.Bytes()))})

				if len(records) == batchSize {
					wLock.Lock()
					if err := w.WriteAll(records); err != nil {
						fmt.Println(err)
						return
					}
					wLock.Unlock()
					// reset records
					records = make([][]string, 0, batchSize)
				}
			}

			// write the remaining records
			wLock.Lock()
			if err := w.WriteAll(records); err != nil {
				fmt.Println(err)
				return
			}
			wLock.Unlock()
			return
		}()
	}

	wg.Wait()

	// work on the remaining records
	buf := &bytes.Buffer{}
	records := make([][]string, 0, remaining)
	for i := 0; i < remaining; i++ {
		r := rand.Int64()
		if err := binary.Write(buf, binary.LittleEndian, r); err != nil {
			fmt.Println(err)
			return
		}

		records = append(records, []string{fmt.Sprintf("e:%x", sha256.Sum256(buf.Bytes()))})
	}

	if err := w.WriteAll(records); err != nil {
		fmt.Println(err)
		return
	}

	return
}
