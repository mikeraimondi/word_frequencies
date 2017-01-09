package main

import (
	"compress/gzip"
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	minYear        = 1960
	minOccurrences = 1000
	totalsFilename = "googlebooks-eng-us-all-totalcounts-20120701.txt"
	dataGlob       = "googlebooks-eng-us-all-1gram-*.gz"
)

type wordStat struct {
	word      string
	frequency float64
	index     int
}

func (w *wordStat) String() string {
	return fmt.Sprint("{", w.word, " ", w.frequency, "}")
}

type descFreq []*wordStat

func (o descFreq) Len() int { return len(o) }
func (o descFreq) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
	o[i].index = i
	o[j].index = j
}
func (o descFreq) Less(i, j int) bool { return o[i].frequency > o[j].frequency }
func (o *descFreq) Push(x interface{}) {
	n := len(*o)
	word := x.(*wordStat)
	word.index = n
	*o = append(*o, word)
}
func (o *descFreq) Pop() interface{} {
	old := *o
	n := len(old)
	word := old[n-1]
	word.index = -1
	*o = old[0 : n-1]
	return word
}

func main() {
	err := runIngest(os.Args[1:])
	if err != nil {
		fmt.Println(err)
	}
}

func runIngest(args []string) (err error) {
	if len(args) != 1 {
		return fmt.Errorf("exactly one data directory is required\n")
	}

	wd, err := os.Getwd()
	if err != nil {
		// return errors.New("getting working directory " + err.Error())
		return err
	}
	wd, err = filepath.EvalSymlinks(wd)
	if err != nil {
		return err
		// return errors.New("evaluating symlinks " + err.Error())
	}

	totalsFile, err := os.Open(filepath.Join(wd, args[0], totalsFilename))
	if err != nil {
		return err
	}
	defer totalsFile.Close()
	tr := csv.NewReader(totalsFile)
	tr.Comma = '\t'
	totals, err := tr.Read()
	if err != nil {
		return err
	}
	var totalWords uint64
	for _, total := range totals {
		yearTotals := strings.Split(total, ",")
		if len(yearTotals) < 4 {
			continue
		}
		year, err := strconv.ParseInt(yearTotals[0], 10, 16)
		if err != nil {
			return err
		}
		if year < minYear {
			continue
		}
		yearTotal, err := strconv.ParseInt(yearTotals[1], 10, 64)
		if err != nil {
			return err
		}
		totalWords += uint64(yearTotal)
	}

	fileNames, err := filepath.Glob(filepath.Join(wd, args[0], dataGlob))
	if err != nil {
		return err
	}

	errs := make(chan error)
	words := make(chan *wordStat)
	done := make(chan bool)
	routines := 0

	for _, fileName := range fileNames {
		routines++
		go func(fName string, wChan chan *wordStat, dChan chan bool, eChan chan error) {
			f, err := os.Open(fName)
			if err != nil {
				eChan <- err
				return
			}
			defer f.Close()

			z, err := gzip.NewReader(f)
			if err != nil {
				eChan <- err
				return
			}
			defer z.Close()

			r := csv.NewReader(z)
			r.Comma = '\t'
			r.FieldsPerRecord = 4
			r.LazyQuotes = true

			wordMap := make(map[string]uint64)
			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					eChan <- err
					return
				}

				// not sure how to handle entries with periods
				// drop them for now
				if strings.Contains(record[0], ".") {
					continue
				}

				year, err := strconv.ParseInt(record[1], 10, 16)
				if err != nil {
					eChan <- err
					return
				}
				if year < minYear { // ignore older usages
					continue
				}

				count, err := strconv.ParseInt(record[2], 10, 64)
				if err != nil {
					eChan <- err
					return
				}
				word := strings.ToLower(strings.Split(record[0], "_")[0]) // underscores separate 1gram from special character
				wordMap[word] += uint64(count)
			}
			for word, occurrences := range wordMap {
				if occurrences > minOccurrences { // don't care about unusual words
					wChan <- &wordStat{
						word:      word,
						frequency: float64(occurrences) / float64(totalWords),
					}
				}
			}
			dChan <- true
		}(fileName, words, done, errs)
	}

	var wordStats descFreq
	heap.Init(&wordStats)
OuterLoop:
	for {
		select {
		case word := <-words:
			heap.Push(&wordStats, word)
		case err := <-errs:
			// TODO cancel all goroutines
			return err
		case <-done:
			routines--
			if routines == 0 {
				break OuterLoop
			}
		}
	}

	for i := 0; i < 100; i++ {
		w := heap.Pop(&wordStats)
		fmt.Print(w, " ")
	}
	return err
}
