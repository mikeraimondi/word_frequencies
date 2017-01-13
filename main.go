package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	minYear        = 1960
	minOccurrences = 1000
	totalsFilename = "googlebooks-eng-us-all-totalcounts-*.txt"
	dataGlob       = "googlebooks-eng-us-all-1gram-*.gz"
)

type wordStat struct {
	word      string
	frequency float64
}

func (w *wordStat) csvOut() []string {
  return []string{w.word, strconv.FormatFloat(w.frequency,'f',-1,64)}
}

func main() {
	words, err := runIngest(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	out, err := os.Create(os.Args[2])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer out.Close()
	w := csv.NewWriter(out)
	for _, wordRecord := range words {
		w.Write(wordRecord.csvOut())
	}
}

func runIngest(dir string) ([]*wordStat, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd, err = filepath.EvalSymlinks(wd)
	if err != nil {
		return nil, err
	}

	totalsFile, err := os.Open(filepath.Join(wd, dir, totalsFilename))
	if err != nil {
		return nil, err
	}
	defer totalsFile.Close()
	tr := csv.NewReader(totalsFile)
	tr.Comma = '\t'
	totals, err := tr.Read()
	if err != nil {
		return nil, err
	}
	var totalWords uint64
	for _, total := range totals {
		yearTotals := strings.Split(total, ",")
		if len(yearTotals) < 4 {
			continue
		}
		year, err := strconv.ParseInt(yearTotals[0], 10, 16)
		if err != nil {
			return nil, err
		}
		if year < minYear { // ignore older usages
			continue
		}

		yearTotal, err := strconv.ParseInt(yearTotals[1], 10, 64)
		if err != nil {
			return nil, err
		}
		totalWords += uint64(yearTotal)
	}

	fileNames, err := filepath.Glob(filepath.Join(wd, dir, dataGlob))
	if err != nil {
		return nil, err
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

	var wordStats []*wordStat
OuterLoop:
	for {
		select {
		case word := <-words:
			wordStats = append(wordStats, word)
		case err := <-errs:
			// TODO cancel all goroutines
			return nil, err
		case <-done:
			routines--
			if routines == 0 {
				break OuterLoop
			}
		}
	}

	sort.Slice(wordStats, func(i, j int) bool {
		return wordStats[i].frequency > wordStats[j].frequency
	})

	return wordStats, err
}
