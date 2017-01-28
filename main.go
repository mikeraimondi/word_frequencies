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
	minOccurrences = 10000
	totalsGlob     = "googlebooks-eng-us-all-totalcounts-*.txt"
	dataGlob       = "googlebooks-eng-us-all-1gram-*.gz"
)

type wordStat struct {
	word      string
	frequency float64
}

func (w *wordStat) csvOut() []string {
	return []string{w.word, strconv.FormatFloat(w.frequency, 'f', -1, 64)}
}

func main() {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wd, err = filepath.EvalSymlinks(wd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	totalsFiles, err := filepath.Glob(filepath.Join(wd, os.Args[1], totalsGlob))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if len(totalsFiles) != 1 {
		fmt.Println("provide exactly one totals file")
		os.Exit(1)
	}
	totalsFile, err := os.Open(totalsFiles[0])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer totalsFile.Close()
	total, err := parseTotal(totalsFile)

	fileNames, err := filepath.Glob(filepath.Join(wd, os.Args[1], dataGlob))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var files []io.Reader
	for _, fileName := range fileNames {

		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println(err)
			os.Exit(1) // TODO this doesn't honor defer
		}
		defer file.Close()
		files = append(files, file)
	}

	words, err := runIngest(total, files...)
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
		if err := w.Write(wordRecord.csvOut()); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func parseTotal(src io.Reader) (uint64, error) {
	tr := csv.NewReader(src)
	tr.Comma = '\t'
	totals, err := tr.Read()
	if err != nil {
		return 0, err
	}
	var totalWords uint64
	for _, total := range totals {
		yearTotals := strings.Split(total, ",")
		if len(yearTotals) < 4 {
			continue
		}
		year, err := strconv.ParseInt(yearTotals[0], 10, 16)
		if err != nil {
			return 0, err
		}
		if year < minYear { // ignore older usages
			continue
		}

		yearTotal, err := strconv.ParseInt(yearTotals[1], 10, 64)
		if err != nil {
			return 0, err
		}
		totalWords += uint64(yearTotal)
	}
	return totalWords, nil
}

func runIngest(totalWords uint64, srcs ...io.Reader) ([]*wordStat, error) {
	errs := make(chan error)
	words := make(chan *wordStat)
	done := make(chan bool)
	routines := 0

	for _, src := range srcs {
		routines++
		go func(source io.Reader, wChan chan *wordStat, dChan chan bool, eChan chan error) {
			z, err := gzip.NewReader(source)
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
		}(src, words, done, errs)
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

	return wordStats, nil
}
