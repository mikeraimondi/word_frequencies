package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Println("error: ", err, "; exiting")
		os.Exit(1)
	}
}

func run(dataDir, outFile string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	wd, err = filepath.EvalSymlinks(wd)
	if err != nil {
		return err
	}

	totalsFiles, err := filepath.Glob(filepath.Join(wd, dataDir, totalsGlob))
	if err != nil {
		return err
	}
	if len(totalsFiles) != 1 {
		return fmt.Errorf("provide exactly one totals file")
	}
	totalsFile, err := os.Open(totalsFiles[0])
	if err != nil {
		return err
	}
	defer totalsFile.Close()
	total, err := parseTotal(totalsFile)

	fileNames, err := filepath.Glob(filepath.Join(wd, dataDir, dataGlob))
	if err != nil {
		return err
	}

	var files []io.Reader
	for _, fileName := range fileNames {

		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer file.Close()
		files = append(files, file)
	}

	words, err := runIngest(total, files...)
	if err != nil {
		return err
	}
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer out.Close()
	w := csv.NewWriter(out)
	for _, wordRecord := range words {
		if err := w.Write(wordRecord.csvOut()); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return nil
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
	wordRegex := regexp.MustCompile(`\W`)

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

				year, err := strconv.ParseInt(record[1], 10, 16)
				if err != nil {
					eChan <- err
					return
				}
				if year < minYear { // ignore older usages
					continue
				}

				word := strings.ToLower(strings.Split(record[0], "_")[0]) // underscores separate 1gram from special character

				// not sure how to handle entries with non-word characters
				// drop them for now
				if wordRegex.MatchString(word) {
					continue
				}

				count, err := strconv.ParseInt(record[2], 10, 64)
				if err != nil {
					eChan <- err
					return
				}
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
