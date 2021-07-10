package main

import (
	"archive/zip"
	"bufio"
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/semaphore"
)

type Row struct {
	MarketCenter string    `db:"market_center"`
	Symbol       string    `db:"symbol"`
	Date         time.Time `db:"dt"`
	Time         time.Time `db:"tm"`
	ShortType    string    `db:"short_type"`
	Size         int       `db:"size"`
	Price        float64   `db:"price"`
}

func main() {
	// open zip file
	zipFileName := "files/FNSQsh202105_1.zip"
	zr, err := zip.OpenReader(zipFileName)
	if err != nil {
		log.Fatalln("failed to read", zipFileName, "with error:", err)
	}
	defer zr.Close()

	// read each file inside zip file
	for _, f := range zr.File {
		file, err := f.Open()
		if err != nil {
			log.Fatalln("failed to open", f.Name, "with error:", err)
		}
		defer file.Close()

		// read file by line
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)

		// populate data per batch
		batch := 100
		p := initPopulate(scanner, batch)

		// insert asynchronously using 25 workers
		workers := 25
		sem := semaphore.NewWeighted(int64(workers))
		ctx := context.TODO()

		var total int
		for p.Scan() {
			if err := sem.Acquire(ctx, 1); err != nil {
				log.Println("failed to acquire semaphore with error:", err)
				break
			}
			total += len(p.rows)
			go func(rows []Row, total int) {
				defer sem.Release(1)
				log.Println("total:", total, "data:", rows[len(rows)-1])
			}(p.rows, total)
		}

		// Acquire all of the tokens to wait for any remaining workers to finish.
		if err := sem.Acquire(ctx, int64(workers)); err != nil {
			log.Fatalln("failed to acquire semaphore with error:", err)
		}

		log.Println("DONE")
	}
}

type populate struct {
	scanner *bufio.Scanner
	batch   int
	done    bool
	rows    []Row
}

func initPopulate(scanner *bufio.Scanner, batch int) *populate {
	return &populate{scanner: scanner, batch: batch}
}

func (p *populate) Scan() bool {
	if p.done {
		return false
	}

	var rows []Row
	for p.scanner.Scan() {
		// convert line into Row struct
		t := strings.Split(p.scanner.Text(), "|")
		if len(t) < 7 {
			continue
		}
		date, err := time.Parse("20060102", t[2])
		if err != nil {
			continue
		}
		time, err := time.Parse("15:04:05", t[3])
		if err != nil {
			continue
		}
		size, err := strconv.Atoi(t[5])
		if err != nil {
			continue
		}
		price, err := strconv.ParseFloat(t[6], 64)
		if err != nil {
			continue
		}
		row := Row{
			MarketCenter: t[0],
			Symbol:       t[1],
			Date:         date,
			Time:         time,
			ShortType:    t[4],
			Size:         size,
			Price:        price,
		}
		rows = append(rows, row)

		// check rows size
		if len(rows) >= p.batch {
			p.rows = rows
			return true
		}
	}

	p.rows = rows
	p.done = true
	return true
}
