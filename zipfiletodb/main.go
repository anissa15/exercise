package main

import (
	"archive/zip"
	"bufio"
	"log"
	"strconv"
	"strings"
	"time"
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
		for p.Scan() {
			log.Println("jumlah data per scan :", len(p.rows))
		}
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
